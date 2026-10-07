// Package storagebench_test compares audit-store designs on YugabyteDB YSQL,
// with WORM segments on S3 Object Lock and an OpenSearch sink. It is a
// test-only harness: skipped unless STORAGEBENCH=1, and under -short.
//
//	STORAGEBENCH=1 SB_PROFILE=quick go test ./tests/storagebench -run TestStorageBench -timeout 3h -v
//
// Numbers from one machine are for comparing designs, not for capacity planning.
package storagebench_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	mrand "math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
)

type profile struct {
	Warmup, Saturation, RateRun time.Duration
	Rates                       []int
	SearchRecords               int64
	Searches                    int
	Variants                    []string
}

var profiles = map[string]profile{
	"quick": {Warmup: 5 * time.Second, Saturation: 20 * time.Second, RateRun: 15 * time.Second,
		Rates: []int{2000, 5000}, SearchRecords: 100_000, Searches: 200, Variants: []string{"v1", "v3", "v2"}},
	"full": {Warmup: 30 * time.Second, Saturation: 3 * time.Minute, RateRun: 2 * time.Minute,
		Rates: []int{2000, 5000, 10000}, SearchRecords: 1_000_000, Searches: 1000, Variants: []string{"v1", "v3", "v2"}},
}

// Variants: v1 = one record per transaction; v2 = batched per partition flush;
// v3 = v2 without the actor and domain indexes (OpenSearch serves those).
var variantDesc = map[string]string{
	"v1": "single-row transaction per record, all 4 search indexes",
	"v2": "batched (100 rows / 50 ms) per partition, all 4 search indexes",
	"v3": "batched, aggregate and correlation indexes only; actor and domain search in OpenSearch",
}

const (
	batchMax = 100
	batchAge = 50 * time.Millisecond
)

type runResult struct {
	Name           string           `json:"name"`
	Variant        string           `json:"variant"`
	WORM           bool             `json:"worm"`
	OpenSearch     bool             `json:"opensearch"`
	TargetRate     int              `json:"target_rate"`
	Seconds        float64          `json:"seconds"`
	Events         int64            `json:"events"`
	EventsPerSec   float64          `json:"events_per_sec"`
	Inserted       int64            `json:"inserted_total"`
	Dups           int64            `json:"dups_skipped"`
	Retries        int64            `json:"write_retries"`
	AppendMs       pct              `json:"append_ms"`
	CommitMs       pct              `json:"commit_ms"`
	SegmentObjects int64            `json:"segment_objects"`
	SegmentMB      float64          `json:"segment_mb"`
	SegmentErrors  int64            `json:"segment_errors"`
	OSIndexed      int64            `json:"os_indexed"`
	OSQueueAtEnd   int              `json:"os_queue_at_end"`
	OSDrainSec     float64          `json:"os_drain_sec"`
	OSErrors       int64            `json:"os_errors"`
	Usage          map[string]usage `json:"usage"`
}

type pct struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	N   int     `json:"n"`
}

func percentiles(d []time.Duration) pct {
	if len(d) == 0 {
		return pct{}
	}
	slices.Sort(d)
	at := func(q float64) float64 {
		i := int(math.Ceil(q*float64(len(d)))) - 1
		return float64(d[max(i, 0)].Microseconds()) / 1000
	}
	return pct{P50: at(0.50), P95: at(0.95), P99: at(0.99), N: len(d)}
}

// ---------------------------------------------------------------- the writer

type runCfg struct {
	name    string
	variant string
	worm    bool
	os      bool
	rate    int // events per second across all partitions; 0 = as fast as possible
	warm    time.Duration
	dur     time.Duration
}

type bench struct {
	e      *env
	writer *pgxpool.Pool
	gens   []*generator
	bucket string
	key    []byte
}

func (b *bench) run(t *testing.T, cfg runCfg) runResult {
	t.Helper()
	ctx := context.Background()
	heads, err := loadHeads(ctx, b.writer)
	must(t, err)

	var sink *osSink
	if cfg.os {
		sink = newOSSink(b.e.osURL, 4)
	}
	start := time.Now()
	warmEnd := start.Add(cfg.warm)
	end := warmEnd.Add(cfg.dur)

	var events, inserted, dups, retries atomic.Int64
	appendLat := make([][]time.Duration, partitions)
	commitLat := make([][]time.Duration, partitions)
	segs := make([]*segmentWriter, partitions)

	var stats *statsSampler
	statsOnce := sync.OnceFunc(func() { stats = sampleStats(b.e) })
	go func() { time.Sleep(cfg.warm); statsOnce() }()

	var wg sync.WaitGroup
	for p := range partitions {
		chains := map[string]*chainState{}
		for c, s := range heads {
			if strings.HasSuffix(c, fmt.Sprintf("/%d", p)) {
				chains[c] = s
			}
		}
		if cfg.worm {
			segs[p] = newSegmentWriter(b.e.s3, b.bucket, p, func(r *record, at time.Time) {
				if !r.created.Before(warmEnd) && !r.ysqlAck.After(end) {
					commitLat[p] = append(commitLat[p], at.Sub(r.created))
				}
			})
		}
		w := &partWriter{
			variant: cfg.variant, db: b.writer, chains: chains, seg: segs[p], sink: sink,
			onAck: func(r *record) {
				inserted.Add(1)
				if r.created.Before(warmEnd) || r.ysqlAck.After(end) {
					return
				}
				events.Add(1)
				appendLat[p] = append(appendLat[p], r.ysqlAck.Sub(r.created))
				if !cfg.worm {
					commitLat[p] = append(commitLat[p], r.ysqlAck.Sub(r.created))
				}
			},
			dups: &dups, retries: &retries,
		}
		g := b.gens[p]
		wg.Go(func() {
			if err := w.drive(ctx, g, cfg.rate/partitions, end); err != nil {
				t.Errorf("partition %d: %v", p, err)
			}
		})
	}
	wg.Wait()
	statsOnce()
	use := stats.finish()

	res := runResult{Name: cfg.name, Variant: cfg.variant, WORM: cfg.worm, OpenSearch: cfg.os, TargetRate: cfg.rate,
		Seconds: cfg.dur.Seconds(), Usage: use}
	for p := range partitions {
		if s := segs[p]; s != nil {
			s.close()
			res.SegmentObjects += s.objects.Load()
			res.SegmentMB += float64(s.bytes.Load()) / (1 << 20)
			res.SegmentErrors += s.errs.Load()
		}
	}
	if sink != nil {
		res.OSQueueAtEnd = len(sink.queue)
		drain := time.Now()
		sink.close()
		res.OSDrainSec = time.Since(drain).Seconds()
		res.OSIndexed = sink.indexed.Load()
		res.OSErrors = sink.errs.Load()
	}
	res.Events = events.Load()
	res.EventsPerSec = float64(res.Events) / cfg.dur.Seconds()
	res.Inserted, res.Dups, res.Retries = inserted.Load(), dups.Load(), retries.Load()
	res.AppendMs = percentiles(slices.Concat(appendLat...))
	res.CommitMs = percentiles(slices.Concat(commitLat...))
	t.Logf("%-34s %8.0f ev/s  append p50/p99 %6.1f/%7.1f ms  commit p50/p99 %6.1f/%7.1f ms  dups %d retries %d",
		cfg.name, res.EventsPerSec, res.AppendMs.P50, res.AppendMs.P99, res.CommitMs.P50, res.CommitMs.P99, res.Dups, res.Retries)
	return res
}

// partWriter is the single writer of one Kafka partition's chains.
type partWriter struct {
	variant string
	db      *pgxpool.Pool
	chains  map[string]*chainState
	seg     *segmentWriter
	sink    *osSink
	onAck   func(*record)
	dups    *atomic.Int64
	retries *atomic.Int64

	batch      []*record
	batchStart time.Time
	stopOnErr  bool // crash test: give up instead of retrying
}

func (w *partWriter) drive(ctx context.Context, g *generator, rate int, end time.Time) error {
	var interval time.Duration
	if rate > 0 {
		interval = time.Second / time.Duration(rate)
	}
	start := time.Now()
	for i := int64(0); ; i++ {
		due := time.Now()
		if interval > 0 {
			due = start.Add(time.Duration(i) * interval)
			if wait := time.Until(due); wait > 0 {
				if len(w.batch) > 0 && time.Since(w.batchStart)+wait >= batchAge {
					if err := w.flush(ctx); err != nil {
						return err
					}
				}
				if w.seg != nil {
					w.seg.tick()
				}
				time.Sleep(time.Until(due))
			}
		}
		if !due.Before(end) {
			return w.flush(ctx)
		}
		if err := w.offer(ctx, g.next(due)); err != nil {
			return err
		}
	}
}

func (w *partWriter) offer(ctx context.Context, r *record) error {
	if len(w.batch) == 0 {
		w.batchStart = time.Now()
	}
	w.batch = append(w.batch, r)
	if w.variant == "v1" || len(w.batch) >= batchMax || time.Since(w.batchStart) >= batchAge {
		return w.flush(ctx)
	}
	if w.seg != nil {
		w.seg.tick()
	}
	return nil
}

// flush dedupes, links and writes the pending records, then hands them on.
func (w *partWriter) flush(ctx context.Context) error {
	if len(w.batch) == 0 {
		return nil
	}
	batch := w.batch
	w.batch = nil

	ids := make([]string, len(batch))
	for i, r := range batch {
		ids[i] = r.EventID
	}
	stored, err := existing(ctx, w.db, ids)
	if err != nil {
		return fmt.Errorf("dedupe: %w", err)
	}
	seen := map[string]bool{}
	var kept []*record
	for _, r := range batch {
		c := w.chains[r.Chain]
		if stored[r.EventID] || seen[r.EventID] || (c != nil && c.seq > 0 && r.Offset <= c.offset) {
			w.dups.Add(1)
			continue
		}
		seen[r.EventID] = true
		if c == nil {
			c = &chainState{offset: -1}
			w.chains[r.Chain] = c
		}
		c.link(r)
		kept = append(kept, r)
	}
	if len(kept) == 0 {
		return nil
	}
	for attempt := 0; ; attempt++ {
		err = appendBatch(ctx, w.db, kept, w.chains)
		if err == nil {
			break
		}
		if w.stopOnErr || ctx.Err() != nil || attempt >= 30 {
			return err
		}
		w.retries.Add(1)
		time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
	}
	now := time.Now()
	for _, r := range kept {
		r.ysqlAck = now
		w.onAck(r)
		if w.seg != nil {
			w.seg.add(r)
		}
		if w.sink != nil {
			w.sink.add(r)
		}
	}
	return nil
}

// ---------------------------------------------------------------- the test

type report struct {
	Profile     string                    `json:"profile"`
	Started     time.Time                 `json:"started"`
	Environment map[string]string         `json:"environment"`
	Runs        []runResult               `json:"runs"`
	Chains      map[string]chainCheck     `json:"chain_checks"`
	Search      map[string]pct            `json:"search_ms"`
	SearchCheck string                    `json:"search_ground_truth"`
	Disk        map[string]float64        `json:"disk_bytes_per_record"`
	Integrity   map[string]string         `json:"integrity"`
	Variants    map[string]string         `json:"variants"`
	Notes       []string                  `json:"notes"`
	TruncTrig   map[string]bool           `json:"truncate_trigger_supported"`
	Extra       map[string]map[string]any `json:"extra,omitempty"`
}

type chainCheck struct {
	Records int64  `json:"records"`
	Broken  string `json:"broken,omitempty"`
}

func TestStorageBench(t *testing.T) {
	if testing.Short() || os.Getenv("STORAGEBENCH") != "1" {
		t.Skip("storage benchmark: set STORAGEBENCH=1 (needs docker, takes minutes)")
	}
	profName := envOr("SB_PROFILE", "quick")
	prof, ok := profiles[profName]
	if !ok {
		t.Fatalf("unknown SB_PROFILE %q", profName)
	}
	if v := os.Getenv("SB_VARIANTS"); v != "" {
		prof.Variants = strings.Split(v, ",")
	}

	e := startEnv(t)
	ctx := context.Background()
	key := make([]byte, 32)
	_, _ = rand.Read(key)

	rep := &report{Profile: profName, Started: time.Now().UTC(), Chains: map[string]chainCheck{},
		Search: map[string]pct{}, Disk: map[string]float64{}, Integrity: map[string]string{},
		Variants: variantDesc, TruncTrig: map[string]bool{},
		Environment: map[string]string{
			"yugabytedb": ybImage, "opensearch": osImage, "s3": minioImage + " (Object Lock API stand-in for Ceph RGW)",
			"limits":  "2 CPUs per store; YugabyteDB 3 GiB, OpenSearch 2 GiB, S3 1 GiB",
			"writers": fmt.Sprintf("%d partitions, one writer each", partitions),
		}}
	if ep := os.Getenv("SB_S3_ENDPOINT"); ep != "" {
		rep.Environment["s3"] = ep
	}
	defer writeReport(t, rep)

	for _, v := range prof.Variants {
		t.Run(v, func(t *testing.T) {
			trig, err := resetSchema(ctx, e.admin, v != "v3")
			must(t, err)
			rep.TruncTrig[v] = trig
			must(t, resetOSIndex(ctx, e.osURL))
			writer := connectRetry(t, ctx, e.writerDSN)
			defer writer.Close()
			b := &bench{e: e, writer: writer, key: key, bucket: fmt.Sprintf("audit-%s-%d", v, time.Now().Unix())}
			must(t, newBucket(ctx, e.s3, b.bucket))
			for p := range partitions {
				b.gens = append(b.gens, newGenerator(p, key))
			}

			add := func(r runResult) { rep.Runs = append(rep.Runs, r) }
			add(b.run(t, runCfg{name: v + " saturation, YSQL only", variant: v, warm: prof.Warmup, dur: prof.Saturation}))
			if v == "v1" {
				rep.Chains[v] = checkChains(t, writer, b.gens[0])
				return
			}
			add(b.run(t, runCfg{name: v + " saturation, full pipeline", variant: v, worm: true, os: true, warm: prof.Warmup, dur: prof.Saturation}))
			for _, rate := range prof.Rates {
				add(b.run(t, runCfg{name: fmt.Sprintf("%s %d ev/s, full pipeline", v, rate), variant: v, worm: true, os: true,
					rate: rate, warm: prof.Warmup / 2, dur: prof.RateRun}))
			}
			if v == "v2" {
				var total int64
				for _, r := range rep.Runs {
					if r.Variant == v {
						total += r.Inserted
					}
				}
				for total < prof.SearchRecords {
					r := b.run(t, runCfg{name: "v2 preload", variant: v, dur: 60 * time.Second})
					total += r.Inserted
				}
				searchYSQL(t, b, prof.Searches, rep)
				searchOS(t, b, prof.Searches, rep)
				rep.SearchCheck = groundTruth(t, b, 50)
				diskUsage(t, b, total, rep)
				integrity(t, b, rep)
			}
			rep.Chains[v] = checkChains(t, writer, b.gens[0])
		})
	}
}

func checkChains(t *testing.T, db *pgxpool.Pool, g *generator) chainCheck {
	n, broken, err := verifyChains(context.Background(), db, g)
	must(t, err)
	if broken != "" {
		t.Errorf("chain broken: %s", broken)
	}
	return chainCheck{Records: n, Broken: broken}
}

// ---------------------------------------------------------------- search

func searchYSQL(t *testing.T, b *bench, n int, rep *report) {
	ctx := context.Background()
	var hot, cold, corr, act []string
	for _, g := range b.gens {
		hot = append(hot, g.samples.hotAggregates...)
		cold = append(cold, g.samples.coldAggregates...)
		corr = append(corr, g.samples.correlations...)
		act = append(act, g.samples.actors...)
	}
	prefixes := []string{"ledger|ledger.operation.", "payment|payment.payment.", "auth|auth.otp.", "notification|notification.message."}
	keys := map[string]func(i int) string{
		"aggregate_hot":  func(i int) string { return hot[i%len(hot)] },
		"aggregate_cold": func(i int) string { return cold[i%len(cold)] },
		"correlation":    func(i int) string { return corr[i%len(corr)] },
		"actor_7d":       func(i int) string { return act[i%len(act)] },
		"type_prefix_1d": func(i int) string { return prefixes[i%len(prefixes)] },
	}
	for name, key := range keys {
		q := ysqlQueries[0]
		for _, c := range ysqlQueries {
			if strings.HasPrefix(name, c.name) {
				q = c
			}
		}
		lat := make([]time.Duration, 0, n)
		for i := range n {
			s := time.Now()
			if _, err := q.run(ctx, b.writer, key(i)); err != nil {
				t.Errorf("search %s: %v", name, err)
				return
			}
			lat = append(lat, time.Since(s))
		}
		rep.Search["ysql "+name] = percentiles(lat)
		t.Logf("search ysql %-16s p50 %6.1f ms  p99 %6.1f ms", name, rep.Search["ysql "+name].P50, rep.Search["ysql "+name].P99)
	}
}

func searchOS(t *testing.T, b *bench, n int, rep *report) {
	ctx := context.Background()
	c := &http.Client{Timeout: 30 * time.Second}
	_, _ = osRequest(ctx, c, http.MethodPost, b.e.osURL+"/"+osIndex+"/_refresh", "")
	var act []string
	for _, g := range b.gens {
		act = append(act, g.samples.actors...)
	}
	domains := []string{"ledger", "payment", "auth", "notification"}
	rnd := mrand.New(mrand.NewPCG(7, 7))
	lat := make([]time.Duration, 0, n)
	for i := range n {
		s := time.Now()
		if _, err := osSearch(ctx, c, b.e.osURL, act[i%len(act)], domains[i%len(domains)], int16(1+rnd.IntN(6))); err != nil {
			t.Errorf("opensearch search: %v", err)
			return
		}
		lat = append(lat, time.Since(s))
	}
	rep.Search["opensearch actor+domain+channel 7d"] = percentiles(lat)
}

// groundTruth compares stored counts with what the generators produced.
func groundTruth(t *testing.T, b *bench, n int) string {
	ctx := context.Background()
	mismatches := 0
	checked := 0
	for i := range n {
		g := b.gens[i%partitions]
		s := g.samples.hotAggregates
		if i%2 == 1 {
			s = g.samples.coldAggregates
		}
		if len(s) == 0 {
			continue
		}
		agg := s[i%len(s)]
		var got int
		if err := b.writer.QueryRow(ctx, `SELECT count(*) FROM audit_records WHERE aggregate_id = $1`, agg).Scan(&got); err != nil {
			t.Errorf("ground truth: %v", err)
			return "error"
		}
		checked++
		if got != g.perAggregate[agg] {
			mismatches++
			t.Logf("ground truth %s: stored %d, generated %d", agg, got, g.perAggregate[agg])
		}
	}
	return fmt.Sprintf("%d of %d sampled aggregates match the generator", checked-mismatches, checked)
}

func diskUsage(t *testing.T, b *bench, records int64, rep *report) {
	ctx := context.Background()
	_, _ = b.e.admin.Exec(ctx, "SELECT 1") // keep the session warm
	if n := diskBytes(ctx, b.e.yb, "/home/yugabyte/var/data"); n > 0 {
		rep.Disk["yugabytedb (all variants so far, incl. WAL)"] = float64(n) / float64(records)
	}
	if n := diskBytes(ctx, b.e.os, "/usr/share/opensearch/data"); n > 0 {
		if c, err := osCount(ctx, b.e.osURL); err == nil && c > 0 {
			rep.Disk["opensearch"] = float64(n) / float64(c)
		}
	}
	var objs, bytes int64
	for o := range b.e.s3.ListObjects(ctx, b.bucket, minio.ListObjectsOptions{Recursive: true}) {
		if o.Err == nil {
			objs++
			bytes += o.Size
		}
	}
	var inBucket int64
	for _, r := range rep.Runs {
		if r.Variant == "v2" && r.WORM {
			inBucket += r.Inserted
		}
	}
	if inBucket > 0 {
		rep.Disk["s3 segments (logical)"] = float64(bytes) / float64(inBucket)
	}
	t.Logf("disk: %v (segment objects %d)", rep.Disk, objs)
}

// ---------------------------------------------------------------- integrity

func integrity(t *testing.T, b *bench, rep *report) {
	ctx := context.Background()
	chain := ""
	_ = b.writer.QueryRow(ctx, `SELECT chain_partition FROM chain_heads WHERE sequence_no > 10 LIMIT 1`).Scan(&chain)

	tryWriter := func(name, sql string) {
		_, err := b.writer.Exec(ctx, sql, chain)
		if err == nil {
			rep.Integrity[name] = "ALLOWED (bad)"
			t.Errorf("%s as audit_writer was allowed", name)
			return
		}
		rep.Integrity[name] = "refused: " + firstLine(err.Error())
	}
	tryWriter("writer UPDATE", `UPDATE audit_records SET event_type = 'x' WHERE chain_partition = $1 AND sequence_no = 3`)
	tryWriter("writer DELETE", `DELETE FROM audit_records WHERE chain_partition = $1 AND sequence_no = 3`)
	_, err := b.writer.Exec(ctx, `TRUNCATE audit_records`)
	if err == nil {
		rep.Integrity["writer TRUNCATE"] = "ALLOWED (bad)"
		t.Error("TRUNCATE as audit_writer was allowed")
	} else {
		rep.Integrity["writer TRUNCATE"] = "refused: " + firstLine(err.Error())
	}

	// The trigger refuses even the table owner when triggers are on.
	if _, err := b.e.admin.Exec(ctx, `UPDATE audit_records SET event_type = 'x' WHERE chain_partition = $1 AND sequence_no = 3`, chain); err == nil {
		rep.Integrity["owner UPDATE (trigger)"] = "ALLOWED (bad)"
		t.Error("owner UPDATE passed the append-only trigger")
	} else {
		rep.Integrity["owner UPDATE (trigger)"] = "refused: " + firstLine(err.Error())
	}

	// A superuser can switch triggers off. The hash chain must catch the edit.
	tx, err := b.e.admin.Begin(ctx)
	must(t, err)
	_, err = tx.Exec(ctx, `SET LOCAL session_replication_role = replica`)
	if err == nil {
		_, err = tx.Exec(ctx, `UPDATE audit_records SET event_type = event_type || '.tampered' WHERE chain_partition = $1 AND sequence_no = 5`, chain)
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		rep.Integrity["superuser bypass (session_replication_role)"] = "not possible: " + firstLine(err.Error())
	} else {
		must(t, tx.Commit(ctx))
		_, broken, verr := verifyChain(ctx, b.writer, b.gens[0], chain)
		must(t, verr)
		if broken == "" {
			rep.Integrity["superuser bypass (session_replication_role)"] = "possible and NOT detected (bad)"
			t.Error("tampered row not detected by the chain verifier")
		} else {
			rep.Integrity["superuser bypass (session_replication_role)"] = "possible; detected by chain verifier: " + broken
		}
		// Put the row back so the final chain check reflects the rest of the data.
		tx2, _ := b.e.admin.Begin(ctx)
		_, _ = tx2.Exec(ctx, `SET LOCAL session_replication_role = replica`)
		_, _ = tx2.Exec(ctx, `UPDATE audit_records SET event_type = replace(event_type, '.tampered', '') WHERE chain_partition = $1 AND sequence_no = 5`, chain)
		_ = tx2.Commit(ctx)
	}

	s3Integrity(t, b, rep)
	crashRecovery(t, b, rep)
}

func s3Integrity(t *testing.T, b *bench, rep *report) {
	ctx := context.Background()
	var obj minio.ObjectInfo
	for o := range b.e.s3.ListObjects(ctx, b.bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
		if o.Err == nil {
			obj = o
			break
		}
	}
	if obj.Key == "" {
		rep.Integrity["s3"] = "no segment objects found"
		return
	}
	err := b.e.s3.RemoveObject(ctx, b.bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID, GovernanceBypass: true})
	if err == nil {
		rep.Integrity["s3 delete locked version"] = "ALLOWED (bad)"
		t.Error("locked segment version was deleted")
	} else {
		rep.Integrity["s3 delete locked version"] = "refused: " + firstLine(err.Error())
	}
	shorter := time.Now().Add(time.Minute)
	mode := minio.Governance
	err = b.e.s3.PutObjectRetention(ctx, b.bucket, obj.Key, minio.PutObjectRetentionOptions{
		VersionID: obj.VersionID, Mode: &mode, RetainUntilDate: &shorter, GovernanceBypass: true})
	if err == nil {
		rep.Integrity["s3 shorten retention"] = "ALLOWED (bad)"
		t.Error("compliance retention was shortened")
	} else {
		rep.Integrity["s3 shorten retention"] = "refused: " + firstLine(err.Error())
	}
	body := "tampered\n"
	_, err = b.e.s3.PutObject(ctx, b.bucket, obj.Key, strings.NewReader(body), int64(len(body)), minio.PutObjectOptions{})
	orig, gerr := b.e.s3.GetObject(ctx, b.bucket, obj.Key, minio.GetObjectOptions{VersionID: obj.VersionID})
	st, serr := orig.Stat()
	switch {
	case err != nil:
		rep.Integrity["s3 overwrite"] = "refused: " + firstLine(err.Error())
	case gerr == nil && serr == nil && st.Size == obj.Size:
		rep.Integrity["s3 overwrite"] = "creates a new version; the original locked version is unchanged"
	default:
		rep.Integrity["s3 overwrite"] = "original version NOT readable after overwrite (bad)"
		t.Error("original segment lost after overwrite")
	}
}

// crashRecovery kills a writer mid-flush, restarts it from chain_heads and
// replays the partition from an earlier offset, as Kafka would after a crash.
func crashRecovery(t *testing.T, b *bench, rep *report) {
	ctx := context.Background()
	g := newGenerator(99, b.key)
	var all []*record
	for range 5000 {
		all = append(all, g.next(time.Now()))
	}
	var dups, retries atomic.Int64
	acked := 0
	newWriter := func(stopOnErr bool) *partWriter {
		heads, err := loadHeads(ctx, b.writer)
		must(t, err)
		chains := map[string]*chainState{}
		for c, s := range heads {
			if strings.HasSuffix(c, "/99") {
				chains[c] = s
			}
		}
		return &partWriter{variant: "v2", db: b.writer, chains: chains, dups: &dups, retries: &retries,
			stopOnErr: stopOnErr, onAck: func(*record) { acked++ }}
	}

	cctx, cancel := context.WithCancel(ctx)
	w := newWriter(true)
	crashAt := 2000 + mrand.IntN(1000)
	var crashErr error
	for i, r := range all {
		if i == crashAt {
			go cancel() // dies during the next flush
		}
		if crashErr = w.offer(cctx, cloneRecord(r)); crashErr != nil {
			break
		}
	}
	cancel()

	// Restart: replay everything from offset 0; the writer must skip what is stored.
	w = newWriter(false)
	for _, r := range all {
		must(t, w.offer(ctx, cloneRecord(r)))
	}
	must(t, w.flush(ctx))

	unique := map[string]bool{}
	for _, r := range all {
		unique[r.EventID] = true
	}
	var stored int
	must(t, b.writer.QueryRow(ctx, `SELECT count(*) FROM audit_records WHERE kafka_partition = 99`).Scan(&stored))
	var brokenAll []string
	rows, err := b.writer.Query(ctx, `SELECT chain_partition FROM chain_heads WHERE chain_partition LIKE '%/99'`)
	must(t, err)
	var chains []string
	for rows.Next() {
		var c string
		_ = rows.Scan(&c)
		chains = append(chains, c)
	}
	rows.Close()
	for _, c := range chains {
		_, broken, err := verifyChain(ctx, b.writer, g, c)
		must(t, err)
		if broken != "" {
			brokenAll = append(brokenAll, broken)
		}
	}
	verdict := fmt.Sprintf("crashed at record %d (%v); after replay %d stored of %d unique, chains intact: %v",
		crashAt, errCause(crashErr), stored, len(unique), len(brokenAll) == 0)
	if stored != len(unique) || len(brokenAll) > 0 {
		t.Errorf("crash recovery: %s %v", verdict, brokenAll)
	}
	rep.Integrity["crash and replay"] = verdict
}

func cloneRecord(r *record) *record {
	c := *r
	c.Seq, c.PrevHash, c.ChainHash = 0, nil, nil
	return &c
}

func errCause(err error) string {
	switch {
	case err == nil:
		return "no error reached the writer"
	case errors.Is(err, context.Canceled):
		return "context cancelled mid-write"
	default:
		return firstLine(err.Error())
	}
}

func firstLine(s string) string {
	s, _, _ = strings.Cut(s, "\n")
	if len(s) > 160 {
		s = s[:160] + "…"
	}
	return s
}

// ---------------------------------------------------------------- report

func writeReport(t *testing.T, rep *report) {
	dir := filepath.Join("..", "..", "bench", "storage")
	_ = os.MkdirAll(dir, 0o755)
	b, _ := json.MarshalIndent(rep, "", "  ")
	_ = os.WriteFile(filepath.Join(dir, "results.json"), b, 0o644)

	var md strings.Builder
	fmt.Fprintf(&md, "# Audit storage benchmark (%s profile)\n\nRun %s. One machine, every store limited to 2 CPUs; compare designs, not capacity.\n\n",
		rep.Profile, rep.Started.Format(time.RFC3339))
	md.WriteString("| Store | Setting |\n|---|---|\n")
	for _, k := range []string{"yugabytedb", "opensearch", "s3", "limits", "writers"} {
		fmt.Fprintf(&md, "| %s | %s |\n", k, rep.Environment[k])
	}
	md.WriteString("\n## Variants\n\n")
	for _, v := range []string{"v1", "v2", "v3"} {
		fmt.Fprintf(&md, "- **%s**: %s\n", v, rep.Variants[v])
	}
	md.WriteString("\n## Writes\n\n| Run | Events/s | Append p50 / p99 (ms) | Commit p50 / p99 (ms) | YSQL CPU avg % | YSQL mem max MiB | OpenSearch queue at end | Dups skipped |\n|---|---|---|---|---|---|---|---|\n")
	for _, r := range rep.Runs {
		if r.Name == "v2 preload" {
			continue
		}
		y := r.Usage["yugabytedb"]
		fmt.Fprintf(&md, "| %s | %.0f | %.1f / %.1f | %.1f / %.1f | %.0f | %.0f | %d | %d |\n",
			r.Name, r.EventsPerSec, r.AppendMs.P50, r.AppendMs.P99, r.CommitMs.P50, r.CommitMs.P99, y.CPUAvgPct, y.MemMaxMiB, r.OSQueueAtEnd, r.Dups)
	}
	md.WriteString("\nAppend = due time to YSQL commit. Commit = due time to the point the Kafka offset may be committed (YSQL and, in the full pipeline, the WORM segment).\n\n## Search (single client)\n\n| Query | p50 ms | p99 ms |\n|---|---|---|\n")
	keys := make([]string, 0, len(rep.Search))
	for k := range rep.Search {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		fmt.Fprintf(&md, "| %s | %.1f | %.1f |\n", k, rep.Search[k].P50, rep.Search[k].P99)
	}
	fmt.Fprintf(&md, "\nGround truth: %s.\n\n## Storage\n\n| Store | Bytes per record |\n|---|---|\n", rep.SearchCheck)
	for k, v := range rep.Disk {
		fmt.Fprintf(&md, "| %s | %.0f |\n", k, v)
	}
	md.WriteString("\n## Integrity\n\n| Check | Result |\n|---|---|\n")
	ik := make([]string, 0, len(rep.Integrity))
	for k := range rep.Integrity {
		ik = append(ik, k)
	}
	slices.Sort(ik)
	for _, k := range ik {
		fmt.Fprintf(&md, "| %s | %s |\n", k, strings.ReplaceAll(rep.Integrity[k], "|", "/"))
	}
	md.WriteString("\n## Chains verified\n\n| Variant | Records | Result |\n|---|---|---|\n")
	for _, v := range []string{"v1", "v2", "v3"} {
		if c, ok := rep.Chains[v]; ok {
			res := "intact"
			if c.Broken != "" {
				res = c.Broken
			}
			fmt.Fprintf(&md, "| %s | %d | %s |\n", v, c.Records, res)
		}
	}
	_ = os.WriteFile(filepath.Join(dir, "RESULTS.md"), []byte(md.String()), 0o644)
	t.Logf("report written to %s", dir)
}
