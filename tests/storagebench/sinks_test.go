package storagebench_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
)

// ---------------------------------------------------------------- WORM segments

const (
	segmentMaxRecords = 1000
	segmentMaxAge     = time.Second
	segmentRetention  = 24 * time.Hour // production: 10 years
)

type segmentLine struct {
	Chain       string    `json:"chain"`
	Seq         int64     `json:"seq"`
	EventID     string    `json:"event_id"`
	EventType   string    `json:"event_type"`
	OccurredAt  time.Time `json:"occurred_at"`
	Topic       string    `json:"topic"`
	Partition   int32     `json:"partition"`
	Offset      int64     `json:"offset"`
	KeyID       string    `json:"key_id"`
	PayloadEnc  string    `json:"payload_enc"`
	PayloadHash string    `json:"payload_sha256"`
	PrevHash    string    `json:"prev_hash"`
	ChainHash   string    `json:"chain_hash"`
}

// segmentWriter buffers the records of one partition and flushes them as one
// Object Lock (Compliance) object. Flushes run on their own goroutine so the
// partition writer is not blocked; onCommit is called per record once its
// segment is stored.
type segmentWriter struct {
	s3       *minio.Client
	bucket   string
	part     int
	buf      bytes.Buffer
	recs     []*record
	first    time.Time
	flushes  chan segmentFlush
	wg       sync.WaitGroup
	onCommit func(r *record, at time.Time)
	bytes    atomic.Int64
	objects  atomic.Int64
	errs     atomic.Int64
}

type segmentFlush struct {
	key  string
	body []byte
	recs []*record
}

func newSegmentWriter(s3 *minio.Client, bucket string, part int, onCommit func(*record, time.Time)) *segmentWriter {
	w := &segmentWriter{s3: s3, bucket: bucket, part: part, flushes: make(chan segmentFlush, 64), onCommit: onCommit}
	w.wg.Add(1)
	go w.loop()
	return w
}

func (w *segmentWriter) add(r *record) {
	if len(w.recs) == 0 {
		w.first = time.Now()
	}
	line, _ := json.Marshal(segmentLine{
		Chain: r.Chain, Seq: r.Seq, EventID: r.EventID, EventType: r.EventType, OccurredAt: r.OccurredAt,
		Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, KeyID: keyID,
		PayloadEnc:  base64.StdEncoding.EncodeToString(r.PayloadEnc),
		PayloadHash: hex.EncodeToString(r.PayloadHash[:]),
		PrevHash:    hex.EncodeToString(r.PrevHash), ChainHash: hex.EncodeToString(r.ChainHash),
	})
	w.buf.Write(line)
	w.buf.WriteByte('\n')
	w.recs = append(w.recs, r)
	if len(w.recs) >= segmentMaxRecords {
		w.flush()
	}
}

// tick flushes a segment that has waited segmentMaxAge.
func (w *segmentWriter) tick() {
	if len(w.recs) > 0 && time.Since(w.first) >= segmentMaxAge {
		w.flush()
	}
}

func (w *segmentWriter) flush() {
	if len(w.recs) == 0 {
		return
	}
	f := w.recs[0]
	key := fmt.Sprintf("segments/p%02d/%s-%d-%d.ndjson", w.part, time.Now().UTC().Format("20060102T150405.000"), f.Offset, len(w.recs))
	body := bytes.Clone(w.buf.Bytes())
	w.flushes <- segmentFlush{key: key, body: body, recs: w.recs}
	w.buf.Reset()
	w.recs = nil
}

func (w *segmentWriter) loop() {
	defer w.wg.Done()
	for f := range w.flushes {
		_, err := w.s3.PutObject(context.Background(), w.bucket, f.key, bytes.NewReader(f.body), int64(len(f.body)),
			minio.PutObjectOptions{
				ContentType:     "application/x-ndjson",
				Mode:            minio.Compliance,
				RetainUntilDate: time.Now().Add(segmentRetention),
			})
		now := time.Now()
		if err != nil {
			w.errs.Add(1)
			continue
		}
		w.bytes.Add(int64(len(f.body)))
		w.objects.Add(1)
		for _, r := range f.recs {
			w.onCommit(r, now)
		}
	}
}

func (w *segmentWriter) close() {
	w.flush()
	close(w.flushes)
	w.wg.Wait()
}

// ---------------------------------------------------------------- OpenSearch

const (
	osIndex      = "audit"
	osBulkDocs   = 1000
	osBulkMaxAge = time.Second
	osQueue      = 200_000
)

const osMapping = `{
  "settings": {"number_of_shards": 3, "number_of_replicas": 0, "refresh_interval": "5s"},
  "mappings": {"properties": {
    "chain": {"type": "keyword"}, "seq": {"type": "long"}, "event_id": {"type": "keyword"},
    "event_type": {"type": "keyword"}, "domain": {"type": "keyword"}, "aggregate_id": {"type": "keyword"},
    "actor_id": {"type": "keyword"}, "correlation_id": {"type": "keyword"}, "channel": {"type": "short"},
    "occurred_at": {"type": "date"}
  }}
}`

type osDoc struct {
	Chain         string    `json:"chain"`
	Seq           int64     `json:"seq"`
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	Domain        string    `json:"domain"`
	AggregateID   string    `json:"aggregate_id"`
	ActorID       string    `json:"actor_id"`
	CorrelationID string    `json:"correlation_id"`
	Channel       int16     `json:"channel"`
	OccurredAt    time.Time `json:"occurred_at"`
}

// osSink bulk-indexes metadata off the commit path, as a separate consumer
// group would. Indexed and blocked count back-pressure and progress.
type osSink struct {
	url     string
	http    *http.Client
	queue   chan osDoc
	wg      sync.WaitGroup
	indexed atomic.Int64
	errs    atomic.Int64
	blocked atomic.Int64
}

func newOSSink(url string, workers int) *osSink {
	s := &osSink{url: url, http: &http.Client{Timeout: 60 * time.Second}, queue: make(chan osDoc, osQueue)}
	for range workers {
		s.wg.Add(1)
		go s.loop()
	}
	return s
}

func osRequest(ctx context.Context, c *http.Client, method, url, body string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return out, fmt.Errorf("%s %s: %d %s", method, url, resp.StatusCode, out)
	}
	return out, err
}

func resetOSIndex(ctx context.Context, url string) error {
	c := &http.Client{Timeout: 30 * time.Second}
	_, _ = osRequest(ctx, c, http.MethodDelete, url+"/"+osIndex, "")
	_, err := osRequest(ctx, c, http.MethodPut, url+"/"+osIndex, osMapping)
	return err
}

func (s *osSink) add(r *record) {
	d := osDoc{Chain: r.Chain, Seq: r.Seq, EventID: r.EventID, EventType: r.EventType, Domain: r.Domain,
		AggregateID: r.AggregateID, ActorID: r.ActorID, CorrelationID: r.CorrelationID, Channel: r.Channel,
		OccurredAt: r.OccurredAt}
	select {
	case s.queue <- d:
	default:
		s.blocked.Add(1)
		s.queue <- d
	}
}

func (s *osSink) loop() {
	defer s.wg.Done()
	var buf bytes.Buffer
	n := 0
	t := time.NewTicker(osBulkMaxAge)
	defer t.Stop()
	send := func() {
		if n == 0 {
			return
		}
		out, err := osRequest(context.Background(), s.http, http.MethodPost, s.url+"/_bulk", buf.String())
		if err != nil || bytes.Contains(out, []byte(`"errors":true`)) {
			s.errs.Add(1)
		} else {
			s.indexed.Add(int64(n))
		}
		buf.Reset()
		n = 0
	}
	for {
		select {
		case d, ok := <-s.queue:
			if !ok {
				send()
				return
			}
			fmt.Fprintf(&buf, `{"index":{"_index":%q,"_id":%q}}`+"\n", osIndex, d.EventID)
			b, _ := json.Marshal(d)
			buf.Write(b)
			buf.WriteByte('\n')
			n++
			if n >= osBulkDocs {
				send()
			}
		case <-t.C:
			send()
		}
	}
}

func (s *osSink) close() {
	close(s.queue)
	s.wg.Wait()
}

// osSearch runs the multi-field query only OpenSearch serves well: an actor's
// events of one domain on one channel in the last 7 days, newest first.
func osSearch(ctx context.Context, c *http.Client, url, actor, domain string, channel int16) (int, error) {
	q := fmt.Sprintf(`{"size":50,"sort":[{"occurred_at":"desc"}],"query":{"bool":{"filter":[
		{"term":{"actor_id":%q}},{"prefix":{"event_type":%q}},{"term":{"channel":%d}},
		{"range":{"occurred_at":{"gte":"now-7d"}}}]}}}`, actor, domain+".", channel)
	out, err := osRequest(ctx, c, http.MethodPost, url+"/"+osIndex+"/_search", q)
	if err != nil {
		return 0, err
	}
	var res struct {
		Hits struct {
			Hits []json.RawMessage `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(out, &res); err != nil {
		return 0, err
	}
	return len(res.Hits.Hits), nil
}

func osCount(ctx context.Context, url string) (int64, error) {
	c := &http.Client{Timeout: 30 * time.Second}
	_, _ = osRequest(ctx, c, http.MethodPost, url+"/"+osIndex+"/_refresh", "")
	out, err := osRequest(ctx, c, http.MethodGet, url+"/"+osIndex+"/_count", "")
	if err != nil {
		return 0, err
	}
	var res struct {
		Count int64 `json:"count"`
	}
	return res.Count, json.Unmarshal(out, &res)
}
