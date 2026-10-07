package storagebench_test

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	mrand "math/rand/v2"
	"time"

	"github.com/hunderaweke/dps-audit-service/internal/const/events"
)

const (
	partitions    = 12
	aggregates    = 200_000
	actors        = 50_000
	domainBuckets = 16
	dupRate       = 0.01
	keyID         = "bench-key-1"
)

// record is one audit record as the consumer would build it. Seq, PrevHash
// and ChainHash are set by the writer when the record joins its chain.
type record struct {
	Chain         string
	Seq           int64
	EventID       string
	EventType     string
	Domain        string
	DomainBucket  int16
	AggregateID   string
	ActorID       string
	CorrelationID string
	Channel       int16
	OccurredAt    time.Time
	Topic         string
	Partition     int32
	Offset        int64
	PayloadEnc    []byte
	PayloadHash   [32]byte
	PrevHash      []byte
	ChainHash     []byte

	created time.Time // when the record was due; latency is measured from here
	ysqlAck time.Time
	dup     bool // the generator re-sent an earlier event_id
}

// weights skews traffic toward the money path, as production will be.
var weights = map[string]int{
	"ledger": 30, "payment": 25, "auth": 12, "notification": 8, "fee": 5, "account": 3,
	"tpi": 3, "airtime": 2, "utility": 2, "onboarding": 2, "adminops": 1, "fuel": 1,
	"lending": 1, "assistant": 1, "ticketing": 1, "airline": 1, "dwh": 1,
}

type eventPick struct {
	domain string
	topic  string
	types  []events.Name
}

var picks, pickTable = buildPicks()

func buildPicks() ([]eventPick, []int) {
	var ps []eventPick
	var table []int
	for _, d := range events.Domains() {
		ps = append(ps, eventPick{domain: d.Name, topic: d.Topic, types: d.Events})
		for range weights[d.Name] {
			table = append(table, len(ps)-1)
		}
	}
	return ps, table
}

// generator produces the records of one Kafka partition of every domain topic.
// It is deterministic per partition, owned by one goroutine, and keeps running
// across scenarios so chains and offsets continue.
type generator struct {
	part    int
	rnd     *mrand.Rand
	zipf    *mrand.Zipf
	aead    cipher.AEAD
	offsets map[string]int64
	n       int64
	recent  []*record

	// ground truth for search checks: records per aggregate, excluding dups.
	perAggregate map[string]int
	samples      sampleSet
}

type sampleSet struct {
	hotAggregates, coldAggregates, correlations, actors []string
}

func newGenerator(part int, key []byte) *generator {
	rnd := mrand.New(mrand.NewPCG(42, uint64(part)))
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		panic(err)
	}
	return &generator{
		part:         part,
		rnd:          rnd,
		zipf:         mrand.NewZipf(rnd, 1.1, 1, aggregates-1),
		aead:         aead,
		offsets:      map[string]int64{},
		perAggregate: map[string]int{},
	}
}

func (g *generator) next(due time.Time) *record {
	g.n++
	if len(g.recent) > 0 && g.rnd.Float64() < dupRate {
		orig := g.recent[g.rnd.IntN(len(g.recent))]
		r := *orig
		r.dup = true
		r.Offset = g.offsets[r.Topic]
		g.offsets[r.Topic]++
		r.created = due
		return &r
	}

	p := picks[pickTable[g.rnd.IntN(len(pickTable))]]
	agg := g.zipf.Uint64()
	r := &record{
		Chain:         fmt.Sprintf("%s/%d", p.topic, g.part),
		EventID:       fmt.Sprintf("p%02d-%012d", g.part, g.n),
		EventType:     string(p.types[g.rnd.IntN(len(p.types))]),
		Domain:        p.domain,
		DomainBucket:  int16(g.rnd.IntN(domainBuckets)),
		AggregateID:   fmt.Sprintf("agg-%06d-%02d", agg, g.part), // same aggregate always lands on the same partition
		ActorID:       fmt.Sprintf("actor-%05d", g.rnd.IntN(actors)),
		CorrelationID: fmt.Sprintf("corr-%02d-%d", g.part, g.n/4), // about 4 events per customer action
		Channel:       int16(1 + g.rnd.IntN(6)),
		OccurredAt:    due.UTC().Truncate(time.Microsecond), // hash what the database stores: timestamptz keeps microseconds
		Topic:         p.topic,
		Partition:     int32(g.part),
		Offset:        g.offsets[p.topic],
		created:       due,
	}
	g.offsets[p.topic]++

	plain := make([]byte, 1024+g.rnd.IntN(1024))
	_, _ = rand.Read(plain)
	r.PayloadHash = sha256.Sum256(plain)
	nonce := make([]byte, g.aead.NonceSize())
	_, _ = rand.Read(nonce)
	r.PayloadEnc = g.aead.Seal(nonce, nonce, plain, []byte(r.EventID))

	g.perAggregate[r.AggregateID]++
	g.sample(r, agg)
	if len(g.recent) < 256 {
		g.recent = append(g.recent, r)
	} else {
		g.recent[g.rnd.IntN(len(g.recent))] = r
	}
	return r
}

func (g *generator) sample(r *record, rank uint64) {
	s := &g.samples
	if rank < 100 && len(s.hotAggregates) < 200 {
		s.hotAggregates = append(s.hotAggregates, r.AggregateID)
	}
	if rank > 10_000 && len(s.coldAggregates) < 200 && g.rnd.IntN(50) == 0 {
		s.coldAggregates = append(s.coldAggregates, r.AggregateID)
	}
	if len(s.correlations) < 200 && g.rnd.IntN(200) == 0 {
		s.correlations = append(s.correlations, r.CorrelationID)
	}
	if len(s.actors) < 200 && g.rnd.IntN(200) == 0 {
		s.actors = append(s.actors, r.ActorID)
	}
}

// decrypt opens a payload sealed by any generator (they share the key).
func (g *generator) decrypt(r *record) ([]byte, error) {
	ns := g.aead.NonceSize()
	if len(r.PayloadEnc) < ns {
		return nil, fmt.Errorf("payload too short")
	}
	return g.aead.Open(nil, r.PayloadEnc[:ns], r.PayloadEnc[ns:], []byte(r.EventID))
}

// chainHash links a record to its predecessor. The byte layout is fixed:
// prev || seq (8 bytes BE) || event_id || 0 || event_type || 0 ||
// occurred_at (unix nanos, 8 bytes BE) || topic || 0 || partition (4) || offset (8) || payload_sha256.
func chainHash(prev []byte, r *record) []byte {
	h := sha256.New()
	var b [8]byte
	h.Write(prev)
	binary.BigEndian.PutUint64(b[:], uint64(r.Seq))
	h.Write(b[:])
	h.Write([]byte(r.EventID))
	h.Write([]byte{0})
	h.Write([]byte(r.EventType))
	h.Write([]byte{0})
	binary.BigEndian.PutUint64(b[:], uint64(r.OccurredAt.UnixNano()))
	h.Write(b[:])
	h.Write([]byte(r.Topic))
	h.Write([]byte{0})
	binary.BigEndian.PutUint32(b[:4], uint32(r.Partition))
	h.Write(b[:4])
	binary.BigEndian.PutUint64(b[:], uint64(r.Offset))
	h.Write(b[:])
	h.Write(r.PayloadHash[:])
	return h.Sum(nil)
}

// chainState is the in-memory head of one chain (one topic partition), owned
// by the writer of that partition. offset is the highest Kafka offset written.
type chainState struct {
	seq    int64
	hash   []byte
	offset int64
}

func (c *chainState) link(r *record) {
	c.seq++
	r.Seq = c.seq
	r.PrevHash = c.hash
	r.ChainHash = chainHash(c.hash, r)
	c.hash = r.ChainHash
	c.offset = r.Offset
}
