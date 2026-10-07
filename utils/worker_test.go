package utils

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const waitTimeout = 2 * time.Second

func submitAll(t *testing.T, p *WorkerPool, jobs []Job) {
	t.Helper()
	go func() {
		for range p.errsChan {
		}
	}()
	for i, j := range jobs {
		select {
		case p.jobsChan <- j:
		case <-time.After(waitTimeout):
			t.Fatalf("submit of job %d blocked", i)
		}
	}
	close(p.jobsChan)
}

func waitDone(t *testing.T, p *WorkerPool) {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(waitTimeout):
		t.Fatal("pool did not close done after jobs channel was closed")
	}
}

func TestWorkerPool_RunsEveryJob(t *testing.T) {
	tests := []struct {
		name       string
		maxWorkers int
		jobs       int
	}{
		{name: "single worker", maxWorkers: 1, jobs: 50},
		{name: "fewer jobs than workers", maxWorkers: 8, jobs: 3},
		{name: "more jobs than workers", maxWorkers: 4, jobs: 500},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewWorkerPool(t.Context(), tt.maxWorkers)

			var ran atomic.Int64
			jobs := make([]Job, tt.jobs)
			for i := range jobs {
				jobs[i] = func() error {
					ran.Add(1)
					return nil
				}
			}

			submitAll(t, p, jobs)
			waitDone(t, p)

			assert.Equal(t, int64(tt.jobs), ran.Load())
		})
	}
}

func TestWorkerPool_RunsEachJobOnce(t *testing.T) {
	const n = 200
	p := NewWorkerPool(t.Context(), 4)

	var counts [n]atomic.Int32
	jobs := make([]Job, n)
	for i := range jobs {
		jobs[i] = func() error {
			counts[i].Add(1)
			return nil
		}
	}

	submitAll(t, p, jobs)
	waitDone(t, p)

	for i := range counts {
		assert.Equalf(t, int32(1), counts[i].Load(), "job %d", i)
	}
}

func TestWorkerPool_RespectsMaxWorkers(t *testing.T) {
	const maxWorkers = 3
	p := NewWorkerPool(t.Context(), maxWorkers)

	var active, peak atomic.Int32
	jobs := make([]Job, 30)
	for i := range jobs {
		jobs[i] = func() error {
			cur := active.Add(1)
			for {
				old := peak.Load()
				if cur <= old || peak.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			active.Add(-1)
			return nil
		}
	}

	submitAll(t, p, jobs)
	waitDone(t, p)

	assert.LessOrEqual(t, peak.Load(), int32(maxWorkers))
}

func TestWorkerPool_RunsJobsConcurrently(t *testing.T) {
	const maxWorkers = 4
	p := NewWorkerPool(t.Context(), maxWorkers)

	// Each job blocks until all maxWorkers jobs have started, so the
	// barrier only opens if the pool runs them in parallel.
	var started sync.WaitGroup
	started.Add(maxWorkers)
	release := make(chan struct{})
	jobs := make([]Job, maxWorkers)
	for i := range jobs {
		jobs[i] = func() error {
			started.Done()
			<-release
			return nil
		}
	}

	submitAll(t, p, jobs)

	allStarted := make(chan struct{})
	go func() {
		started.Wait()
		close(allStarted)
	}()
	select {
	case <-allStarted:
	case <-time.After(waitTimeout):
		t.Fatal("jobs did not run concurrently")
	}
	close(release)
	waitDone(t, p)
}

func TestWorkerPool_ClosesWithNoJobs(t *testing.T) {
	p := NewWorkerPool(t.Context(), 2)

	submitAll(t, p, nil)

	waitDone(t, p)
}

func TestWorkerPool_DrainsQueueOnClose(t *testing.T) {
	p := NewWorkerPool(t.Context(), 1)

	// The first job holds the only worker, so the rest queue up and must
	// still run after the jobs channel is closed.
	release := make(chan struct{})
	var ran atomic.Int64
	jobs := []Job{func() error {
		<-release
		ran.Add(1)
		return nil
	}}
	for range 10 {
		jobs = append(jobs, func() error {
			ran.Add(1)
			return nil
		})
	}

	submitAll(t, p, jobs)
	close(release)
	waitDone(t, p)

	require.Equal(t, int64(len(jobs)), ran.Load())
}
