package utils

import "context"

type Job func() error
type WorkerPool struct {
	ready    chan chan Job
	jobsChan chan Job
	errsChan chan error
	done     chan struct{}
	max      int
	ctx      context.Context
}

func NewWorkerPool(ctx context.Context, maxWorkers int) *WorkerPool {
	p := &WorkerPool{
		ready:    make(chan chan Job),
		jobsChan: make(chan Job),
		errsChan: make(chan error),
		done:     make(chan struct{}),
		max:      maxWorkers,
		ctx:      ctx,
	}
	go p.run()
	return p
}

func (p *WorkerPool) run() {
	var (
		workers  int
		jobsChan = p.jobsChan
		ctxDone  = p.ctx.Done()
		closing  bool
		queue    []Job
		idle     []chan Job
	)
	for {
		select {
		case j, ok := <-jobsChan:
			if !ok {
				jobsChan = nil
				closing = true
				break
			}
			switch {
			case len(idle) > 0:
				inbox := idle[len(idle)-1]
				idle = idle[:len(idle)-1]
				inbox <- j
			case workers < p.max:
				workers++
				go worker(make(chan Job), p.ready, j, p.errsChan, p.ctx)
			default:
				queue = append(queue, j)
			}
		case inbox := <-p.ready:
			if len(queue) > 0 {
				j := queue[0]
				queue = queue[1:]
				inbox <- j
			} else {
				idle = append(idle, inbox)
			}
		case <-ctxDone:
			ctxDone = nil
			jobsChan = nil
			closing = true
			queue = nil
		}
		if closing && len(queue) == 0 && len(idle) == workers {
			for _, in := range idle {
				close(in)
			}
			close(p.done)
			return
		}
	}
}

func worker(inbox chan Job, ready chan<- chan Job, first Job, errsChan chan<- error, ctx context.Context) {
	for j, ok := first, true; ok; j, ok = <-inbox {
		select {
		case errsChan <- j():
		case <-ctx.Done():
			return
		}
		// run reads ready until every worker is idle, so this never blocks forever.
		ready <- inbox
	}
}
