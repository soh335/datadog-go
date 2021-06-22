package statsd

import (
	"sync"
	"sync/atomic"
)

type metricHandlerCB func(metric) error

type worker struct {
	inputMetrics chan metric
	callback     metricHandlerCB
}

func (w *worker) pullMetric(wg *sync.WaitGroup, stopChan chan struct{}) {
	for {
		select {
		case m := <-w.inputMetrics:
			w.process(m)
		case <-stopChan:
			wg.Done()
			return
		}
	}
}

func (w *worker) process(m metric) error {
	return w.callback(m)
}

type workerPool struct {
	totalDropped uint64
	wg           sync.WaitGroup
	stopChan     chan struct{}
	bufferSize   int
	workers      []*worker
	ChannelMode  bool
}

func newWorkerPool(bufferSize int, ChannelMode bool) *workerPool {
	return &workerPool{
		stopChan:    make(chan struct{}),
		bufferSize:  bufferSize,
		workers:     []*worker{},
		ChannelMode: ChannelMode,
	}
}

func (p *workerPool) addWorker(callback metricHandlerCB) {
	w := &worker{callback: callback}
	p.workers = append(p.workers, w)

	if p.ChannelMode {
		p.wg.Add(1)
		w.inputMetrics = make(chan metric, p.bufferSize)
		go w.pullMetric(&p.wg, p.stopChan)
	}
}

func (p *workerPool) stop() {
	if p.ChannelMode {
		close(p.stopChan)
		p.wg.Wait()
	}

	p.workers = p.workers[:0]
}

func (p *workerPool) process(m metric) error {
	h := hashString32(m.name)
	workerID := h % uint32(len(p.workers))

	if p.ChannelMode {
		select {
		case p.workers[workerID].inputMetrics <- m:
		default:
			atomic.AddUint64(&p.totalDropped, 1)
		}
		return nil
	}
	return p.workers[workerID].process(m)
}

func (p *workerPool) processBlocking(m metric) error {
	h := hashString32(m.name)
	workerID := h % uint32(len(p.workers))
	return p.workers[workerID].process(m)
}
