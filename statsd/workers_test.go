package statsd

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorker(t *testing.T) {
	var callbackMetric metric
	w := worker{
		callback: func(m metric) error {
			callbackMetric = m
			return nil
		},
		inputMetrics: make(chan metric, 1),
	}

	var wg sync.WaitGroup
	wg.Add(1)
	stop := make(chan struct{})

	go w.pullMetric(&wg, stop)

	m := metric{name: "test metric"}
	w.inputMetrics <- m

	// wait for up to 3s for the worker to process the metric
	for i := 0; i < 3; i++ {
		if len(w.inputMetrics) == 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}

	close(stop)
	wg.Wait()
	assert.Equal(t, callbackMetric.name, "test metric")
}

func TestWorkerDirectProcess(t *testing.T) {
	var callbackMetric metric
	w := worker{
		callback: func(m metric) error {
			callbackMetric = m
			return nil
		},
	}

	m := metric{name: "test metric"}
	w.process(m)
	assert.Equal(t, callbackMetric.name, "test metric")
}

func TestWorkerPoolCreation(t *testing.T) {
	p := newWorkerPool(10, false)
	require.NotNil(t, p)
	assert.Equal(t, p.bufferSize, 10)
	assert.Equal(t, p.ChannelMode, false)
	assert.Len(t, p.stopChan, 0)
	assert.Len(t, p.workers, 0)

	p = newWorkerPool(10, true)
	require.NotNil(t, p)
	assert.Equal(t, p.bufferSize, 10)
	assert.Equal(t, p.ChannelMode, true)
	assert.Len(t, p.stopChan, 0)
	assert.Len(t, p.workers, 0)
}

func TestWorkerPoolAddWorker(t *testing.T) {
	p := newWorkerPool(10, false)
	require.NotNil(t, p)

	cb := func(metric) error { return nil }
	p.addWorker(cb)

	assert.Len(t, p.workers, 1)
}

func TestWorkerPoolAddWorkerChannelMode(t *testing.T) {
	p := newWorkerPool(10, true)
	require.NotNil(t, p)

	cb := func(metric) error { return nil }
	p.addWorker(cb)

	assert.Len(t, p.workers, 1)
}

func TestWorkerPoolStop(t *testing.T) {
	p := newWorkerPool(10, false)
	require.NotNil(t, p)

	cb := func(metric) error { return nil }
	p.addWorker(cb)
	assert.Len(t, p.workers, 1)

	p.stop()
	assert.Len(t, p.workers, 0)
}

func TestWorkerPoolStopChannelMode(t *testing.T) {
	p := newWorkerPool(10, true)
	require.NotNil(t, p)

	cb := func(metric) error { return nil }
	p.addWorker(cb)
	assert.Len(t, p.workers, 1)

	p.stop()
	assert.Len(t, p.workers, 0)
}

func TestWorkerPoolProcess(t *testing.T) {
	p := newWorkerPool(10, false)
	require.NotNil(t, p)

	m := metric{name: "test metric"}
	var cbMetric metric

	cb := func(m metric) error {
		cbMetric = m
		return nil
	}
	p.addWorker(cb)
	p.addWorker(cb)
	p.addWorker(cb)
	assert.Len(t, p.workers, 3)

	p.process(m)
	assert.Equal(t, cbMetric.name, "test metric")

	p.stop()
}

func TestWorkerPoolProcessChannelMode(t *testing.T) {
	p := newWorkerPool(10, true)
	require.NotNil(t, p)

	m := metric{name: "test metric"}
	var cbMetric metric
	var cbCalled int32

	cb := func(m metric) error {
		cbMetric = m
		atomic.StoreInt32(&cbCalled, 1)
		return nil
	}
	p.addWorker(cb)
	p.addWorker(cb)
	p.addWorker(cb)
	assert.Len(t, p.workers, 3)

	p.process(m)
	noCallback := true
	for i := 0; i < 3; i++ {
		if atomic.LoadInt32(&cbCalled) == 1 {
			noCallback = false
			break
		}
		time.Sleep(1 * time.Second)
	}
	if noCallback {
		assert.FailNow(t, "Timeout: workerPool didn't process metric")
	}
	assert.Equal(t, cbMetric.name, "test metric")
	p.stop()

}
