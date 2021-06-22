package statsd

import (
	"math/rand"
	"sync"
	"time"
)

type metricHandler struct {
	pool       *bufferPool
	buffer     *statsdBuffer
	sender     *sender
	random     *rand.Rand
	randomLock sync.Mutex
	sync.Mutex
}

func newMetricHandler(pool *bufferPool, sender *sender) *metricHandler {
	// Each metricHandler uses its own random source and random lock to prevent
	// handlers in separate goroutines from contending for the lock on the
	// "math/rand" package-global random source (e.g. calls like
	// "rand.Float64()" must acquire a shared lock to get the next
	// pseudorandom number).
	// Note that calling "time.Now().UnixNano()" repeatedly quickly may return
	// very similar values. That's fine for seeding the handler-specific random
	// source because we just need an evenly distributed stream of float values.
	// Do not use this random source for cryptographic randomness.
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &metricHandler{
		pool:   pool,
		sender: sender,
		buffer: pool.borrowBuffer(),
		random: random,
	}
}

func (mh *metricHandler) processMetric(m metric) error {
	if !shouldSample(m.rate, mh.random, &mh.randomLock) {
		return nil
	}
	mh.Lock()
	var err error
	if err = mh.writeMetricUnsafe(m); err == errBufferFull {
		mh.flushUnsafe()
		err = mh.writeMetricUnsafe(m)
	}
	mh.Unlock()
	return err
}

func (mh *metricHandler) writeAggregatedMetricUnsafe(m metric, metricSymbol []byte) error {
	globalPos := 0

	// first check how much data we can write to the buffer:
	//   +3 + len(metricSymbol) because the message will include '|<metricSymbol>|#' before the tags
	//   +1 for the potential line break at the start of the metric
	tagsSize := len(m.stags) + 4 + len(metricSymbol)
	for _, t := range m.globalTags {
		tagsSize += len(t) + 1
	}

	for {
		pos, err := mh.buffer.writeAggregated(metricSymbol, m.namespace, m.globalTags, m.name, m.fvalues[globalPos:], m.stags, tagsSize)
		if err == errPartialWrite {
			// We successfully wrote part of the histogram metrics.
			// We flush the current buffer and finish the histogram
			// in a new one.
			mh.flushUnsafe()
			globalPos += pos
		} else {
			return err
		}
	}
}

func (mh *metricHandler) writeMetricUnsafe(m metric) error {
	switch m.metricType {
	case gauge:
		return mh.buffer.writeGauge(m.namespace, m.globalTags, m.name, m.fvalue, m.tags, m.rate)
	case count:
		return mh.buffer.writeCount(m.namespace, m.globalTags, m.name, m.ivalue, m.tags, m.rate)
	case histogram:
		return mh.buffer.writeHistogram(m.namespace, m.globalTags, m.name, m.fvalue, m.tags, m.rate)
	case distribution:
		return mh.buffer.writeDistribution(m.namespace, m.globalTags, m.name, m.fvalue, m.tags, m.rate)
	case set:
		return mh.buffer.writeSet(m.namespace, m.globalTags, m.name, m.svalue, m.tags, m.rate)
	case timing:
		return mh.buffer.writeTiming(m.namespace, m.globalTags, m.name, m.fvalue, m.tags, m.rate)
	case event:
		return mh.buffer.writeEvent(*m.evalue, m.globalTags)
	case serviceCheck:
		return mh.buffer.writeServiceCheck(*m.scvalue, m.globalTags)
	case histogramAggregated:
		return mh.writeAggregatedMetricUnsafe(m, histogramSymbol)
	case distributionAggregated:
		return mh.writeAggregatedMetricUnsafe(m, distributionSymbol)
	case timingAggregated:
		return mh.writeAggregatedMetricUnsafe(m, timingSymbol)
	default:
		return nil
	}
}

func (mh *metricHandler) flush() {
	mh.Lock()
	mh.flushUnsafe()
	mh.Unlock()
}

func (mh *metricHandler) pause() {
	mh.Lock()
}

func (mh *metricHandler) unpause() {
	mh.Unlock()
}

// flush the current buffer. Lock must be held by caller.
// flushed buffer written to the network asynchronously.
func (mh *metricHandler) flushUnsafe() {
	if len(mh.buffer.bytes()) > 0 {
		mh.sender.send(mh.buffer)
		mh.buffer = mh.pool.borrowBuffer()
	}
}
