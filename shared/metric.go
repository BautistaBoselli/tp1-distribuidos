package shared

import "time"

type Metric struct {
	interval int
	i        int
	totalI   int
	lastTs   time.Time
	startTs  time.Time
	msgFn    func(total int, elapsed time.Duration, rate float64) string
}

func NewMetric(interval int, msgFn func(total int, elapsed time.Duration, rate float64) string) *Metric {
	return &Metric{
		interval: interval,
		i:        0,
		totalI:   0,
		lastTs:   time.Now(),
		startTs:  time.Now(),
		msgFn:    msgFn,
	}
}

func (m *Metric) Update(i int) {
	m.i += i
	m.totalI += i
	if m.i >= m.interval {
		elapsed := time.Since(m.lastTs)
		m.lastTs = time.Now()
		msg := m.msgFn(m.totalI, elapsed, float64(m.i)/elapsed.Seconds())
		log.Infof(msg)
		m.i = 0
	}
}

func (m *Metric) Finalize(msgFn func(total int, elapsed time.Duration, rate float64) string) {
	elapsed := time.Since(m.startTs)
	msg := msgFn(m.totalI, elapsed, float64(m.totalI)/elapsed.Seconds())
	log.Infof(msg)
}
