package arc

import (
	"fmt"
	"reflect"
)

// Metrics receives cache counters and gauges. Implementations must be safe
// for concurrent use and should return promptly.
type Metrics interface {
	Increment(key string)
	Gauge(key string, value any)
}

type noopMetrics struct{}

func (noopMetrics) Increment(string)  {}
func (noopMetrics) Gauge(string, any) {}

type metricNames struct {
	hit        string
	miss       string
	expired    string
	expiredGet string
	deleted    string
	length     string
}

func newMetricNames(name string) metricNames {
	return metricNames{
		hit:        fmt.Sprintf("cache.%s.hit", name),
		miss:       fmt.Sprintf("cache.%s.miss", name),
		expired:    fmt.Sprintf("cache.%s.expired", name),
		expiredGet: fmt.Sprintf("cache.%s.expired_get", name),
		deleted:    fmt.Sprintf("cache.%s.delete", name),
		length:     fmt.Sprintf("cache.%s.len", name),
	}
}

func isNilMetrics(metrics Metrics) bool {
	if metrics == nil {
		return true
	}
	value := reflect.ValueOf(metrics)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
