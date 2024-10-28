package gopubsub

import "time"

func Wait(ch <-chan any, timeout time.Duration) (any, error) {
	timer := time.NewTimer(timeout)
	select {
	case <-timer.C:
		return nil, ErrTimeout
	case data := <-ch:
		return data, nil
	}
}
