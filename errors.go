package gopubsub

import "errors"

var (
	ErrTimeout  = errors.New("timeout")
	ErrNotFound = errors.New("not found")
)
