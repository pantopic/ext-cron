package cron

import (
	"time"
)

func Init(fn cronFunc, opts ...Option) {
	cron = fn
	for _, opt := range opts {
		opt()
	}
}

func Start(name []byte, d time.Duration) {
	setName(name)
	val = uint64(d / time.Millisecond)
	_start()
}

func Stop(name []byte) {
	setName(name)
	_stop()
}

func Exists(name []byte) bool {
	setName(name)
	_exists()
	return val > 0
}
