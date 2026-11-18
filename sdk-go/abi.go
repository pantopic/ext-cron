package cron

import (
	"unsafe"
)

type cronFunc func(name []byte)

var (
	meta   = make([]uint32, 4)
	val    uint64
	bufCap uint32 = 64
	bufLen uint32
	buf    = make([]byte, bufCap)

	cron cronFunc
)

//export __cron
func __cron() uint32 {
	for i, p := range []unsafe.Pointer{
		unsafe.Pointer(&val),
		unsafe.Pointer(&bufCap),
		unsafe.Pointer(&bufLen),
		unsafe.Pointer(&buf[0]),
	} {
		meta[i] = uint32(uintptr(p))
	}
	return uint32(uintptr(unsafe.Pointer(&meta[0])))
}

//go:wasm-module pantopic/ext-cron
//export __cron_start
func _start()

//go:wasm-module pantopic/ext-cron
//export __cron_stop
func _stop()

//go:wasm-module pantopic/ext-cron
//export __cron_exists
func _exists()

//export __cron_call
func __cron_call() {
	cron(getName())
}

func setName(b []byte) {
	copy(buf, b)
	bufLen = uint32(len(b))
}

func getName() []byte {
	return buf[:bufLen]
}

var _ = __cron
var _ = __cron_call
