package wazero_cron

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"

	"github.com/pantopic/ext-cluster/host-wazero/pb"
	"github.com/pantopic/wazero-pool"
)

// Name is the name of this host module.
const Name = "pantopic/ext-cron"

var (
	ctxKeyMeta  = Name + `/meta`
	ctxKeyLocal = Name + `/local`
	ctxKeyMutex = Name + `/mutex`
)

type meta struct {
	ptrVal    uint32
	ptrBufCap uint32
	ptrBufLen uint32
	ptrBuf    uint32
}

type Manager interface {
	Start(ctx context.Context, pool wazeropool.Instance, term uint64, shard *pb.Shard) (err error)
	Stop(ctx context.Context, pool wazeropool.Instance, term uint64, shard *pb.Shard) (err error)
	Reload(ctx context.Context, pool wazeropool.Instance, term uint64, shard *pb.Shard) (err error)
}

type hostModule struct {
	sync.RWMutex
}

func New(opts ...Option) *hostModule {
	h := &hostModule{}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

func (h *hostModule) Name() string {
	return Name
}

// InitContext retrieves the meta page from the wasm module
func (h *hostModule) InitContext(ctx context.Context, m api.Module) (context.Context, error) {
	fn := m.ExportedFunction(`__cron`)
	if fn == nil {
		return ctx, nil
	}
	stack, err := fn.Call(ctx)
	if err != nil {
		return ctx, err
	}
	meta := &meta{}
	ptr := uint32(stack[0])
	for i, v := range []*uint32{
		&meta.ptrVal,
		&meta.ptrBufCap,
		&meta.ptrBufLen,
		&meta.ptrBuf,
	} {
		*v = readUint32(m, ptr+uint32(4*i))
	}
	ctx = context.WithValue(ctx, ctxKeyMeta, meta)
	return ctx, nil
}

// ContextCopy populates dst context with the meta page from src context.
func (h *hostModule) ContextCopy(dst, src context.Context) context.Context {
	if v := src.Value(ctxKeyMeta); v != nil {
		dst = context.WithValue(dst, ctxKeyMeta, v.(*meta))
		if v := src.Value(ctxKeyMutex); v != nil {
			dst = context.WithValue(dst, ctxKeyMutex, v.(*sync.RWMutex))
			if v := src.Value(ctxKeyLocal); v != nil {
				dst = context.WithValue(dst, ctxKeyLocal, v.(map[string]time.Timer))
			} else {
				dst = context.WithValue(dst, ctxKeyLocal, make(map[string]time.Timer))
			}
		} else {
			dst = context.WithValue(dst, ctxKeyMutex, &sync.RWMutex{})
		}
	}
	return dst
}

// Register instantiates the host module, making it available to all module instances in this runtime
func (h *hostModule) Register(ctx context.Context, r wazero.Runtime) (err error) {
	builder := r.NewHostModuleBuilder(Name)
	register := func(name string, fn func(ctx context.Context, m api.Module, stack []uint64)) {
		builder = builder.NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(fn), nil, nil).Export(name)
	}
	for name, fn := range map[string]any{
		"__cron_start": func(ctx context.Context, mu *sync.RWMutex, m map[string]*time.Ticker, name []byte, d time.Duration) {
			mu.Lock()
			defer mu.Unlock()
			k := string(name)
			if _, ok := m[k]; ok {
				return
			}
			t := time.NewTicker(d)
			m[k] = t
			go func() {
				for range t.C {
					meta := get[*meta](ctx, ctxKeyMeta)
					wazeropool.FromContext(ctx).Run(func(mod api.Module) {
						setName(mod, meta, name)
						if _, err = mod.ExportedFunction("__cron_call").Call(ctx); err != nil {
							log.Printf("Error triggering __cron_call: %v", err)
							return
						}
					})
				}
			}()
		},
		"__cron_stop": func(mu *sync.RWMutex, m map[string]*time.Ticker, name []byte) {
			mu.Lock()
			defer mu.Unlock()
			k := string(name)
			t, ok := m[k]
			if ok {
				t.Stop()
				delete(m, k)
			}
		},
		"__cron_exists": func(mu *sync.RWMutex, m map[string]*time.Ticker, name []byte) bool {
			mu.RLock()
			defer mu.RUnlock()
			_, ok := m[string(name)]
			return ok
		},
		"__cron_clear": func(mu *sync.RWMutex, m map[string]*time.Ticker) {
			mu.Lock()
			defer mu.Unlock()
			for k, t := range m {
				t.Stop()
				delete(m, k)
			}
		},
	} {
		switch fn := fn.(type) {
		case func(ctx context.Context, mu *sync.RWMutex, m map[string]*time.Ticker, name []byte, d time.Duration):
			register(name, func(ctx context.Context, mod api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				fn(ctx, get[*sync.RWMutex](ctx, ctxKeyMutex), get[map[string]*time.Ticker](ctx, ctxKeyLocal), getName(mod, meta), time.Duration(getVal(mod, meta))*time.Millisecond)
			})
		case func(mu *sync.RWMutex, m map[string]*time.Ticker, name []byte):
			register(name, func(ctx context.Context, mod api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				fn(get[*sync.RWMutex](ctx, ctxKeyMutex), get[map[string]*time.Ticker](ctx, ctxKeyLocal), getName(mod, meta))
			})
		case func(mu *sync.RWMutex, m map[string]*time.Ticker, name []byte) bool:
			register(name, func(ctx context.Context, mod api.Module, stack []uint64) {
				meta := get[*meta](ctx, ctxKeyMeta)
				ret := fn(get[*sync.RWMutex](ctx, ctxKeyMutex), get[map[string]*time.Ticker](ctx, ctxKeyLocal), getName(mod, meta))
				var val uint64
				if ret {
					val = 1
				}
				setVal(mod, meta, val)
			})
		default:
			log.Panicf("Method signature implementation missing: %#v", fn)
		}
	}
	_, err = builder.Instantiate(ctx)
	return
}

func (h *hostModule) Start(ctx context.Context, pool wazeropool.Instance, term uint64, shard *pb.Shard) (err error) {
	return
}

func (h *hostModule) Stop(ctx context.Context, pool wazeropool.Instance, term uint64, shard *pb.Shard) (err error) {
	return
}

func (h *hostModule) Reload(ctx context.Context, pool wazeropool.Instance, term uint64, shard *pb.Shard) (err error) {
	return
}

func get[T any](ctx context.Context, key string) T {
	v := ctx.Value(key)
	if v == nil {
		log.Panicf("Context item missing %s", key)
	}
	return v.(T)
}

func getName(m api.Module, meta *meta) []byte {
	return read(m, meta.ptrBuf, meta.ptrBufLen, meta.ptrBufCap)
}

func getNameCopy(m api.Module, meta *meta) []byte {
	return append([]byte(nil), getName(m, meta)...)
}

func nameBuf(m api.Module, meta *meta) []byte {
	return read(m, meta.ptrBuf, 0, meta.ptrBufCap)
}

func setVal(m api.Module, meta *meta, val uint64) {
	writeUint64(m, meta.ptrVal, val)
}

func getVal(m api.Module, meta *meta) (val uint64) {
	return readUint64(m, meta.ptrVal)
}

func setName(m api.Module, meta *meta, b []byte) {
	buf := nameBuf(m, meta)
	copy(buf[:cap(buf)], b)
	writeUint32(m, meta.ptrBufLen, uint32(len(b)))
}

func read(m api.Module, ptrData, ptrLen, ptrMax uint32) (buf []byte) {
	buf, ok := m.Memory().Read(ptrData, readUint32(m, ptrMax))
	if !ok {
		log.Panicf("Memory.Read(%d, %d) out of range", ptrData, ptrLen)
	}
	return buf[:readUint32(m, ptrLen)]
}

func readUint32(m api.Module, ptr uint32) (val uint32) {
	val, ok := m.Memory().ReadUint32Le(ptr)
	if !ok {
		log.Panicf("Memory.Read(%d) out of range", ptr)
	}
	return
}

func readUint64(m api.Module, ptr uint32) (val uint64) {
	val, ok := m.Memory().ReadUint64Le(ptr)
	if !ok {
		log.Panicf("Memory.Read(%d) out of range", ptr)
	}
	return
}

func writeUint32(m api.Module, ptr uint32, val uint32) {
	if ok := m.Memory().WriteUint32Le(ptr, val); !ok {
		log.Panicf("Memory.Read(%d) out of range", ptr)
	}
}

func writeUint64(m api.Module, ptr uint32, val uint64) {
	if ok := m.Memory().WriteUint64Le(ptr, val); !ok {
		log.Panicf("Memory.Read(%d) out of range", ptr)
	}
}
