package wazero_cron

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"

	"github.com/pantopic/ext-util/host-wazero"
	"github.com/pantopic/wazero-pool"
)

// Name is the name of this host module.
const Name = "pantopic/ext-cron"

var (
	ctxKeyMeta  = Name + `/meta`
	ctxKeyLocal = Name + `/local`
)

type meta struct {
	ptrVal    uint32
	ptrBufCap uint32
	ptrBufLen uint32
	ptrBuf    uint32
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
		*v = wazero_util.ReadUint32(m, ptr+uint32(4*i))
	}
	ctx = context.WithValue(ctx, ctxKeyMeta, meta)
	return ctx, nil
}

// ContextCopy populates dst context with the meta page from src context.
func (h *hostModule) ContextCopy(dst, src context.Context) context.Context {
	if v := src.Value(ctxKeyMeta); v != nil {
		dst = context.WithValue(dst, ctxKeyMeta, v.(*meta))
		if v := src.Value(ctxKeyLocal); v != nil {
			dst = context.WithValue(dst, ctxKeyLocal, v.(map[string]*time.Ticker))
		} else {
			dst = context.WithValue(dst, ctxKeyLocal, make(map[string]*time.Ticker))
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
		"__cron_start": func(ctx context.Context, m map[string]*time.Ticker, name []byte, d time.Duration) {
			k := string(name)
			if _, ok := m[k]; ok {
				return
			}
			t := time.NewTicker(d)
			m[k] = t
			go func() {
				for range t.C {
					meta := wazero_util.Get[*meta](ctx, ctxKeyMeta)
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
		"__cron_stop": func(m map[string]*time.Ticker, name []byte) {
			k := string(name)
			t, ok := m[k]
			if ok {
				t.Stop()
				delete(m, k)
			}
		},
		"__cron_exists": func(m map[string]*time.Ticker, name []byte) bool {
			_, ok := m[string(name)]
			return ok
		},
		"__cron_clear": func(m map[string]*time.Ticker) {
			for k, t := range m {
				t.Stop()
				delete(m, k)
			}
		},
	} {
		switch fn := fn.(type) {
		case func(ctx context.Context, m map[string]*time.Ticker, name []byte, d time.Duration):
			register(name, func(ctx context.Context, mod api.Module, stack []uint64) {
				meta := wazero_util.Get[*meta](ctx, ctxKeyMeta)
				fn(ctx, wazero_util.Get[map[string]*time.Ticker](ctx, ctxKeyLocal), getName(mod, meta), time.Duration(getVal(mod, meta))*time.Millisecond)
			})
		case func(m map[string]*time.Ticker, name []byte):
			register(name, func(ctx context.Context, mod api.Module, stack []uint64) {
				meta := wazero_util.Get[*meta](ctx, ctxKeyMeta)
				fn(wazero_util.Get[map[string]*time.Ticker](ctx, ctxKeyLocal), getName(mod, meta))
			})
		case func(m map[string]*time.Ticker):
			register(name, func(ctx context.Context, mod api.Module, stack []uint64) {
				fn(wazero_util.Get[map[string]*time.Ticker](ctx, ctxKeyLocal))
			})
		case func(m map[string]*time.Ticker, name []byte) bool:
			register(name, func(ctx context.Context, mod api.Module, stack []uint64) {
				meta := wazero_util.Get[*meta](ctx, ctxKeyMeta)
				ret := fn(wazero_util.Get[map[string]*time.Ticker](ctx, ctxKeyLocal), getName(mod, meta))
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

func getName(m api.Module, meta *meta) []byte {
	return wazero_util.Read(m, meta.ptrBuf, meta.ptrBufLen, meta.ptrBufCap)
}

func getNameCopy(m api.Module, meta *meta) []byte {
	return append([]byte(nil), getName(m, meta)...)
}

func nameBuf(m api.Module, meta *meta) []byte {
	return wazero_util.Read(m, meta.ptrBuf, 0, meta.ptrBufCap)
}

func setVal(m api.Module, meta *meta, val uint64) {
	wazero_util.WriteUint64(m, meta.ptrVal, val)
}

func getVal(m api.Module, meta *meta) (val uint64) {
	return wazero_util.ReadUint64(m, meta.ptrVal)
}

func setName(m api.Module, meta *meta, b []byte) {
	buf := nameBuf(m, meta)
	copy(buf[:cap(buf)], b)
	wazero_util.WriteUint32(m, meta.ptrBufLen, uint32(len(b)))
}
