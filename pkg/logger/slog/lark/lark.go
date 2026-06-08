package logger

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"time"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

// SlogWrapper 实现了 Logger 接口
type SlogWrapper struct {
	logger *slog.Logger
}

var _ larkcore.Logger = (*SlogWrapper)(nil)

// NewSlogWrapper 创建一个包装器实例
// 如果传入 nil，则默认使用系统的 slog.Default()
func NewSlogWrapper(l *slog.Logger) *SlogWrapper {
	if l == nil {
		l = slog.Default()
	}
	return &SlogWrapper{logger: l}
}

// Debug 实现 Debug 接口
func (w *SlogWrapper) Debug(ctx context.Context, args ...interface{}) {
	w.log(ctx, slog.LevelDebug, args...)
}

// Info 实现 Info 接口
func (w *SlogWrapper) Info(ctx context.Context, args ...interface{}) {
	w.log(ctx, slog.LevelInfo, args...)
}

// Warn 实现 Warn 接口
func (w *SlogWrapper) Warn(ctx context.Context, args ...interface{}) {
	w.log(ctx, slog.LevelWarn, args...)
}

// Error 实现 Error 接口
func (w *SlogWrapper) Error(ctx context.Context, args ...interface{}) {
	w.log(ctx, slog.LevelError, args...)
}

// 核心日志处理逻辑：解决调用栈深度与参数解析
func (w *SlogWrapper) log(ctx context.Context, level slog.Level, args ...interface{}) {
	// 1. 检查当前日志级别是否启用，减少不必要的性能开销
	if !w.logger.Enabled(ctx, level) {
		return
	}

	var msg string
	var attrs []slog.Attr

	// 2. 解析参数
	if len(args) > 0 {
		msg = fmt.Sprint(args[0])
		attrs = argsToAttrs(args[1:])
	}

	// 3. 获取正确的调用者 PC
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:])

	// 4. 构建 Record 并写入
	r := slog.NewRecord(time.Now(), level, msg, pcs[0])
	// r.AddContext(ctx) <-- 删掉这一行，slog.Record 没有这个方法
	for _, attr := range attrs {
		r.AddAttrs(attr)
	}

	// context 会在这里作为第一个参数传给 Handler，Handler 内部会自动处理 context
	_ = w.logger.Handler().Handle(ctx, r)
}

// 辅助函数：将随后的参数转换为 slog 的 Attr 属性
func argsToAttrs(args []interface{}) []slog.Attr {
	var attrs []slog.Attr
	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			// 正常成对的 Key-Value
			key, ok := args[i].(string)
			if !ok {
				key = fmt.Sprint(args[i])
			}
			attrs = append(attrs, slog.Any(key, args[i+1]))
		} else {
			// 奇数个参数时，最后一个参数由于没有 Value，标记为 !BADKEY
			attrs = append(attrs, slog.Any("!BADKEY", args[i]))
		}
	}
	return attrs
}
