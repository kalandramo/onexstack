# astore

把只读多、写少的数据（配置、角色名单、资源表）在进程内保存为一份不可变快照，读路径只读内存，刷新在后台完成。

## 两种刷新方式

| Interval | TTL | 行为 |
|---|---|---|
| > 0 | 0 | 定时全量刷新（预热），不依赖流量 |
| 0 | > 0 | 惰性 SWR：读到过期快照时先返回旧值，后台刷新 |
| > 0 | > 0 | 推荐：定时预热 + 读时兜底 |
| 0 | 0 | 关闭缓存，每次读都回源 |

失败保留上一代快照：一次数据库抖动不会让请求失败，错误通过 `Status()`、日志和 `OnRefresh` 暴露。

## 用法

```go
s := astore.New(astore.Options{
    Name:     "settings",
    Interval: time.Minute,
}, load)

// 在装配层调用一次，绑定应用生命周期。
if err := s.Start(ctx); err != nil {
    slog.WarnContext(ctx, "first load failed; retrying in the background", "error", err)
}

gen, ok := s.Load(ctx) // 读：无锁原子读
```

`Named[T]` 用于带 key 的列表，额外提供按名查找与 miss 回源：

```go
n := astore.NewNamed(astore.NamedConfig[*v1.Agent]{
    Options: astore.Options{Name: "agents", Interval: 5 * time.Minute, TTL: 5 * time.Minute},
    Load:    agents.List,
    Key:     func(a *v1.Agent) string { return a.Name },
    Lookup:  agents.Get,
})
```

## 不变量

- 整代快照原子替换（`atomic.Pointer`），读者不会看到半份数据。
- 刷新合并（singleflight），并发读不会放大回源。
- 回源结果不推进 `LoadedAt`：补一个 key 不代表整表新鲜。
- 不做负缓存；`ErrNotFound` 与 `(nil, nil)` 都表示「确实不存在」。
- 首次加载失败也启动循环，失败后按 `FailureRetry` 更快重试。

## 测试

```bash
go test -v ./pkg/astore/
```
