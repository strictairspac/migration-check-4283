# Schema v2 — 目标行容量台账

新增 `capacity` / `reservations` / `capacity_events` 三表，`runs` 增加 `heartbeat_at`。

- `capacity` 单行台账（id=1）：`row_limit` 目标行上限（管理员可调，不得低于当前实占），`used` 实占，`reserved` 预留。所有扣减用带条件的原子 UPDATE（`used+reserved+n<=row_limit`），并发受理不会超额。
- `reservations` 每次受理一条，`run_id` 唯一（重试不重复占额）：`rows` 预留行数（受理时按映射预校验源行得出，坏行与同键重复行不计入），`status` active/committed/released，`reason` 归还原因。
- `capacity_events` 容量流水：reserve/commit/release/reject/restore/limit，页面据此核对预留、实占与拒绝原因。
- `runs.heartbeat_at` 执行心跳：worker 受理与每块进度时刷新；running 且心跳超过 `EXECUTOR_STALE_SECONDS` 判定执行器失联，回收预留并置 failed。落库提交在同事务内做状态守卫（run 仍为 running 且预留仍 active），预留转实占；被回收后迟到的执行者提交守卫失败，事务回滚，不能再落库。失败/取消/失联均归还预留。备份恢复按目标行净增 `delta=max(0,新-旧)` 占额，等量替换净增 0，满库也能恢复。
