# 长时采集与鲁棒性验证：交付报告

验证日期：2026-09-05。已实现软件工作流并完成短时公网与故障验证；**尚未完成小时级/24 小时 soak test，也没有验证预测稳定性或盈利能力。**

## 交付与文件

新增核心模块：

- `app/capture.py`、`app/diagnostics.py`：共享运行时、独立符号管线、生命周期、健康与调度诊断。
- `market/bootstrap.py`：REST bootstrap 原始记录。
- `recorder/durability.py`、`recorder/frozen.py`、`recorder/compact.py`、`recorder/verify.py`：原子文件、恢复、冻结、维护 CLI。
- `replay/streaming.py`、`replay/reconstruction.py`、`replay/offset.py`：有界流式回放、REST+diff 重建、历史时钟状态。
- `research/batch.py`、`research/robustness.py`、`research/latency.py`：批量分段诊断、依赖性/成本与延迟证据。
- `tests/test_capture_runtime.py`、`tests/test_capture_diagnostics.py`、`tests/test_durability_recovery.py`、`tests/test_robustness.py`。
- 本报告。

主要更新：`app/main.py`、`config/settings.py`、`exchange/binance/{market_ws,rest,orderbook_sync}.py`、`market/{events,clock,orderbook}.py`、`features/{models,history,trade_flow}.py`、`recorder/{catalog,compaction,feature_recorder,raw_models,raw_recorder,raw_schemas,schemas,versions}.py`、`replay/{engine,features,loader,models}.py`、`research/{labels,splits}.py`、相关现有测试/fixtures、`README.md`、`.env.example`。

未增加策略家族、阈值优化、模型、认证接口、杠杆或实盘执行。四个原有基线参数未改变。

## 多符号隔离

一个 `CaptureRuntime` 共享 session UUID、capture sequencer、REST client、服务器时钟估计器和两个异步 recorder；BTCUSDT/BTCUSDC 分别拥有 socket、同步器、订单簿、连接/同步代数、成交窗口、价格历史、特征引擎、计数与健康状态。

BTCUSDT 仍是发现/研究主 feed；BTCUSDC 只是潜在执行研究候选。共享时钟不共享行情/特征状态。单符号重连只重置自身；写盘/磁盘等共享完整性故障会停止整个数据 session，并标记 invalid。

## 持久性与恢复

| 项目 | 实现与边界 |
|---|---|
| 轮转 | raw 默认 20,000 行或 30s；features 6,000 行或 60s，任一先到即轮转；随后按符号/类型/时间分区 |
| 原子提交 | `.tmp` → 带嵌入式 manifest 的 Parquet footer → read-back → fsync → 原子 rename → 目录 fsync → catalog 原子更新 |
| 恢复 | 独占 dataset lock；检查缺失/重复/大小/行身份/序列范围/schema/hash；有完整嵌入式元数据的孤立成品可登记 |
| 不明确损坏 | 临时文件移入 quarantine；未知 orphan、重复 ID、缺失成品、损坏 schema/hash 报 invalid，不猜测修复 |
| 背压 | bounded queue：70% WARNING、90% CRITICAL；1s supervisor 安全停止；突发满队列立即报错，不静默丢 raw |
| 磁盘 | 默认保留 2 GB；每次新文件前及周期检查；低于阈值停止开新文件并 invalid；RAM buffer 可能无法再落盘，不声称完整 |
| 退出 | SIGINT/SIGTERM 停接新工作/特征，取消 REST/clock，drain recorder、finalize、写 summary、关闭 streams |
| 崩溃 | OS 自动释放锁；旧 RUNNING session 启动恢复为 INTERRUPTED/invalid；已提交前缀可用，RAM 未提交尾部不可恢复 |
| 压缩 | 只写新目录/version，保留源文件；输入按文件数、64 MB 压缩字节、200,000 行上限切批 |

默认轮转比旧的 1s/2s 更少产生小文件。短样本的 10s/5s 轮转仍有明显 footer 开销，因此默认值只是安全初值，不是经过长期吞吐优化的结论。

接收空闲默认 5s 重连；解析失败终止 capture 完整性，而非跳过损坏消息。WebSocket、同步 buffer、去重缓存、clock/latency 样本、mid/trade windows、recorder buffers 有界。100,000 levels/side 或有限 REST 覆盖无法保证 top-10 时要求 resnapshot，而不是偷偷裁剪深档。

## Schema 与代数

- Feature v2：`aggregate_trade_event_count_*` → `trade_event_count_*`；v1 明确映射字段名，保留原数值，不把聚合消息数解释为成交笔数。
- `trade_source=AGGREGATE/INDIVIDUAL` 的 raw 来源标签保留；生产 `@trade` 为 INDIVIDUAL。早期 feature v1 可能已经使用 individual feed，不能仅凭 v1 推断 source；无 raw 来源证明的历史数据不能静默重新标记。
- Raw trade v1 的 `aggregate_trade_id` 为兼容而保留；INDIVIDUAL 时值为 Binance `t`，等于 first/last trade ID。
- Lifecycle v2 增加 connection/sync generation；clock v2 保存完整时间与 accepted；bootstrap/runtime schemas 为 v1；旧 clock/lifecycle v1 显式读取，未知版本拒绝。
- 进程重启：新 session，sequence 从头开始；同进程重连：session 不变，connection generation 增加；每次 REST 同步尝试：新 sync generation。

REST 记录保存全部 bids/asks、lastUpdateId、请求/响应 wall+monotonic time、exchange E/T（可用时）、limit、reason、offset 和已缓冲 diff IDs。重建的价格/数量不读取 trusted books；trusted books 仅作为比较目标。

## 最终公网 smoke 与退出验证

最终验证根目录：`data/longrun-final-smoke-20260905/`。

Session A：`38e5c70a977d4ae2a4525f488c66a746`，UTC 04:25:32.900330–04:26:47.956925；受控断开 BTCUSDC。

Session B：`0250652ea2794bd6a3f8d6bd4f1590f2`，UTC 04:28:53.897060–04:29:12.648883；新进程启动、实际 SIGTERM，退出码 0。

| Session / symbol | 时长 s | Trades | Depth | Features | Files | Bytes | Resync | Gap | Invalid features |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| A BTCUSDT | 75.056595 | 194 | 731 | 742 | 48 | 834,363 | 0 | 0 | 0 |
| A BTCUSDC | 75.056595 | 56 | 594 | 742 | 51 | 733,795 | 1 | 0 | 35 |
| B BTCUSDT | 18.751823 | 243 | 180 | 181 | 25 | 384,322 | 0 | 0 | 0 |
| B BTCUSDC | 18.751823 | 95 | 145 | 181 | 25 | 340,124 | 0 | 0 | 0 |

受控断线前后，BTCUSDT 特征计数 142 → 276，connection/sync 均保持 1；BTCUSDC connection/sync 升到 2 并重新 SYNCED。35 个 invalid features 如实保留；标签不能跨失效 tick 或生命周期边界。`dataset_valid` 表示已记录数据内部一致、失效区间明确，**不代表无停机或全时段行情覆盖**。

最终原始重建：

| Session / symbol | REST bootstraps | Trusted snapshots compared | Mismatch | Gap |
|---|---:|---:|---:|---:|
| A BTCUSDT | 1 | 728 | 0 | 0 |
| A BTCUSDC | 2 | 592 | 0 | 0 |
| B BTCUSDT | 1 | 178 | 0 | 0 |
| B BTCUSDC | 1 | 144 | 0 | 0 |
| 总计 | 5 | 1,642 | 0 | 0 |

first mismatch 全部为 null。1,846 个 FeatureSnapshots 的所有字段比较通过；Decimal 精确相等，derived float 容差 1e-12。

早一轮故障 smoke 在 `data/longrun-smoke-20260905/` 暴露了重连后成交流年龄的时间戳来源差异。已修复并添加回归测试；该早期根目录不属于本次最终冻结数据，未删除或混入成功结果。

## Clock / latency

| 指标 ms | Session A | Session B |
|---|---:|---:|
| 最后窗口 median offset | +447.144 | +451.775 |
| offset MAD | 0.961 | 0.398 |
| median RTT | 80.233 | 80.441 |
| min RTT | 78.137 | 79.612 |
| 最后窗口首末 offset 变化 | -52.722 | -0.492 |
| 整个 session 首末 sample offset 变化 | -141.090 | -0.492 |
| Event-loop lag p50 / p95 / max | 0.750 / 2.292 / 3.569 | 0.764 / 2.208 / 5.563 |

Clock 成功/失败 A=20/0，B=5/0。首末 offset 差包括启动估计收敛与 RTT 不对称，不能直接当成设备真实时钟漂移。

| Session / symbol | Raw lag p50 | Corrected p50 / p95 / p99 | Corrected max |
|---|---:|---:|---:|
| A BTCUSDT | -387.296 | 61.471 / 76.881 / 131.521 | 220.562 |
| A BTCUSDC | -406.242 | 43.917 / 63.094 / 66.704 | 204.925 |
| B BTCUSDT | -387.726 | 64.049 / 122.322 / 149.053 | 150.877 |
| B BTCUSDC | -411.662 | 40.113 / 74.888 / 75.446 | 97.686 |

A/B 两个较大 BTCUSDT depth lag 约 220ms，前置接收间隔约 259/256ms，附近 heartbeat lag 约 0.64/1.39ms。B 的 151ms trade lag 出现在亚毫秒消息间隔的 burst 中，附近 heartbeat 约 0.31ms。证据不支持把这些尾部直接归因于同量级事件循环停顿，但仍无法区分网络、socket/上游缓冲、exchange E/T 语义和 midpoint offset 误差。

此前约 1.7s 的正延迟尾部在最终 smoke 未重现，原因仍未确定。受控断线造成的 BTCUSDC book age 最大 6.41s 是另一个明确的停机指标，不应混作 exchange event lag。没有 clamp 原始/校正 lag。

## Storage projection

以下按两个 session 总计 93.808418s 的观测字节率外推，单位十进制 GB；包括短文件 footer、启动 bootstrap 和故障实验开销，**不是长期存储容量承诺**。

| 类型 | BTCUSDT GB/h | BTCUSDT GB/day | BTCUSDC GB/h | BTCUSDC GB/day |
|---|---:|---:|---:|---:|
| trade | 0.004368 | 0.104834 | 0.003427 | 0.082249 |
| depth | 0.010695 | 0.256679 | 0.006363 | 0.152702 |
| trusted_book | 0.007080 | 0.169913 | 0.006759 | 0.162213 |
| bootstrap | 0.001632 | 0.039180 | 0.002529 | 0.060690 |
| feature | 0.016263 | 0.390316 | 0.014171 | 0.340098 |
| clock | 0.001698 | 0.040754 | 0.001698 | 0.040754 |
| runtime heartbeat | 0.003181 | 0.076336 | 0.003181 | 0.076339 |
| book lifecycle | 0.000628 | 0.015083 | 0.001248 | 0.029949 |
| stream lifecycle | 0.001223 | 0.029347 | 0.001838 | 0.044113 |
| 全部 | 0.046768 | 1.122441 | 0.041213 | 0.989107 |

原始合计 149 files / 2,292,604 bytes / 5,990 rows；新目录压缩后 36 files / 943,108 bytes / 5,990 rows。源文件与冻结指纹保持不变，压缩版四个 session/symbol 的 feature digests 与原版一致。没有因容量外推而删减 raw depth。

## 依赖性诊断（示例固定 1s horizon，Session A）

完整文件保留全部 250ms/500ms/1s/2s/5s/10s horizons。下表的 Neff 是非重叠 x/y 的 initial-positive ACF 估计较小者，不是独立观测的精确计数。

| Symbol / feature | Raw pairs | Nonoverlap pairs | Signal ACF 100ms | ACF 1s | ACF 5s | Approx Neff |
|---|---:|---:|---:|---:|---:|---:|
| USDT L1 | 731 | 69 | 0.951 | 0.723 | 0.498 | 1.00 |
| USDT L5 | 731 | 69 | 0.952 | 0.730 | 0.517 | 1.00 |
| USDT L10 | 731 | 69 | 0.951 | 0.717 | 0.490 | 1.00 |
| USDT microprice offset | 731 | 69 | 0.951 | 0.723 | 0.498 | 1.00 |
| USDT trade imbalance 1s | 567 | 53 | 0.845 | 0.012 | -0.012 | 1.00 |
| USDC L1 | 686 | 66 | 0.996 | 0.978 | 0.885 | 2.38 |
| USDC L5 | 686 | 66 | 0.993 | 0.977 | 0.881 | 2.48 |
| USDC L10 | 686 | 66 | 0.969 | 0.938 | 0.833 | 2.45 |
| USDC microprice offset | 686 | 66 | 0.996 | 0.978 | 0.885 | 2.38 |
| USDC trade imbalance 1s | 195 | 19 | 0.947 | -0.402 | N/A | 1.00 |

USDT 在 A 中 mid 不变，return ACF 与 predictive correlation 为 null，不强填 0；Neff=1 是常量序列保护值，不能解释为有一个有效统计证据。USDC 1s returns 的 100ms ACF=0.822、1s ACF=-0.014。L5 predictive Pearson 全量/非重叠为 0.161/0.148，但样本极小且高度相关。

Session B 的 L5：USDT raw/nonoverlap/Neff=170/16/2.51，Pearson=-0.120/+0.154；USDC=171/17/3.26，Pearson=+0.120/+0.107。符号/采样变化说明不能把高频行数当独立样本。

按 session 的 USDC L5→1s Pearson 为 0.161、0.120，中位数 0.140、min/max 0.120/0.161、同号比例 2/2。USDT 只有一个非退化 session，另一段为 null。这里的同号比例**没有稳定性证据意义**。

离线 block bootstrap 使用 10s 时间块、200 次、seed=42。A 只有 8 个时间块；USDC L5 high 条件均值估计 0.003274 bps，近似区间 [0, 0.389664]，192 次可用抽样；USDT 对应条件无观测，区间 null。小块数与稀疏条件不支持正式推断。波动桶为明确标注的描述性小时内 quantiles；正式评估必须仅从 TRAIN 计算 cutpoints。

## Edge vs cost：不是成交收益

固定 trade-flow baseline 的 signed future log-price movement（bps），所有 horizons：

| Horizon | A USDT | A USDC | B USDT | B USDC |
|---|---:|---:|---:|---:|
| 250ms | 0 | 0.004525 | 0.011898 | 0.060693 |
| 500ms | 0 | 0.004525 | 0.012160 | 0.110212 |
| 1s | 0 | 0.006895 | 0.013018 | 0.131796 |
| 2s | 0 | 0.027510 | 0.014754 | 0.127393 |
| 5s | 0 | 0.031673 | 0.163466 | 0.366268 |
| 10s | 0 | 0.043766 | 0.060356 | -0.045529 |

Book-imbalance baseline 及全部 feature/condition 的结果同样保留在机器可读报告。Microprice momentum 和 combined baseline 在这些样本无触发，其 edge 为 null，而不是 0；没有降低阈值来制造样本。

固定 1s 的净 movement 敏感性（gross − 假设 round-trip cost，bps）：

| Cost | A USDT | A USDC | B USDT | B USDC |
|---|---:|---:|---:|---:|
| 0 | 0.0000 | 0.0069 | 0.0130 | 0.1318 |
| 0.5 | -0.5000 | -0.4931 | -0.4870 | -0.3682 |
| 1 | -1.0000 | -0.9931 | -0.9870 | -0.8682 |
| 2 | -2.0000 | -1.9931 | -1.9870 | -1.8682 |
| 3 | -3.0000 | -2.9931 | -2.9870 | -2.8682 |
| 5 | -5.0000 | -4.9931 | -4.9870 | -4.8682 |
| 8 | -8.0000 | -7.9931 | -7.9870 | -7.8682 |
| 10 | -10.0000 | -9.9931 | -9.9870 | -9.8682 |
| 12 | -12.0000 | -11.9931 | -11.9870 | -11.8682 |

例如 B USDC 1s gross=0.131796 bps；TAKER/TAKER 假设 11 bps：net=-10.868204、edge/cost=0.01198；MAKER/TAKER 假设 8 bps：net=-7.868204、ratio=0.01647；break-even cost=0.131796 bps。11/8 及 cost grid 都是可配置假设，不是当前费率、促销承诺或 maker 成交保证。没有据此选出“最佳 horizon”。

## BTCUSDT / BTCUSDC 操作性比较

以 A 为例（不足以判断长期适用性）：

| 指标 | BTCUSDT | BTCUSDC |
|---|---:|---:|
| Trades/s | 2.585 | 0.746 |
| Depth events/s | 9.739 | 7.914 |
| Spread median / p95 bps | 0.012574 / 0.012574 | 0.012570 / 0.012570 |
| L1 bid / ask median BTC | 4.1445 / 6.1540 | 0.1100 / 1.3560 |
| L5 bid / ask median BTC | 4.7375 / 6.4250 | 0.2400 / 1.4040 |
| L10 bid / ask median BTC | 4.8140 / 6.8055 | 0.3500 / 5.7460 |
| RV 1s median | 0 | 0 |
| Unchanged feature-tick fraction | 100% | 99.73% |
| Price-update fraction | 0% | 0.27% |
| Gap rate | 0 | 0 |
| Resync rate | 0 | 1 次人工故障 / 75.06s |

本样本 USDT 交易/深度事件更频繁，支持继续把它作为发现 feed 收集；不能外推时间段外的流动性。USDC 的人工停机也影响本表，不能把它当作天然可靠性差异。执行研究还缺长期流动性、真实可配置成本与成交质量证据；没有新增跨符号交易逻辑。

## 冻结与重复性

最终冻结 ID / manifest fingerprint：

`61feeee558ff34c0761989b3ab867d11c3de4d4cd81896174a9a4aa61f1dbd75`

Batch fingerprint：`bab70cd4850d961af82f9a232790ae64d9d500f6dbedbe2ea8771cc9ca850d44`

Research source fingerprint：`f3d096752bfaacd8821ecf752ebcd1f79d8b65a49fd7a90e7464364654d50374`

同一冻结数据/配置/seed 重跑两次，13 个 JSON/Parquet 研究文件逐字节一致。Batch report SHA-256：`77d89efb0f74a22b97d792b3cf79f39e11da74ab56bf4abbd06c885fe3f264cd`。

压缩新版本 fingerprint：`20810fb13f3b6492f0cde14ccea3bc113ba49f6963c51d9b40ca8c4e4637b5cd`。重新验证源冻结数据未变；原版/压缩版四组 feature-output SHA-256 一致。

这只是**操作验证用冻结数据**，不是足以进行统计确认的正式长期研究样本。

## Tests / quality gates

| Gate | 结果 |
|---|---|
| `pytest -q` | 176 passed（基线 113，新增 63 cases） |
| `ruff check .` | All checks passed |
| `ruff format --check .` | 95 files already formatted |
| strict `mypy app config exchange execution features market recorder replay research signals strategies` | Success: no issues found in 67 source files |
| `git diff --check` | pass |

覆盖：多符号隔离、bootstrap 完整字段、原始重建与精确 Decimal 差异检测、版本辨别、session/connection/sync 语义、轮转/原子提交/恢复/隔离/manifest reconciliation、背压/disk、有限 buffers/coverage、时钟 drift/MAD、loop lag、pending REST shutdown、高队列 drain、catalog selection、小时/重启/短 resync 标签边界、nonoverlap/ACF/ESS/bootstrap/cost grid、freeze 指纹/防删/新版本 compaction、完整特征重放和源连接时间戳回归。

工作区原有源码均未被 Git 跟踪，因此普通 `git diff --check` 本身覆盖面有限；未替用户创建 commit 或 stage 文件。Ruff 对整个源码树执行，测试使用合成传输/时钟与短 fixtures，不依赖真实市场或长时间等待。

## 剩余风险

- 没有完成数小时/多 session/24h soak；内存、CPU、文件数量、压缩率和极端成交 burst 尚需实测。不得把本次 smoke 当长期通过。
- Catalog JSON 每次重写，启动 recovery 全量验证；metadata 内存/恢复耗时随档案文件数增加。事件级缓存有硬界，metadata 不是固定空间。
- SIGKILL、真实断电或磁盘完全耗尽会丢失 RAM 尾部。恢复会标 invalid；这不是零数据丢失的 WAL recorder，也没有自动安装 OS 进程守护。
- 时钟 midpoint 不能证明单向延迟。长时间 server-time 请求失败时，应结合 `last_updated`/failure counters 排除陈旧 clock 估计段；目前不以 offset 年龄自动切断 raw capture。
- 校验成功不等于无停机覆盖；此次人工断线的 invalid features/标签边界必须保留。无法收到的停机期间交易没有被伪造或回填。
- 有限 REST top-1000 不是全市场全深度；top-10 覆盖不足会 resync。重放/压缩可能需要大量 CPU/解压内存，硬界超出时应报错而非隐藏。
- Regime cutpoints 目前为描述性；ESS 与 block-bootstrap CI 是近似，稀疏标签/常量 returns/很少 blocks 不支持正式推断。
- Frozen 是应用层内容寻址和防修改检查，不是 OS WORM；外部手工修改会被发现而不是被系统权限阻止。当前仓库尚无受跟踪 Git revision，研究源码指纹用于补充可追溯性。

## 唯一下一个里程碑

**收集并分析一个 materially larger 的冻结数据集：覆盖多个 UTC 时段、数小时检查点，最终至少一个完整 24 小时周期；先验证覆盖、时钟/停机、依赖性和跨 session 稳定性，不新增或优化策略。**

操作命令及参数见根目录 `README.md`。
