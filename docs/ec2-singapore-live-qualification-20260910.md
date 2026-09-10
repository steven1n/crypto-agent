# Singapore EC2 首次 Linux 现场资格验证（2026-09-10）

## 结论与范围

**LINUX_LIVE_QUALIFICATION_PASS — READY_FOR_RESOURCE_SOAK**

仅一个 BTCUSDT + BTCUSDC 公共行情会话。通过短时采集、原始重建、零容差特征比较；这不是长期资源 PASS、Stage B 研究或执行延迟认证。没有自动启动后续 campaign，没有启用 systemd，没有扩盘、交易、优化或认证 Binance API。

## 身份、环境与指纹

- EC2：i-0b357fe2738d4a496；IMDSv2 核实 t3.xlarge、ap-southeast-1 / ap-southeast-1b、AMI ami-0532913178263be11。
- 公网 IP（仅操作元数据）：54.179.165.51；hostname：ip-172-31-30-43。
- OS：Ubuntu 26.04 LTS；kernel：7.0.0-1006-aws；x86_64，4 logical CPUs。
- /proc/meminfo MemTotal：16161560 kB；实际约 16 GB 级内存，不是 8 GB。
- Python：3.14.4 (main, Aug 20 2026, 10:41:58) [GCC 15.2.0]；PyArrow：23.0.1；glibc 2.43；Arrow allocator：mimalloc。
- timezone：Etc/UTC；采集前 NTPSynchronized=yes。
- source fingerprint：`2822085ea27f48d346d3f61f4dafd6a0a1c70c5620400fa281a943de3efadf93`
- configuration fingerprint：`cdeacd3e86e8dda1cec03f99c2030f32b92ec89dff2ffc128410791c56629e71`
- controls fingerprint：`cc225b299a296c97d626bda39f7ee9045638ee298084f44727e3abbb34bfbb2d`
- Linux wheel-hash manifest SHA256：`32fe5492f036b16dd8f7e9cb6584f9a9b8e0bff07d17a2de31669ed5731cf103`
- capture observer script SHA256：`1dc82414ed2a5873e9517d07839b9c6422a06a461299536dba2dca16a78dbd06`
- Feature schema：2；raw schema：`{"book_lifecycle":2,"clock_sample":2,"depth":1,"orderbook_bootstrap_snapshot":1,"runtime_sample":1,"stream_lifecycle":2,"trade":1,"trusted_book":1}`。
- 完整 Settings、transport constants、各源文件 SHA256 与 host provenance 均在远端 summary.json / frozen-configuration.json / host-provenance.json 中。源文件校验在采集前、采集后和验证后保持一致。源指纹与已接受 Mac 核心代码一致。
- 精确 SSH 私钥未打印、未上传。仅用已有 SSH 登录执行；IMDSv2 只读取实例身份，不读取 IAM 凭据。

## 配置与实际时间

复用已接受设置，只替换 data_directory 和 capture_duration_seconds=900。保留 100ms 特征、@trade、@depth@100ms、REST limit 1000、raw queue 100000 / feature queue 10000、raw rotation 20000 rows / 30s、feature rotation 6000 rows / 60s、clock 300s / 5 samples / max RTT 1000ms、disk reserve 2GB。未修改核心源码、策略、特征、阈值或历史证据。

- Session：`5062847fe97a4108a3e0230e9759d94a`
- preflight UTC：2026-09-10T02:28:31.882152+00:00
- 行情会话 UTC：2026-09-10T02:28:32.873749+00:00 → 2026-09-10T02:43:32.888343+00:00
- 新加坡本地时间：2026-09-10 10:28:32.873749 → 10:43:32.888343（UTC+8）。
- 行情时长：900.015 秒；包含启动/flush/关闭的进程墙钟单调计时：916.846 秒。
- 采集进程退出 UTC：2026-09-10T02:43:48.807716+00:00；exit code 0。
- 操作恢复后仅验收已结束的这一个会话，没有延长或重采。原始验证与特征验证退出码 0。

## 采集正确性与覆盖

Session FINALIZED，dataset_valid=true，disk_degraded=false，invalid_reasons=[]。启动恢复无 interrupted / invalid / quarantine / reconciliation；结束后只读 recovery valid。录制行数与接收/发出计数对账通过，无 writer error。重同步与 invalid tick 保留，不将有效数据集等同于完全无失效区间的覆盖。

| Symbol | Trades | Depth | Features | Trusted books | Resyncs | Reconnects | Invalid feature ticks | Gaps |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| BTCUSDT | 21990 | 8820 | 8956 | 8815 | 2 | 0 | 5 (0.056%) | 0 |
| BTCUSDC | 6156 | 7714 | 8953 | 7711 | 2 | 0 | 2 (0.022%) | 0 |

每个 symbol 只有一次 WebSocket connection，没有重连。BTCUSDC 初始 snapshot bridge 重试后恢复；后续存在 top-depth 超出已知 REST 覆盖而重新取快照的正常 fail-closed 路径。没有抹去这些边界。invalid tick 计数不是精确失效持续秒数，未据 cadence 补造无观测时长。

## 原始 REST + diff 重建

先完成两币种原始重建，均通过后才进入 feature replay。使用已有 recover_dataset(repair=False)、validate_raw_reconstruction 和 iter_session；不依赖 trusted book 价格作为重建输入，不做 repair、freeze、研究 batch。

| Symbol | 全部源事件（含 feature/runtime） | REST bootstraps | Trusted compared | Mismatches | Sequence gaps | First mismatch |
|---|---:|---:|---:|---:|---:|---|
| BTCUSDT | 49503 | 3 | 8815 | 0 | 0 | null |
| BTCUSDC | 31454 | 3 | 7711 | 0 | 0 | null |

盘口 Decimal 及既有比较字段均精确一致。活动 bid/ask 层数最大值未被当前外部遥测暴露，标为 unavailable；未加入热路径对象遍历。

## Feature replay：严格零容差

ReplayEngine retain_results=False，已有全字段比较器 absolute_tolerance=0，并分别累计 Decimal/float 字段差异。生命周期/失效语义照原实现，无重新排序或改写时间戳，无标签生成。

| Symbol | Source rows | Compared | Decimal differences | Float differences | Maximum float error | Mismatch rows | First mismatch |
|---|---:|---:|---:|---:|---:|---:|---|
| BTCUSDT | 8956 | 8956 | 0 | 0 | 0 | 0 | null |
| BTCUSDC | 8953 | 8953 | 0 | 0 | 0 | 0 | null |

两边 replay data_quality.valid=true，errors=[]；源行严格 1:1。没有容差放宽，没有运行历史数据或 Stage B。

## Linux 资源观察（描述性）

16 次 /proc 采样（含刚启动的一次），约每 60 秒；采样错误 0。RSS/VMHWM 按 Linux kB×1024 转 bytes，以下 MB 用 10^6 bytes。CPU 从 /proc/<pid>/stat 得到进程累计用户+系统 CPU / 存活时间，100% 表示一个核，不是采样区间瞬时 CPU。

| 指标 | 观察 |
|---|---|
| 首个 post-startup RSS（60.01s） | 143.790 MB |
| 最后 live RSS（900.12s） | 312.938 MB |
| sampled maximum RSS | 312.938 MB |
| 最大观察到的 VmHWM | 312.938 MB |
| FD min / max / final | 3 / 12 / 10 |
| Threads min / max / final | 1 / 5 / 5 |
| Sockets min / max / final | 0 / 7 / 5 |
| CPU lifetime % p50 / p95 / max | 20.074 / 25.657 / 26.018 |

初始 min 值包含 Python 启动阶段；首分钟也不代表已经证明稳定。最后 live 样本不覆盖后续约 16 秒 flush 的所有瞬时峰值，进程退出后不能读取最终 /proc VmHWM。无 FD/thread 长期泄漏判定；15 分钟明显不足以证明内存有界，不从增长或一个平坦片段推断长期通过/失败。

队列为单个 runtime 共用的 raw/feature recorder；每 symbol 的 RuntimeSample 按顺序观察同一队列，不是两套 writer。每边 894 个约 1 秒 runtime 样本：
- raw queue：BTCUSDT 观察 max 41，BTCUSDC max 42；共用容量 100000，最大观察占用 0.042%。
- feature queue：max 2 / capacity 10000，即 0.02%。
- 分钟 health 14 次：raw NORMAL=14、features NORMAL=14；WARNING=0、CRITICAL=0。只是观察计数，不冒充连续精确停留时长。
- writer last_error 在所有 health 样本中均 null；最终有效 session 和行数对账提供收尾证据。

## Clock / latency（ms）

以下按已录下的全部 trade/depth/runtime 事件，在原始验证遍历期间做离线描述性汇总；raw lag = local_receive - exchange_event_time；corrected lag 使用当时已接受 clock 样本的原有滚动中位 offset。没有 clamping。p 分位采用排序后 floor((N−1)p) 的经验顺序统计量。

| Symbol | Event / lag | N | p50 | p95 | p99 | Max |
|---|---|---:|---:|---:|---:|---:|
| BTCUSDT | trade / raw | 21990 | 41.232 | 101.019 | 184.376 | 227.060 |
| BTCUSDT | trade / corrected | 21990 | 42.252 | 102.226 | 185.584 | 228.268 |
| BTCUSDT | depth / raw | 8820 | 34.529 | 38.161 | 54.736 | 185.532 |
| BTCUSDT | depth / corrected | 8820 | 35.636 | 39.315 | 55.444 | 186.740 |
| BTCUSDC | trade / raw | 6156 | 43.530 | 121.902 | 171.877 | 244.049 |
| BTCUSDC | trade / corrected | 6156 | 44.482 | 123.110 | 173.085 | 245.257 |
| BTCUSDC | depth / raw | 7714 | 35.974 | 50.038 | 85.202 | 256.484 |
| BTCUSDC | depth / corrected | 7714 | 37.087 | 51.246 | 86.380 | 257.692 |

事件循环是同一个 runtime，两个 symbol 录下的是同一份 894 个观察，不能合并成 1788 个独立样本：p50 1.098、p95 24.578、p99 54.658、max 186.203 ms。最终会话 summary 的 median 实现得到 p50 1.102ms，和上述低秩 p50 1.098ms 的微小差异仅为分位定义，不是数据不一致。

Clock 共 15 个接受样本（按 symbol 复制记录，不重复计独立样本），failure=0。全样本 offset p50 1.178ms，范围 -12.541 到 17.872ms；RTT min 73.958、p50 77.313、p95 107.490ms。结束时滚动 offset 1.208ms，MAD 0.506ms，RTT 76.365ms。offset variation 不直接解释为真实设备时钟漂移。

不做 Mac 对比，不把接收延迟当执行延迟，不在这些观察上宣称尾延迟因果。

## 磁盘与文件

共 290 个最终 Parquet 文件、11795241 bytes（11.795 MB）。BTCUSDT 146 files / 7455958 bytes；BTCUSDC 144 files / 4339283 bytes。

按本次 900.015 秒 Parquet 速率简单外推：约 0.047 GB/hour、1.132 GB/day；不包括 journal/日志、包缓存、离线临时内存或文件，不保证未来市场活动下的速率。采集前根盘容量 19594608640 bytes、free 16669806592 bytes；最后 live free 16657203200 bytes；验证后观测 free 16231383040 bytes。没有 disk safety event，没有扩盘。

## 产物与限制

远端机器报告：
`/var/lib/crypto-capture/work/ec2-singapore-live-qualification-20260910/summary.json`

同目录资源序列 resources.jsonl、capture.log、host-provenance.json、frozen-configuration.json、dataset/ 保留。summary 含完整 per-symbol 原始行数、raw/feature 验证、latency、环境、schema、源码 hash 和资源序列引用。运行工具位于 /opt/crypto-deployment，不修改 /opt/crypto-agent 核心。

新建外部 observer 在启动前曾因 file_sha256 导入路径错误退出；当时未创建会话、未启动 capture。修正仅涉及该新工具后才开始本次唯一会话，最终 runner hash 已在 preflight 冻结。没有 restart-and-merge。

结论只允许把下一步列为**另行授权的 Linux resource soak**。不保证长期 RSS 有界，不修改 Mac RESOURCE_GATE_WARNING，不进行研究或收益解释。报告完成即停止。

LINUX_LIVE_QUALIFICATION_PASS — READY_FOR_RESOURCE_SOAK
