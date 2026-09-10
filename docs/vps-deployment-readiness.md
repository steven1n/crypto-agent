# VPS deployment readiness audit

审计日期：2026-09-08。范围仅为 BTCUSDT + BTCUSDC 公开行情采集的 Linux 部署准备。没有 VPS 访问授权或连接操作；没有安装依赖、部署、启用服务或启动任何采集。下列命令是未来经授权后的操作程序，不是本次执行记录。

## 1. 结论与阻碍分类

**核心程序可进入 Linux 安装与资格测试阶段，无需核心源码修改；尚未取得 clean-Linux 实测资格。** READY 指部署准备流程已定义，不表示已通过 VPS smoke/resource gate，更不允许自动开始24h。

| 项目 | 审计结果 | 处理 |
|---|---|---|
| app.capture / recorder / FeatureEngine | 标准Python、asyncio、POSIX flock/fsync/rename；未发现Mac必需运行路径 | 不改核心 |
| 原有 app.campaign_observer | caffeinate仅在darwin分支；ps RSS/CPU格式适用于带procps的Linux，但CPU统计口径需标注 | 可复用基础RSS安全监测；先做ps解析检查 |
| data/resource-soak-tools-20260908/run_soak.py | 无条件libproc.dylib、sysctl、caffeinate；明确的**Mac campaign脚本移植阻碍** | 不在Linux执行，不修改历史脚本；不是核心capture阻碍 |
| capture_probe.py | ru_maxrss直接命名bytes只适合Mac；Linux getrusage单位不同 | 不直接使用此脚本的peak字段；Linux用/proc VmHWM kB×1024 |
| 依赖复现 | pyproject为范围，未找到lock/requirements锁；build hatchling亦未锁 | 长采集前须完成Linux平台的版本/轮子hash锁定；见第3节 |
| 完整资源遥测 | 基础observer没有Linux FD/thread计数；没有便携现成的完整6h资源观测命令 | 资格计划增加外部/proc采样，不改capture；未实现/验证前不签发完整资源PASS |
| 特征单独验证 | 无独立feature-only CLI；research.batch会做额外统计 | 第8节用既有ReplayEngine API的短数据适配段，禁止拿完整batch代替 |
| 主机身份 | 当前session-summary无region/environment字段 | 用外部签名式hash sidecar绑定session和冻结manifest，不改schema |

没有发现必须马上修复的核心运行阻碍。部署前的环境锁定、网络准入、权限、Linux wheel和资格测试是明确的验收前置条件；不是已通过事项。

## 2. 路径、操作系统和隐式环境

检查了源模块、config、tests、docs、README、.env.example和忽略目录中的Python/shell/Markdown/TOML文本。历史JSON/Parquet数据未批量扫描或修改；其中已有工作站配置绝不能当Linux配置复用。

| 位置 / 假设 | 分类 | 结论 |
|---|---|---|
| 所检索项目源码/配置/脚本/既有Markdown中的 `/Users/`、`/Applications/`、`/opt/homebrew/`、`/usr/local/` | 未发现匹配的硬编码工作站路径 | 无需替换；本报告提到的路径不属于原代码发现 |
| .venv/pyvenv.cfg 的Homebrew解释器和工作站venv创建路径 | 本地环境、不可迁移 | 不复制.venv，在Linux重建 |
| docs/data-campaign.md 的caffeinate描述 | 无害历史文档 | 保留 |
| app/campaign_observer.py SOURCE_ROOT | 从__file__推导，非Mac绝对路径 | 必须部署整个源树和pyproject，不能只拷贝一个脚本或只装wheel运行observer |
| Mac campaign runner 中libproc/sysctl/caffeinate、固定历史campaign相对路径 | campaign-only脚本 | 排除Linux启动路径；不篡改其历史provenance |
| frozen configuration/session JSON 的历史data_directory/解释器信息 | research artifact / provenance | 作为历史证据保留；移动后按当前root+相对file_path验证 |
| 测试临时目录 | test-only fixture | 使用tmp_path，不要求工作站目录 |

Linux选择glibc发行版和本地ext4/XFS等支持POSIX锁及原子rename的文件系统，不把NFS、对象存储挂载当首个验收目标。临时文件在目标文件同目录；目录必须允许创建、rename、fsync、lock。fcntl是Linux支持的POSIX依赖，不是Windows兼容承诺。

时间使用aware UTC、monotonic及capture_seq；无locale数值解析依赖。部署设`TZ=UTC`、`LANG=C.UTF-8`、`LC_ALL=C.UTF-8`，避免日志/ps格式歧义，不改变记录wall值或已有回放规则。

Settings大小写不敏感，进程环境优先于cwd的.env；空值可能被忽略。部署使用无.env的确定cwd，加完整审核过的EnvironmentFile，禁止意外继承笔记本.env。特别注意`SYMBOLS=BTCUSDT,BTCUSDC`才是多symbol控制，单数SYMBOL不是替代。

HTTPX/WebSocket可使用环境代理；本项目不依赖VPN守护程序。检查大小写的HTTP_PROXY、HTTPS_PROXY、ALL_PROXY、NO_PROXY以及WS相关代理，禁止继承localhost代理。无代理时显式清理；需要代理时确认依赖/协议支持，记录代理类别及凭据脱敏后的出口信息。不得打印完整环境泄漏代理密码。不要关闭TLS验证。VPS出口是否允许访问Binance公共接口必须现场确认；不可用则该主机不能通过，不以绕过限制作为部署方案。

## 3. 可复现安装

建议资格目标：Ubuntu 24.04 LTS/glibc、x86_64、CPython 3.12（记录发行版实际完整patch/build）。aarch64需单独wheel和资源资格，不能直接复用x86结论。项目声明Python>=3.12；当前Mac实际3.14.3，因此Linux3.12是新环境维度而非位级相同环境。也可选择同版本3.14.3作对照，但必须冻结可信解释器来源及构建，不能复制Mac可执行文件。

当前安装的直接依赖：

```text
httpx==0.28.1
pyarrow==23.0.1
pydantic==2.13.5
pydantic-settings==2.15.0
python-socks==2.8.2
structlog==25.5.0
websockets==15.0.1
mypy==1.20.2
pytest==8.4.2
pytest-asyncio==1.4.0
ruff==0.16.6
```

本地传递依赖快照（不是已认证Linux锁）：annotated-types0.8.0、anyio4.15.0、certifi2026.7.22、h11 0.16.0、httpcore1.0.9、idna3.19、iniconfig2.3.0、librt0.15.0、mypy_extensions1.1.0、packaging26.3、pathspec1.1.1、pluggy1.6.0、pydantic_core2.46.5、pygments2.21.0、python-dotenv1.2.3、typing_extensions4.16.0、typing-inspection0.4.4；pip26.2.1。hatchling仅有声明>=1.27，没有本地持久安装版本证据。

**最小复现改进建议，不升级包：** 在首台目标Linux上生成独立deployment requirements完整版本锁和wheel哈希清单，包含dev及构建依赖；若当前pin无目标wheel，停止并记录兼容性阻碍，不能静默升级。保留每个wheel及SHA256，冻结OS镜像标识、apt软件版本、Python patch、pip和hatchling版本。使用`--only-binary=:all:`避免悄悄本地编译Arrow，使用hash验证锁重建第二个干净venv，再做质量门。平台条件产生的额外依赖须明确记录，不假装Mac freeze就是Linux lock。

示例安装步骤（管理员只负责系统包/用户/目录，Python环境与运行用非root）：

```sh
sudo apt-get update
sudo apt-get install python3.12-venv python3.12-dev ca-certificates procps git rsync
# 将审核过的源树放至 /opt/crypto-agent，创建运行用户capture和其可写工作目录。
# 不复制 .env、.venv、Mac campaign脚本作为启动器，也不复制原始数据来冒充新session。
cd /opt/crypto-agent
python3.12 -m venv .venv
# 以下文件为未来Linux锁定步骤产物，当前仓库尚未提供；不存在就停止。
.venv/bin/python -m pip install --require-hashes --only-binary=:all: -r /etc/crypto-agent/linux-requirements.lock
.venv/bin/python -m pip install --no-deps --no-build-isolation -e .
.venv/bin/python -m pip check
.venv/bin/python -m pip freeze --all
```

第二条pip命令前，锁须包含已验证hatchling及其build依赖。完整源树仍保留用于source fingerprint。SSL模块、CA链、系统OpenSSL/libc版本需记录；不要求另装全套Arrow C++，首选对应Linuxwheel。PyArrow23官方安装文档列出Linux和Python3.12/3.14支持，但不能替代精确wheel存在性与本机测试。[Arrow 23安装说明](https://arrow.apache.org/docs/23.0/python/install.html)

## 4. 主机身份及配置冻结

每次严重采集之前在独立campaign根目录建立`host-provenance.json`，不得写入行情事件或策略特征。必填：

```text
host_id: 人工分配稳定非敏感ID（例vps-sg-01），不要公开原始machine-id
capture_environment: vps-singapore-01（由真实部署选择，不预设已存在）
provider_region: 实际供应商region/zone，不能仅根据IP猜
os_distribution, kernel, architecture, libc
python_full_version, interpreter_build, openssl, package_versions, wheel_lock_sha256
pyarrow_version, allocator_backend, cpu_count, RAM, filesystem, mount_options
timezone, observed_utc_offset, time_sync_diagnostics_file_sha256
source_fingerprint, full_source_file_hashes, configuration_fingerprint
full_effective_Settings, transport_constants, symbols, schema_versions
service_unit_sha256, operational_tools_sha256, environment_file_redacted_sha256
```

复用`app.campaign_observer.freeze_configuration`取得settings、transport、source及schema fingerprints；源树没有Git提交也有完整文件hash。host sidecar单独计算stable_fingerprint或SHA256。采集开始后创建独立binding，关联host sidecar hash、实际session ID、冻结配置hash；最终再关联frozen manifest fingerprint。不得在冻结后修改session-summary来补region。

核心schema当前dataset1、feature2；trade/depth/trusted_book/bootstrap1；clock/book_lifecycle/stream_lifecycle2；runtime_sample1。生成时以模块值为准，不手工覆盖版本。

只改变目的目录/时长/主机部署项，不暗改100ms、@trade、@depth@100ms、REST1000、raw100000/feature10000 queue、raw20000/30s、feature6000/60s、clock300s/5samples/1000ms。磁盘reserve若改为部署建议值，要在该新会话前显式冻结并说明，不能称与Mac完整配置相同。

mac-local和每个VPS环境独立分组。禁止混合RSS、CPU、网络延迟得出环境无关结论。未来合并市场统计也必须显式保留session/environment映射及失效边界。

## 5. 时间同步：只观察，不重写语义

现场保存以下可用输出，命令不存在时记录UNAVAILABLE而非零：

```sh
date --iso-8601=seconds
date -u --iso-8601=seconds
timedatectl status
timedatectl show
chronyc tracking
chronyc sources -v
systemctl status systemd-timesyncd --no-pager
timedatectl timesync-status
uname -a
```

chrony与timesyncd无需同时安装/运行；使用主机已有合适服务。NTP状态只是操作证据，不是event ordering输入。即使同步显示正常也允许wall step；不要运行makestep或调整clock来“修正”数据，更不要动ReplayClock。

## 6. 最小进程监督与日志

推荐systemd管理**核心CLI**，初始服务固定900秒，手动start，不enable、无Restart循环。所有路径为部署占位规范，先创建用户/目录与环境文件并运行`systemd-analyze verify`；本次未生成/安装unit。

```ini
[Unit]
Description=Public market capture qualification only
Wants=network-online.target
After=network-online.target

[Service]
Type=exec
User=capture
Group=capture
WorkingDirectory=/var/lib/crypto-capture/work
EnvironmentFile=/etc/crypto-agent/capture.env
Environment=PYTHONPATH=/opt/crypto-agent
Environment=TZ=UTC
Environment=LANG=C.UTF-8
Environment=LC_ALL=C.UTF-8
Environment=PYTHONUNBUFFERED=1
ExecStart=/opt/crypto-agent/.venv/bin/python -m app.capture --symbols BTCUSDT,BTCUSDC --data-directory /var/lib/crypto-capture/smoke-001/dataset --duration 900
Restart=no
KillSignal=SIGTERM
KillMode=mixed
TimeoutStopSec=infinity
StandardOutput=journal
StandardError=journal
UMask=0027
LimitNOFILE=4096
NoNewPrivileges=yes
```

不设Install段防止无意enable；显式start为唯一启动步骤。无限stop等待是为避免默认超时杀死flush；停机超过5分钟要告警人工检查，并不是无限容忍挂起。若机器必须硬关机或OOM杀死，必须将会话记为中断，下一次恢复验证，不能声称正常完成。不要用RuntimeMaxSec代替应用duration抢断写盘。退出时间包含flush，不把900秒当强杀期限。默认Restart=no不会自动复活；手动重开新output目录/新配置冻结，新CaptureRuntime仍会产生新session ID，不合并会话。[systemd官方服务语义](https://github.com/systemd/systemd/blob/main/man/systemd.service.xml)

EnvironmentFile只含允许的配置与必要代理设置、权限0640或更严，无Binance密钥。可由完整accepted Settings导出；不能把dotenv中注释和空字段未经检查当Shell执行。日志是INFO JSON，应用未做轮转；原observer的capture.log确实可无限增长。

部署层使用持久journald管理滚动日志，专用主机可规划约4GB journal预算，但journal自动回收会丢旧日志：**journal不是唯一永久证据**。在容量阈值前/每会话结束导出该unit及准确UTC范围的JSON日志到campaign归档，压缩并hash、复制到工作站；检查journal suppression/rate-limit提示，有丢日志则记录证据缺口。不要关闭限流后无限吃盘，不用未经验证的copytruncate处理collector日志。若复用基础observer，其capture.log绕过journal，应先限定6h会话并结束后归档；不得把这条路径无人值守无限延长。logrotate自动删除历史文件未配置也不建议默认为原始证据策略。[journald官方容量/持久化设置](https://github.com/systemd/systemd/blob/main/man/journald.conf.xml)

## 7. 网络、磁盘与资源安全

网络断开：每symbol WebSocket async context退出、记录disconnect；重连循环独立；book invalidation/candidate与buffer恢复、REST失败重试；FeatureEngine断开清history，已有book时输出invalid tick，首次book之前可能没有feature。长outage会存在invalid和未观察区间，不能用dataset_valid当无中断证明。生命周期generation、bootstrap、最终summary应保留。进程重启新session；异常旧session由startup recovery检测。上述为源码语义，不是VPS实测。

按6h观察约1.18GB/day规划，实际该口径含raw+features Parquet，不是精确raw-only恒速保证：

| 天数 | 基准Parquet | 按3倍速率 + 一份compaction输出 + 5GB操作余量 |
|---|---:|---:|
| 1 | 1.18GB | 12.08GB |
| 3 | 3.54GB | 26.24GB |
| 7 | 8.26GB | 54.56GB |
| 30 | 35.40GB | 217.40GB |

公式为基准×3×2+5，预算不是保证；OS另预留10–15GB。freeze当前是manifest/summary hash元数据，不再复制所有Parquet；compaction会额外占输出；验证还需要日志/结果。建议首个≤7日研究计划**至少80GB SSD、4GB RAM、2vCPU**，30日保留+保守重写规划选≥250GB或先分批转移保留副本；不自动删除raw/frozen。每1000files/h可能24h累计约24k条目录，需监测登记CPU及catalog工作集，不在此重设计。

首个VPS仍保留应用2GB硬reserve作为原配置，操作预警10GB，低于5GB先人工/监督SIGTERM，给flush留缓冲；若直接把MIN_FREE_DISK_GB提高到5，则冻结为新VPS配置。所有分区包括journal/OS盘都要观察。目录读取/写权限在非root用户下验证。

资源：4GB机器建议RSS预警1GB、已有observer `--max-rss-mb 1536`作为采样SIGTERM护栏，必须在试运行前冻结；不是已证明1.5GB足够。不要用紧MemoryMax/CPUQuota导致OOM或节流伪造测量；OS最终OOM仍可能发生，需保存内核证据并使会话无效。CPU看持续分布，不把单样本100%称饱和。LimitNOFILE4096配合观测，错误不能隐藏。重复crash不自动restart，先检查summary/恢复再另开session。

## 8. Clean-host资格程序（只准备，尚未执行）

### A. 安装与快速质量门

完成锁定、第二干净venv重建、source/config/host provenance后执行：

```sh
cd /opt/crypto-agent
.venv/bin/python -m pytest -q
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/mypy --strict app config exchange execution features market recorder replay research signals strategies
git diff --check
```

记录真实test count/退出码。218是此前接受基线，不冒充Linux执行结果。无Git提交/全未跟踪时diff不能证明所有内容，通过source-file hash与发布副本对比补充。

### B. 900秒公共capture

用第6节手动service，或在完整导出环境和新cwd下直接运行同一ExecStart。不要同时启动两种。目录smoke-001必须不存在，归档旧试验后换新编号，不覆盖旧输出。

可替代使用基础observer（它自动创建新output并冻结effective settings）：

```sh
/opt/crypto-agent/.venv/bin/python -m app.campaign_observer --output-directory /var/lib/crypto-capture/smoke-001 --duration 900 --sample-seconds 60 --max-rss-mb 1536
```

这两个例子二选一；observer会创建capture.log。先确认`ps -o rss=,pcpu= -p <capture-pid>`输出两数字，RSS为KiB×1024，CPU是ps口径。observer RSS针对child而不是它自己；systemd若监督observer，SIGTERM应发送主observer，由它转发并等capture结束，KillMode=mixed避免同时重复打断child。

### C. 记录与停机

检查每symbol `latest-health.json`：SYNCED、trade/depth/features递增、raw/features queue NORMAL、writer last_error为空；看bootstrap及文件轮转。最终session-summary必须FINALIZED、dataset_valid=true、disk_degraded=false、invalid_reasons空、recovery无歧义。记录disconnect/reconnect/resync、invalid ticks和覆盖缺口；不是有文件就合格。收集stdout及服务exit状态。

### D. 原始重建

```sh
/opt/crypto-agent/.venv/bin/python -m recorder.verify --data-directory /var/lib/crypto-capture/smoke-001/dataset --reconstruct
```

退出0；两个symbol都有非零bootstrap/trusted比较，mismatches=0、sequence_gap_count=0、first_mismatch=null。不要用--repair把失败试验修成无证据的成功。失败停止资格流程。

### E. 有界feature-only一致性

不用research.batch。为900秒数据使用下面已有API适配段，从`/opt/crypto-agent`执行；仅保留累计比较计数，不保留整个feature历史。结果重定向至外部campaign文件。本段尚未在Linux执行。

```python
import json
from pathlib import Path
from features.engine import FeatureEngine
from recorder.frozen import select_manifests
from replay.clock import ReplayClock
from replay.engine import ReplayEngine
from replay.features import feature_equivalence_errors
from replay.offset import RecordedClockOffset
from replay.streaming import iter_session

root = Path("/var/lib/crypto-capture/smoke-001/dataset")
entries = select_manifests(root)
assert entries
for session, symbol in sorted({(e.capture_session_id, e.symbol) for e in entries}):
    summary = json.loads((root / "sessions" / session / "session-summary.json").read_text())
    assert summary["status"] == "FINALIZED" and summary["dataset_valid"]
    cfg = summary["configuration"]
    selected = tuple(e for e in entries if (e.capture_session_id, e.symbol) == (session, symbol))
    clock = ReplayClock()
    offsets = RecordedClockOffset(cfg["clock_sync_sample_count"])
    engine = FeatureEngine(
        symbol,
        interval_ms=cfg["feature_interval_ms"],
        book_stale_after_ms=cfg["orderbook_stale_after_ms"],
        trade_stale_after_ms=cfg["trade_stale_after_ms"],
        mid_history_seconds=cfg["mid_history_seconds"],
        trade_history_seconds=cfg["trade_history_seconds"],
        imbalance_levels=tuple(cfg["feature_imbalance_levels"]),
        clock_offset=offsets,
        monotonic_clock_ns=clock.monotonic_ns,
        wall_clock=clock.wall_time,
    )
    counts = {"compared": 0, "tolerance_mismatch_rows": 0, "exact_difference_rows": 0}

    def compare(actual, recorded):
        assert recorded is not None
        counts["compared"] += 1
        counts["tolerance_mismatch_rows"] += bool(feature_equivalence_errors(actual, recorded))
        counts["exact_difference_rows"] += bool(
            feature_equivalence_errors(actual, recorded, absolute_tolerance=0)
        )

    result = ReplayEngine(
        iter_session(root, selected),
        clock,
        engine,
        retain_results=False,
        feature_handler=compare,
        clock_offset=offsets,
    ).run()
    expected = sum(e.row_count for e in selected if e.event_type == "feature")
    print(json.dumps({"session": session, "symbol": symbol, "source_rows": expected, **counts}))
    assert result.data_quality.valid and expected == counts["compared"] > 0
    assert counts["tolerance_mismatch_rows"] == 0
    assert counts["exact_difference_rows"] == 0
```

严格全字段exact差异为0足以证明Decimal及float差异均0。若只有float精度差异，默认文档容差1e-12仍适用，但本保守资格脚本会停下供人工列出Decimal/float字段和最大误差，不能自动忽略。检查两symbol均有输出。不要对多日数据误用这段为无限时测试；这里仅900秒session。依赖/硬件变化时重新验收。

## 9. 准备下一次VPS 4–6h资源资格，不执行

只有A–E通过才另开VPS resource session。目标6h，4h中止不能称6h通过。复用基础observer安全线；每60秒外部读取**capture PID**的`/proc/PID/status`（VmRSS/VmSize/VmHWM kB、Threads）、`/proc/PID/fd`数量/链接socket数量、`ps` CPU、uptime、free disk、catalog bytes/entries、已有latest-health，append到独立JSONL。读取失败记null/error而非0；记录process start防PID复用。不要套Mac libproc或在capture中对象遍历。这个完整Linux外部采样适配仍是启动资格前的部署工具准备项，不声称本次已经完成执行验证。

内部book/cache/history/Arrow更细gauge若需要，可未来将read-only scalar probe单独做Linux适配并提前冻结/短测，不能直接使用错误单位的Mac版。本轮仅定义方案，不增加框架。没有这些指标就报告缺失，不能以推测补齐PASS证据。

结束再raw重建和feature-only比较；比较first-useful/小时RSS、后10min/1–2h/2–4h/后3h/后2h回归、hourly deltas；FD/thread必须回落/平台，queue CRITICAL0，storage/catalog可管理。任一数据损坏、disk触发、source/config变动、OOM或不明中断停止资格。Linux通过只证明该host/config适合长采集，Mac仍是RESOURCE_GATE_WARNING。未授权自动安排24h。

## 10. 冻结与传输

采集完全结束且上述验收通过后，按实际目录执行：

```sh
python -m recorder.verify --data-directory /var/lib/crypto-capture/smoke-001/dataset --freeze
# 保存命令返回的真实frozen路径与fingerprint，不手工编造。
python -m recorder.verify --data-directory /var/lib/crypto-capture/smoke-001/dataset --frozen /var/lib/crypto-capture/smoke-001/dataset/frozen/ACTUAL_FINGERPRINT.json
```

`ACTUAL_FINGERPRINT`必须替换为真实返回值。采集host侧manifest不动；host/provenance/binding也随campaign一起转移并有独立SHA256。VPS冻结源保留，不在源目录compaction；如需实验输出新目录新版本，原freeze保留。

工作站创建新的接收staging目录，使用授权SSH目的地址：

```sh
rsync -a --partial --checksum capture@AUTHORIZED_HOST:/var/lib/crypto-capture/smoke-001/ /path/to/new-staging/smoke-001/
python -m recorder.verify --data-directory /path/to/new-staging/smoke-001/dataset --frozen /path/to/new-staging/smoke-001/dataset/frozen/ACTUAL_FINGERPRINT.json
```

不用--delete、--inplace，不覆盖现存冻结副本。rsync checksum不是最终验收替代；verify_frozen会重算manifest fingerprint、每个文件及session-summary SHA256并检查schema/行标识。还要比较host sidecar/binding hash。成功后标记接收副本可研究；失败保留staging并重传，不修源数据、不修改内容时间戳。上述/path占位是部署示例，不是运行时硬编码。

## 11. 最小安全范围与本次变化

只需公网HTTPS/WSS出站及DNS/系统时间服务；入站除限制来源的SSH外不需要行情服务端口。用SSH key、非root runtime、最小防火墙、及时安全更新；更新安排在会话外并重冻环境。没有Binance API key、secret、交易/提币权限或认证endpoint。TRADING_ENABLED保持false且Settings拒绝true。

本次只新增本文档。未改变任何核心模块、测试、schema、策略参数、历史frozen/provenance；没有制造commit，没有运行昂贵历史验证。因为没有核心修改，不重跑218测试或宣称Linux质量门已通过；未来Linux的实际结果必须单独记录。已按源码核对示例CLI参数和API，未在无授权VPS执行。

VPS_READY — NO CORE CHANGES REQUIRED
