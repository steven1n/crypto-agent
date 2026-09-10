# Offline research / replay / batch complexity audit

2026-09-08。仅静态源码及已有小型测量JSON读取；未运行batch、回放、重建、compaction、profiler、测试或新capture，未修改源码、控制参数和历史数据。唯一新增内容为本文档。

## 1. 总结

**当前主batch有合理的小时级内存边界，但不足以直接签发24h/多日处理资格。** 最强的正面证据是`iter_session`流式输入、`retain_results=False`、逐小时片段释放、session Pearson合并矩，以及按块预聚合bootstrap。最重要的风险是：

1. 原始重建的50,000条完整depth缓存和Decimal盘口计算，并非“streaming所以内存接近零”。
2. 每个feature/horizon反复进行排序、ACF及regime统计，有很大的CPU常数。
3. legacy全量loader、默认retain_results=True及独立策略evaluation不是多日安全入口。
4. frozen校验/读取多次扫描文件，小文件、manifest及报告元数据仍随文件/片段数增长。

没有证据证明必须现在优化。适宜结论是先取得同工作负载、同环境的分阶段资源测量；不要根据不同验收流程做一个“每行耗时”回归。

### 覆盖差异，不在本次修改

附件预算为10个feature，当前`research.batch.FEATURES`实际上只有9个：microprice、L1/L5/L10 imbalance、trade imbalance 1s/5s、return250ms/1s、rv1s，**没有rv5s**。6个future horizon全部存在。当前非重叠输出仅Pearson，没有非重叠Spearman；session统计为合并小时moments的Pearson；最终报告提供跨session稳定性，不计算全session或跨session精确Spearman。不能假设这些尚未实现的计算已包含历史runtime，也不在本次补功能。

## 2. 阶段图与符号

```text
frozen JSON全量加载/逐文件hash和行验证
  → session/symbol manifest分组（元数据全量）
  → iter_session：每类512行批次 + capture_seq k路合并
  → ReplayEngine：recorded clock / FeatureEngine
      同流consumer：raw reconstruction、bounded latency、生命周期边界
  → 单feature比较 + digest + HourBuffer
  → 小时连续片段 + 单调时间未来后缀
      labels → diagnostics/ACF/ESS/bootstrap → 4 baseline × 6 horizons × 9 costs
  → 当前片段labels Parquet + segment JSON，释放行对象
  → 保留精简segment moments → session统计 → 最终JSON
```

E=原始及feature-tick总事件，N=feature行，n=当前片段含未来上下文的feature数，F=文件数，P=实际小时连续片段数，S=session-symbol组数，K=事件类型流数（约9，固定），D=当前盘口层数，L=每条depth更新层数，W=短期history大小，C=min(50,000,近期depth数)，H=6个期限，Q=9当前feature（规划10），A=6个ACF lag，J≤50个ESS lag，B=200，M=当前片段非空10s时间块数，G=9成本点。空间以对象数/数据体积表示，不把压缩Parquet字节当解压后Python体积。

## 3. 复杂度清单

| 模块/函数 | 模式及主要内存 | 时间估计 | 空间估计 |
|---|---|---|---|
| catalog.entries / frozen.verify_frozen | 全manifest JSON/dict及验证局部批次 | O(F)+所有文件hash/投影行扫描；schema/footer开销 | O(F)+单验证批次 |
| frozen.select_manifests | 全目录后选完整session、排序 | O(F log F) | O(F) |
| batch.run_batch fingerprint/groups | frozen字典、entries、asdict序列化、分组引用并存 | O(F)序列化+分组排序 | O(F)，不是O(N)行情行 |
| replay.streaming.iter_session | 每类型512行全列Arrow batch及to_pylist，heap head | O(E log K)+文件验证+decode+O(F log F)分组排序 | O(F+K×512×行宽)，临时验证批次可能更大 |
| ReplayDatasetLoader.load / load_feature_ticks | legacy全symbol/session list、排序、tuple | O(E log E) / O(N log N) | O(E) / O(N)，外加单文件全表/Python副本 |
| ReplayEngine.step/run retain_results=False | 单个previous event、clock、consumer及handler | O(E)+下游处理 | 引擎自身O(1)历史；总体由输入/consumer决定 |
| RawReconstructionValidator.accept | recent OrderedDict完整depth、LocalOrderBook | 约Σdepth O(D+L) + Σtrusted O(D log D)；bootstrap重放buffer另计 | O(C×depth体积+D)，非全session但可很大 |
| FeatureEngine重算 | latest top10 book、mid/trade短窗口 | book处理与trade逐出摊销；每feature returns/RV扫描W，约O(NW) | O(W)，受history硬界保护 |
| feature_equivalence_errors / batch handler | 每行asdict两份、局部errors、digest | O(N×字段数/字段体积) | O(单feature字段体积)，错误fail-fast |
| HourBuffer | list、capture_seq dict、boundaries | append摊销；每次finish O(n)身份校验/删除/清理 | n≤100,000；常态36,000+约100条后缀 |
| LabelGenerator.generate | timestamps、invalid_prefix、labels tuple | O(nH(log n+log I)+nΣw_h)，I为边界数 | O(nH)+当前excursion path，H固定 |
| chronological_split | 输入全tuple，timestamps和3组引用 | O(N log N)因purge逐行bisect | O(N)引用；不深复制features |
| analyze_feature Pearson/Spearman/bins | pairs、observed、left/right、sorted/ranks | O(n log n)，Pearson本身O(n) | O(n)，多份数组/引用并存 |
| non_overlapping_indices | 顺序扫描、eligible index tuple | O(n) | O(n_h)输出，但仍扫描全部n |
| autocorrelation | 每行bisect前lag目标，left/right | O(n log n)每lag | O(n)临时 |
| effective_sample_size | 非重叠x/y，slice→list | O(J n_h)，遇非正相关早停 | O(n_h)每lag临时，不是O(J n_h)同时保留 |
| block_bootstrap | 每块4个聚合数、每次抽样M个引用、B个scalar draws | O(n + BM + B log B) | O(n)调用端condition/tuple + O(M+B)函数内；不是O(nB) |
| batch baseline_edges | 每策略evaluated列表，逐horizon movements | O(4n+4Hn+4HG) | O(n+4HG)；四策略顺序执行 |
| batch labels输出 | flatten整片段dict list→Arrow table | O(n×字段数) | O(n×展开字段体积)，与feature/label短时并存 |
| session/report assembly | compact segment moments/summary、sessions/stability | O(PQH+SQH)，JSON写出同量级 | O(F+PQH+SQH)，不保存全部行情行 |
| compact_parquet_fragments | 子集全表、concat、sort、readback | O(m log m)+I/O，m为子集行数 | 多份解压表O(m×行宽) |
| compact_dataset | 全metadata分组/子集，逐子集处理 | 验证+上述sort；output catalog重复登记可能O(F_out²)元数据工作 | O(F)+单子集峰值 |

I、L、D、W不固定时不可简写所有事件处理为同一O(E)常数。上述排序时间为典型算法阶，不宣称底层Arrow实现精确常数。

## 4. 物化清单与所有权

| 位置/对象 | 分类 | 是否主batch路径、生命周期 |
|---|---|---|
| frozen JSON、manifest列表及group引用 | NECESSARY_FULL_MATERIALIZATION（当前API） | 主路径O(F)，保留到batch结束；可未来设计流式但现在无需架构替换 |
| fingerprint中asdict所有entries、编码JSON bytes | AVOIDABLE_COPY | 主路径启动瞬时O(F)，hash后局部释放；可能提升allocator高水位 |
| legacy loader `_load_file` table.read/to_pylist + load items + sorted tuple | STREAMABLE | 非主batch；全session对象会被返回且被ReplayEngine._events引用 |
| `load_feature_ticks` / `merge_replay_streams` | STREAMABLE | 非主batch；全feature列表与排序后的总tuple，不能当流式入口 |
| ReplayEngine默认两组feature list→ReplayResult两tuple | STREAMABLE | retain_results=True才积累；关闭并不会释放调用者已创建的全events tuple |
| iter_session每类to_pylist | AVOIDABLE_COPY（Python decode目前需要值） | 512行；Arrow batch和Python rows同时存在，yield暂停时还保留该批次 |
| iter_session全列读取 | UNKNOWN（逐类型确认后才可投影） | 完整decode/重建多数列需要；feature-only replay仍读bootstrap等不消费payload，存在未来选择性decode候选，不可直接跳过ordering/identity验证 |
| labels/timestamps/invalid_prefix/ResearchRow tuple | NECESSARY_FULL_MATERIALIZATION（当前片段API） | n级；ResearchRow引用feature，不深拷贝整个feature；LabelSnapshot是新对象 |
| hourly eligible/selected/quality/healthy/observed_features | AVOIDABLE_COPY | 多份引用而非每份deepcopy，均随flush局部返回释放 |
| analytical_rows flatten + Arrow output table | STREAMABLE | 目前一片段全物化，Decimal转字符串和新dict真正增加体积 |
| correlate pairs/observed/ordered/ranks；regime selected | AVOIDABLE_COPY | 每feature/horizon和regime反复创建；不累积所有期限全数组到report |
| bootstrap condition/t/sx/fy | AVOIDABLE_COPY | condition n级，输入tuple也n级；replications只保留scalar |
| compact所有source tables/sorted canonical/validated | NECESSARY_FULL_MATERIALIZATION（当前算法） | helper本身无总容量参数；CLI wrapper按100文件/64MB压缩/200k行限子集。不是RAM64MB限制 |
| `evaluate_strategy` signals/trades、`evaluate_fixed_horizons`所有EvaluationResult | STREAMABLE | 独立shadow-evaluation接口，非batch gross-edge路径；调用端全session输入+每horizon输出可能O(HN) |
| ResearchRunWriter trades/signals asdict列表→Arrow | STREAMABLE | 独立run artifacts路径，随evaluation结果大小增长；主batch不使用 |
| data_quality_report.invariant_errors | UNKNOWN（故障数据规模） | 每行每异常可append，坏片段O(n)文本；fail时仅展示前5不等于只收集5 |

主batch没有pandas/DataFrame或DuckDB/Polars懒执行；“streaming”来自Python iterator+Arrow批次，不能根据技术名称推断成本。没有发现主batch `.read_all()`，但`.read()`的legacy/compaction路径效果同样是全表读取。

## 5. Replay与完整feature comparison

ReplayEngine._events保持输入对象引用：若传全tuple，关闭results也仍持有全部events；若传iter_session生成器，则逐步消费。_previous_event仅一个。retain_results=False使_features和_recorded_features不append，ReplayResult对应tuple为空；无事件结果数组。

主batch正确传iter_session、关闭results。消费者保留：reconstruction最近50k完整depth（约100ms下83分钟量级）、latency每分布6000float+50条tail、生命周期与capture_seq按当前hour清理、FeatureEngine短历史、hour buffer。不是O(1)总内存，而是O(F+bounded payload+hour+report metadata)。正常缺失重算feature由最终expected/compared accounting失败，不能静默漏行。

`feature_equivalence_errors`递归比较每字段，先asdict实际/记录；只返回本行错误路径tuple。batch遇首行错误抛错，正常只保留compared和digest。比较本身额外空间与N无关，时间O(N×字段大小)。重算FeatureSnapshot会因后续小时分析保留，而不是比较器保留；记录feature仅当前iter batch引用。不能将“比较row-by-row”错误描述成“batch只用一条feature内存”。

新6h资源验收的`finish_soak.verify_symbol`是另一条campaign路径：比较counter流式，但raw阶段累积每event lag数组、runtime_rows、clock_hours及wall_steps；这些是**O(E)标量/tuple辅助诊断**。`retain_results=False`对此无效。900秒适配段仅比较counter则没有这组全session诊断。未来纯24h replay测量不要不加区分地复用完整soak后处理并称为O(1)。本次只审阅，不改脚本。

## 6. UTC片段与标签正确性成本

按连续source-order civil-hour前缀处理，civil hour可再次出现，后缀到最长10s未来条件满足才flush；不排序/夹紧wall。100ms常态每片段约36,000行，加约100条future context；max_hour_features100,000 fail closed。capacity是行上限不是MB预算。

每flush先用完整buffer生成label，再按前缀/时间范围输出；HourBuffer.finish检查(monotonic_ns,created_at)顺序1:1，仅删除已输出前缀。boundaries与capture_seq dict随后清理。EOS逐片段排空。报告保留精简stats，不会再次把所有labels Parquet读到一个大DataFrame。

任意频繁wall震荡可能增大P，产生很多小fragment并重复计算上下文；正常“每小时”成本估计不适用于P≈N的极端civil clock行为。不是新的row-retention正确性问题，也不在此改变闭合的语义。墙钟长期停在同一civil hour可触发容量fail-closed而非无限RAM。

六个future return目标全部`bisect_left(timestamps,target,lo=index+1)`：O(log n)，不是每目标O(n)线性搜索。invalid_prefix是O(n)构造/O(1)区间无效检查；显式生命周期边界通过两次bisect_right判定O(log I)。>3s观察gap也标入prefix。excluded range变成附加边界，排序O(I log I)；不允许跨restart/无效区间。

1s/2s/5s excursion另对路径切片计算所有返回并max/min；100ms下通常10+20+50≈80项/feature，所以近似O(n)但常数大；任意密度下严格上界可以O(n²)，不能给所有输入保证O(n log n)。当前10Hz及hour cap下通常没有这种密度。返回/RV的live engine历史也扫描短W，并非整session。

chronological_split单独接受完整ResearchRow tuple，timestamps全长；purge每行bisect，O(N log N)和O(N)引用。**主batch没调用它**，当前regime边界为描述性小时quantile，不是formal TRAIN/VLD/TEST。不能把已有formal split接口存在，描述成每次batch都执行全套out-of-sample评估。

## 7. Correlation、ACF、ESS、bootstrap与成本曲线

每segment当前Q×H=54关系。`analyze_feature`为每关系创建pairs/observed、两数值数组、用于bins的排序，以及两个Spearman ranking。三个regime又调用同一完整analyze_feature，虽然最后只拿Pearson，**仍计算未输出的Spearman/bins**。约216次analyze调用/片段；规划10feature则240次。排序空间仍O(n)峰值，不是同时216份n深拷贝。

每关系signal和return各6lag，共648次ACF调用/片段；规划10则720。每次ACF遍历全部时间戳并binary search，O(n log n)。signal ACF对相同feature的6个horizon重复；return ACF跨feature也重复。ESS在非重叠x/y各最多50lag，切片tuple再list，成本O(100n_h)每关系；实际遇非正rho早停。非重叠indices每关系重算O(n)，同horizon可重复9次；缺失值过滤后的n_h不保证绝对等距，ESS本来就是近似，不以性能名义改统计定义。

24h双symbol通常48个小时片段（首尾不齐可更多），Q9时约2592关系；Q10规划2880。再乘2组6lag ACF与regime重复排序，CPU放大明显。session moments合并只需O(PQH)标量，不重算全sessionrank；主batch没有全局精确Spearman，所以后者不计入投影。

Bootstrap只用于imbalance_l5，6horizon×2统计（high mean/high-minus-low）=12调用/片段，不是54关系全做。实现先把n个样本归并每10s块的四个和/计数，每replication抽M个块引用并求和，最多200个scalar draws排序，返回置信区间而非draws全集。常态1h M≈360，每调用≈72,000块抽取，12调用≈864,000/小时片段；24h双symbol≈41.5百万块抽取，另有sum和random开销。时间O(n+BM)，内部空间O(M+B)，调用端仍有O(n)条件数组；不是200×完整数据复制。不能减replications、block时长或选择更好看的区间。

四baseline每个先evaluate所有healthy row一次，保留(row,signal)列表；然后6horizon各扫描建立movements。不是每成本重演策略。九成本仅对gross scalar减法及ratio计算O(G)，已很轻；总O(4n+4Hn+4HG)，不会把所有horizon labels深复制。每策略evaluated在下一策略替换，前一局部列表可能到赋值后才释放但不全局累积。独立shadow evaluation则保存signals/trades与多horizon结果，是不同内存模型。

## 8. Arrow、文件与输出

- `validate_file`逐文件hash、schema/footer检查，iter_batches投影5列（capture_seq/symbol/session/schema/dataset），未显式batch_size，受Arrow默认批次大小而非512约束；大compacted文件会看到比stream更大的验证批。row to_pylist有Python转换开销。
- `iter_session`先validate_file再重新开reader，512行全列decode；k路yield暂停时每stream保留当前batch+Python row list，header/Parquet rowgroup/字典解码native内存还受文件布局影响，512不是全进程硬MB上限。
- frozen.verify_frozen对每文件validate_file（内含hash）后又hash；freeze同样额外hash；主batch之后iter_session又validate。存在重复I/O，但不能在没有immutable/trust边界证明前跳过完整性验证。
- loader/features legacy `.read()`无projection；compaction `.read()`所有列是为完整内容相等检查需要，合并/排序/readback可能多份Arrow表并存。64MB压缩输入不能保证64MB/128MB RSS；嵌套深度和Python反序列化可放大。
- 多处ParquetFile依赖局部析构，未显式context/close；每stream正常只有当前reader，无应用保存所有reader的列表。异常或生成器停止后的释放时点值得之后FD测量，但静态证据不等于泄漏。
- labels展开list及Arrow表只在flush内；最终batch JSON保留每片段54组compact moments和少量market summaries，并非行、所有bootstrap draws或ACF序列。ACF只6个标量点；完整relationship JSON写在segment文件。O(PQH)可达数MB/数十MB，正常24–168h不是N级主要风险，但很多tiny fragments会放大。
- ResearchRunWriter独立路径将所有signals/trades再asdict/JSONable转换写表，可能显著；不能替代主batch的内存结论。
- recover_dataset还对文件名列表做membership扫描，坏目录/大量文件的恢复路径可有O(F²)列表查找；catalog注册每次全量重写也可有O(F_out²)累计开销。冻结verify的主要正常循环不是因此一概O(N²)。

## 9. 已有测量（不重跑，不混合拟合）

| Workflow | 规模 | Wall time | Peak RSS | 包含范围 / 限定 |
|---|---:|---:|---:|---|
| StageA完整batch | 142,071 features / 2symbol | 1,766.54s外层；内部elapsed1,715.94s | 1.493GB | frozen/replay/reconstruction/feature比较/hour统计；阶段不是单独label吞吐 |
| wall-fix验收 | 352,346 features；1,272,963 events | 6,909.22s | 1.921GB | 组合验收/重复验证等，非单次纯feature replay；不从总时长反推每个阶段 |
| hour-retention最终regression | 352,346 analysis rows | 270.81s | 0.934GB | 已有特征/label/行与旧输出对账；不含raw重建和完整统计bootstrap |
| StageA独立raw verify | 130,275 trusted books | 精确结束时间unknown；1225s仅最后观测elapsed | unknown | 不把最后观测当精确耗时 |
| 新6h raw重建+辅助观测 | 399,191 trusted；2,142,684总events | USDT3584.63s + USDC1188.41s ≈4773.05s | 独立阶段unknown | iter_session全事件decode及latency/runtime辅助收集；不是纯Decimal内核 |
| 新6h feature-only replay | 418,227 features | USDT567.26s + USDC350.37s ≈917.63s | 独立阶段unknown | 完整stream decode+FeatureEngine+字段比较；不含盈利统计 |
| StageA compaction本体 | 504,321全类型rows；2188→36 files | 11.95s | children累计peak约0.443GB | 外部child max RSS不是每步严格独立峰值 |
| compacted StageA全batch验证 | 同一批特征 | 1847.36s | children累计peak约1.567GB | 不比未compact1766s快；不是控负载A/B基准，不能断言compaction必然加速 |

依据：`data/campaign-20260905/stage-a-2h/{batch-measurements,compaction-measurements,validation-status}.json`；stage-b的`wall-fix-acceptance.json`、`utc-hour-retention-regression-final.json`；`data/resource-soak-20260908-6h/integrity-BTCUSDT.json`与BTCUSDC对应报告。此前版本/控制的细小差异及机器状态也不是统一基准；没有新测量、没有从这些运行得出alpha结论。

## 10. 24h容量预算，不是性能承诺

输入假设每symbol864,000feature/日，共1,728,000；事件数和深度消息体积可能更大。正常按两个symbol顺序、每片段36k、最长10s后缀；未假设并行加速。目标同量级工作站或8GB Linux主机，不外推跨环境精确RSS。

| 24h阶段 | LOW时间 / RSS | CENTRAL时间 / RSS | HIGH时间 / RSS |
|---|---|---|---|
| 原始重建+必要decode/验证 | 2–4h / 0.5–1.5GB | 4–8h / 1–3GB | 12–36h / 3–8GB+ |
| feature replay+逐行比较，不带全event观测数组 | 0.5–1h / 0.3–1GB | 1–3h / 0.5–2GB | 4–10h / 2–5GB |
| 主full batch（已含重建/比较/统计，不再相加前两行） | 4–7h / 1.5–2.5GB | 8–16h / 2–4GB | 24–72h / 4–8GB+ |

LOW=低事件密度/少盘口层/快CPU/本地热缓存；CENTRAL=最近活动量级与常态片段；HIGH=深度密集、盘口大、慢vCPU/存储、allocator高水位或fragment频繁。以上只是运营排程的宽区间，不是由3个异构benchmark拟合得出。实际native轮子、CPU、D/L分布可能使范围失效；不将HIGH视为可接受。

可核对的条件锚点：新6h raw4773s在**活动、D/L和机器相同**假设下×4约5.3h，feature918s×4约1.0h；StageA batch1766s按近似小时片段数×12约5.9h。但clean24h无重连时缓存/盘口驻留可能不同，阶段统计覆盖也不同，不能把这些锚点当预测值。352k retention271s绝不意味着完整1.7M batch仅22分钟。

内存不按1.7M/142k线性放大主batch小时行：hour峰值大致相同，增长主要来自F、P、native高水位、depth缓存体积。C=50k若每event100个层、每层约250–300B，光缓存价量粗估1.25–1.5GB；不是实测，也说明“固定50k”不等于小RAM。legacy全量入口则可随E/N线性膨胀到8GB以上，不属于上述正常主batch预算。

## 11. 3日 / 7日

3日约5.18M features，7日约12.10M。按每session/symbol迭代与hour释放，主要数值工作在稳定D/L时近似按小时数线性增；peak行对象不必×3/×7。metadata文件可约72k/168k（按1k files/hour），全量frozen JSON、多份manifest、serialization以及恢复/compaction的二次列表工作越来越显著。CENTRAL8–16h/日可意味着3日24–48h、7日56–112h顺序总时长，仅为条件容量预算，不建议现在直接跑7日全batch。

CPU更可能受Decimal+排序、重复diagnostics限制；RAM更可能受depth payload、legacy入口、compaction解压以及metadata限制。一个7日continuous session也不是等同七个独立1日：book状态、F、报告汇总及时间选择成本不同。`--from/--to`会保留完整session bootstrap上下文，frozen路径更先验证整个冻结选择；不能认为只选一小时就只读一小时。之后若分阶段/分session运行需保留正确session/lifecycle和hour label上下文，不可任意截断。

## 12. Top5瓶颈与优化风险

| 排名 | 候选 / 源码证据 | scaling / confidence | 未来分类 |
|---|---|---|---|
| 1 | raw重建Decimal字典copy、每trusted全侧排序；50k完整depth缓存 | 时间E×D/log D，RAM C×payload；机制高，耗时占比需阶段测量 | 保持Decimal语义HIGH_CORRECTNESS_RISK；只做测量/减少无用包装可SAFE_PERFORMANCE_CANDIDATE |
| 2 | 54关系×重复regime排名、signal/return ACF跨horizon/feature重算 | 时间PQH×(n log n+A n log n+J n_h)，RAM n；机制高 | 已相同输入的复用SAFE_PERFORMANCE_CANDIDATE；rank/tie/缺失掩码必须等价 |
| 3 | 全列decode/to_pylist、per-row asdict/digest、重复文件校验 | 时间E×字段/bytes+F开销；批次/metadata RAM；高 | 投影、减少复制SAFE_PERFORMANCE_CANDIDATE；取消验证HIGH_CORRECTNESS_RISK |
| 4 | 调用到legacy全session loader / retained replay /多horizon evaluation | O(E+HN) RAM及全局sort；风险高但主batch未走 | 选择已有stream入口SAFE_PERFORMANCE_CANDIDATE；无需改全架构 |
| 5 | 小文件catalog/frozen/compaction多表排序及长期metadata | O(F)内存；部分恢复/登记O(F²)，compaction O(m log m)；中高 | 先度量NOT_CURRENTLY_NECESSARY；新版本有界compaction已可用，但不是自动提速承诺 |

Bootstrap虽有12×200重复，是第一级之后的CPU候选，**已预聚合**且不保存N×B；无证据把它强行排第一。JSON final通常只存moments，不是当前首要内存瓶颈。

## 13. 仅规划的路线与门槛

P0 — 不改：保留streaming主batch、retain_results=False、hour-accounting、fail-closed容量、monotonic/lifecycle标签语义、centered moments、block预聚合、scalar cost曲线。正确慢不等于该重写。

P1 — 只有测量需要时：明确入口选择；独立阶段计时/peak；验证后减少相同数组/ACF/non-overlap重复构造、逐类型column projection、显式reader生命周期、尽早释放展开片段；每一处必须证明source rows、Decimal/float容差、mask/边界和fingerprint变化原因。不要以少跑feature/horizon/bootstrap换速度。

P2 — 24h实测超目标才做：vectorized诊断、rank缓存、bootstrap抽样索引复用。浮点归约次序、rank tie、missing mask、seed RNG调用序列均可能影响结果，属于需严格回归证明而非天然无风险；labels若改two-pointer/excursion算法也需反例/生命周期/rollback精确证明。

P3 — 无充分证据不做：替换Decimal、重写ReplayClock/UTC保留语义、其他语言重写全回放、替换存储/目录架构。禁止减少统计覆盖、优化阈值、换策略或引入模型。

建议8GB机器资格目标：离线单进程peak≤3GB理想、≤4GB可接受；>4GB警告并保留OS余量，接近6GB安全停止且报告未完成，不让OOM被视为成功。不要和live capture同机并行重分析。24h完整流程目标≤12h，12–24h排查，>24h不满足日更节奏、暂不批量3/7日；这些是本项目资源预算不是行业定理。

先把raw integrity、feature equivalence、研究统计分别计时（可独立进程隔离allocator，但重复I/O需计入）；不要为了拆阶段削掉验证。确认同一source/config、完整冻结fingerprint、每symbol计数和精确比较再测日级。需要的下一个证据是**代表性同工作负载的分阶段时间/RSS与D/L/F指标**，不是再跑已完成历史验收。本文没有执行任何该计划。

OFFLINE_PIPELINE_SCALING_UNCLEAR — MEASUREMENT NEEDED
