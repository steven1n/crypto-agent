# 6h Stage B campaign — 收尾中止报告

审阅时间：2026-09-10 04:00 Asia/Singapore。状态：STOPPED_INCOMPLETE，尚未完成Stage B研究验收，不赋予预测或经济分类。

## 已完成与失败位置

采集实际UTC 2026-09-09 05:33:01.379480至11:33:01.345120（本地13:33:01至19:33:01），21599.96564秒；进程19:33:27正常退出，exit 0。唯一session为71ca7214cca243b19a225e19db347033，FINALIZED且dataset_valid=true。午夜断网前已结束。但有大量恢复重连，不能称为无中断的干净覆盖。

| 项目 | BTCUSDT | BTCUSDC |
|---|---:|---:|
| 特征行 | 211774 | 211836 |
| trade | 434457 | 165367 |
| depth | 154630 | 186789 |
| bootstrap | 115 | 11 |
| 原始重建比较盘口 | 151915 | 186418 |
| raw差异 / gap | 0 / 0 | 0 / 0 |
| 特征Decimal / float差异 | 0 / 0 | 0 / 0 |
| reconnect | 110 | 4 |
| resync | 115 | 10 |
| 无效特征ticks | 49250 | 1810 |

原始重建first_mismatch均null。特征retain_results=False、源行exact，最大float误差0、first_mismatch null。无效tick比例约23.26% / 0.85%，不是精确无效持续时间。尚未进一步汇编生命周期覆盖，不应以dataset_valid替代连续覆盖证明。

恢复报告invalid、quarantined、reconciled、interrupted_sessions均为空。冻结与verify已完成，6759文件、241427998字节、1747594总行：
- frozen ID/manifest fingerprint：d95fc3228c352742e5e4a87c9e56b3e0f4bed8e30e9c69381a2915bf954a589b
- source：2822085ea27f48d346d3f61f4dafd6a0a1c70c5620400fa281a943de3efadf93
- config：9fbfdf7f304159ef500004d33c1f6a4ef838a3ccef3602138ce0a6ee397d04ea
- controls：cc225b299a296c97d626bda39f7ee9045638ee298084f44727e3abbb34bfbb2d

既有Stage B batch进程exit0，耗时3768.790秒；随后campaign.py第286行直接比较配置字典，2026-09-09 21:38:28本地报错“research controls changed”并停止。

## 只读根因核对

这是操作包装器的容器类型比较缺陷，而不是当前证据中的参数数值变化：
- 内存中的asdict(BatchConfiguration()).cost_grid_bps是tuple；
- JSON读回的batch configuration.cost_grid_bps是list；
- Python直接字典相等为False；
- 对内存配置按JSON规范化后，与batch配置完全相等；
- 保存的frozen-configuration.json研究配置与batch配置逐字段差异为空。
- 当前核心文件hash全部与冻结配置一致；冻结manifest的文件hash与记录一致。

原始错误和STOPPED_INCOMPLETE状态保留，不修改包装器、不重跑batch、不修改冻结数据，也不自动将其升级为已完成研究验收。此次只作失败事实与类型差异诊断，未进一步解读已有预测/经济结果。

已有batch路径：
data/clean-6h-stage-b-20260909/dataset/research/batches/1d3b8aefbd055cc8c8e11c7a4a36abd5ee53ec6ec9e89f3b324e864b90f3b151/batch-report.json

## 独立资源结果

一分钟样本RSS峰值/最后样本602.161MB；这是采样峰值，不代替live getrusage峰值。最后3h回归斜率+47.03MB/h，最后2h+61.74MB/h，长时上界未建立。FD样本max12，线程max5；CPU样本median17.7%、p95 90.9%。详细小时资源保留于summary.json/resource_result与resources.jsonl。本轮不会把资源WARNING升级为PASS。

RESOURCE_GATE_WARNING — LONG-RUN BOUND NOT ESTABLISHED

## 下一步与边界

本自动任务按失败停止规则收尾。需要用户明确授权后，才继续对现有产物做报告级验收；已有证据没有要求重新采集或重跑重任务。尚未完成所有feature/horizon的小时稳定性、依赖与经济表格，因此不作Stage B最终分类。ps在当前沙箱被拒绝，无法独立核对当前PID；退出结论依据已记录的exit code、终态和traceback，不把旧PID存在与否当证据。

未改变核心源码、配置、策略、阈值、features/horizons、bootstrap或任何市场/回放语义；未启动新采集、修复或优化。
