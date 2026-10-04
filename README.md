# 野生动物疫病监测与离线同步

这是一个只使用Python标准库和SQLite的模块化项目，默认端口为`8305`。所有业务规则集中在`src/rules.py`，`app.py`只负责组装依赖和启动服务。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动和信号处理。
- `src/domain.py`：角色、数据结构、领域异常和基础校验。
- `src/rules.py`：状态机、权限、领域计算、冲突和跨对象校验。
- `src/repository.py`：SQLite建表、查询、事务、乐观锁、离线批次与检查点。
- `src/service.py`：用例编排、幂等处理、版本控制和审计写入。
- `src/offline.py`：可续传离线批次（稳定编号、重放、陈旧冲突、失效重算）。
- `src/device_client.py`：巡护设备本地持久化队列与可续传上传器（标准库）。
- `src/http_api.py`：HTTP路由、请求解析和统一错误响应。
- `src/audit.py`：实体操作审计时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则和失败场景测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8305
```

服务启动时会自动建表。`--host`可修改监听地址，`--db`可指定其他SQLite文件。

## 核心对象

- `observation`：现场观察；`sample`：样本与实验室结果；`cluster`：异常聚集事件。

## 主要接口

- `GET /health`：健康检查。
- `GET /api/<kind>`：按对象类型查询，可用`?status=`过滤。
- `POST /api/<kind>`：创建对象；请求体为JSON。
- `GET /api/entities/<id>`：读取对象当前版本。
- `POST /api/entities/<id>/actions`：提交`{"action":"动作名","data":{...},"expected_version":数字}`。
- `GET /api/audit`：读取审计记录。

请求身份通过`X-User-Id`和`X-Role`请求头传入。创建和动作的可执行角色由规则引擎控制。

## 可续传离线批次

巡护员无网时在设备上登记观察和样本，回站后以"批次 + 稳定事件编号"上传：

- `PUT /api/sync/batches/<batch_id>`：提交批次（同一批可用相同`batch_id`反复重传）。请求体：
  `{"device_id":"设备号","entries":[{"seq":1,"op":"create","kind":"observation",
  "id":"设备端稳定ID","data":{...}},{"seq":2,"op":"amend","kind":"observation",
  "entity_id":"...","data":{...},"expected_version":3},{"seq":3,"op":"action",
  "entity_id":"...","action":"submit","data":{...}}]}`。
  设备可用`X-Device-Id`请求头代替`device_id`。
- `GET /api/sync/<batch_id>`：返回`confirmed_seq`（已确认点）、`next_seq`和每条记录的终态结果。

续传与幂等保证：

- 批次、条目、结果和`confirmed_seq`全部落库。条目先落盘（spool）再处理，处理与检查点推进在同一事务内；
  中断/崩溃后重试从已确认点之后继续，已确认条目只回放存储结果，不会多出样本或审计记录。
- 重传时同一条目的载荷必须一致，否则返回`409`；编号必须从1开始且无空洞，否则`400`且不写入任何数据。
- 每条条目独立给出`applied`/`rejected`（规则不满足）/`conflict`（陈旧）状态，单条失败不阻断后续条目。
- 跨批次遇到相同稳定事件（`event_id`+`observed_at`或`sample_code`）：载荷相同则回放同一实体，
  载荷不同则作为冲突拒绝，不覆盖。

陈旧冲突与下游结论：

- 样本一旦出实验室结果（或聚集事件确认并入某观察），服务端会推进该观察的乐观版本。
  设备以旧版本修改地点/物种/时间时，返回`409 StaleConflictError`，并在`detail`中给出
  `offline_values`与`server_values`，旧值绝不覆盖下游结论。
- 以当前版本修改受保护字段时允许写入，但会使已得出的样本结果失效（样本退回`in_lab`，
  结果字段移除），并对包含该观察的已确认聚集事件按当前全部成员重算：仍满足则
  `recompute_cluster`更新质心，不满足则`invalidate_cluster`。旧结果、旧质心、旧成员全部留审计。
- 非受保护字段（如`note`）的修改不触发下游失效。

设备端示例：

```python
from src.device_client import DeviceQueue, BatchUploader

queue = DeviceQueue("./device.db", "device-42")
queue.create("observation", {"event_id": "E-1", "species": "deer", ...})  # 离线登记
uploader = BatchUploader(queue, "http://127.0.0.1:8305", "ranger-1", "field", "TRIP-1")
status = uploader.sync()  # 断网重试、重启后都会从 confirmed_seq 继续
```

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

离线同步基于批次、稳定事件编号与服务端持久检查点演示；设备端队列是标准库SQLite实现，
不包含真实野外通信协议（LoRa/卫星）、地图底图、完整空间索引或多设备对同一条目的自动合并。
