
# radix-tree设计
### step1
sequence进入proxy后先进行tokenizer序列化计算拿到完整的token_ids序列，此时sequence转换为token_ids，根据配置的block_size逐个计算block hash，如：
```
block_size = 8
Hash算法 = XXHash64
H0 = hash(t0,....t7)
H1 = hash(H0, t8,....,t17) // attention计算中transformers的计算方式前向观察，下一个block hash以前一个block hash为基准
.....
Hn = hash(Hn-1, t8*n-8,...t8*n-1)
不满足block_size的尾块token_ids不满足匹配不参与hash计算
```
### step2
zeroMQ、nats（需另外配置部署nats-server）作为kvevent时间广播传递事件更新：
* BLOCK_STORED: 插入节点；
* BLOCK_REMOVED: LRU释放删除节点/整棵树；
* BLOCKS_CLEARED: 销毁整个Radix tree，如：推理框架vllm/sglang触发全量kvcache清空（gpu/npu device下线等）、rl权重重载、显存回收、worker心跳超时失联节点下线；

### step3
节点插入算法
1. 加锁ngx_shmtx_lock
2. current = root
3. 

读写锁设计
```
插入节点写操作 悲观锁 读写锁
删除节点/树操作 悲观锁 读写锁
hash匹配 悲观锁 读写锁
hash匹配 乐观锁 写锁
```
为什么要有两个匹配查询锁？

写操作（插入/删除节点/树）为计算复杂操作，避免高并发场景锁被占用长时等待悲观锁；
在真是集群内部署场景中，基本为读多写少场景，所以读锁乐观锁避免推理请求长时等待；
参考raft核心的同步任期号设计，version号为omni-proxy内的任期，每次写操作自增；

运行逻辑如下：

* 写-写之间：竞争同一把 mutex，先到先得，串行执行，不存在持有多把锁的情况
* 读-写之间：读线程不请求 mutex，不存在"读线程持有锁 A 等待锁 B"的情况
* 读-读之间：都无锁，无竞争

遍历写入/删除逻辑:
线程 A (写): lock(mutex) → 修改树 → version++ → unlock(mutex) 线程 B (写): lock(mutex) → 修改树 → version++ → unlock(mutex) 线程 C (读): 读 version → 无锁遍历 → 再读 version → 一致则返回

遍历查询逻辑：


* 场景 1：遍历期间无写入。读线程: 读 version=5 → 遍历 → 读 version=5 → 一致 → 返回结果 ✓


* 场景 2：遍历期间有写入。写线程: lock → 修改树 → version=6 → unlock 读线程: 读 version=5 → 遍历(读到部分旧数据+部分新数据) → 读 version=6 → 不一致 → 重试

读线程不会返回不一致的中间状态，而是重试直到遍历期间没有任何写入。

* 场景 3：遍历完成后才有写入。读线程: 读 version=5 → 遍历 → 读 version=5 → 一致 → 返回结果 ✓ 写线程: lock → 修改 → version=6 → unlock

读线程基于 version=5 的快照返回了正确结果，写线程之后的修改不影响已返回的结果。

# KV-AWARE-ROUTING设计

目前业界主流推理请求调度除了针对prefix-cache做radix-tree cache外，还会综合考虑decode的整体负载情况，避免局部过载；针对decode节点的负载监控，当前引入etcd的event trigger informer机制。prefill/decode worker定时上报当前负载心跳由proxy侦听，此时的负载路由设计打分机制：
```
type WokerLoadCfg struct {
    request_waiting 
    resouse_usage
    prefix_cache_hit_rate
    request_slots
    kv_active_blocks
    kv_block_nums
    ....
}
```
* proxy_worker_forward = rating(radix-cache-max-depth, WokerLoadCfg)

prefill/decode worker拉起后连接etcd成功上报开始心跳，写入worker event：

```
/instances/omniinfer/backend/prefill/generate/{name}_{role}_{rank}_{aarch} #worker 1
/instances/omniinfer/backend/decode/generate/{name}_{role}_{rank}_{aarch} #worker 1

```
name: cli/assemble 脚本中写的worker容器名称
role：kv_consumer/kv_producer, prefill/decode等同的意思，即pd分离场景的职责
rank: node rank
aarch：节点device架构，atlas3（a3/910c）a5（950）

worker active → register put etcd → omni-proxy get update aware routing → update rating map

prompt sequence → tokenizer → prefix cache maping → rating → routing 

注：打分的权重可以根据etcd event动态配置，根据当前集群的planner吞吐和负载分析可从管理面动态修改权重占比，实现proxy修改打分权重热加载。

# Backend统一封装Omni-Worker实现
以sglang举例
```
import sglang
engine = sglang.Engine(server_args)
handler = DecodeWorkerHandler(
    engine, omni_config, publisher, generate_endpoint, shutdown_event
    )
hander.register_route(omni_runtime) # omni_runtime即为优先拉起的omniinferruntime
```
优化痛点，结合特性设计：https://gitee.com/SiuhungLai/sglang/wikis/sglang%20multi-apiserver%20%E5%A4%A7EP%E6%96%B9%E6%A1%88%E5%88%86%E6%9E%90

sglang原生框架会在prefill和decode侧同时做一次针对相同sequence请求的tokenizer，在该大ep部署方案的场景下，以prefill device_num : decode device_num = 2:1的部署规格（昇腾a3大ep超节点方案）即576：288，tokenizer数量比例达到36：1，decode侧打满batch_size比prefill长导致整体吞吐会受影响，最开始采用的优化方案是多apiserver的侵入式修改部署形式，通过修改开始dp_attention是data_parallel_process的启动设置，将attn_tp_rank用原本逻辑的tp_rank填充确保每个节点都启动tokenizer暴露api最终做到server的比例为2：1来做到优化吞吐。

但该方案的弊端也很明显，侵入式修改无法跟进sglang主线的更新，但从Huawei内部的更新团队就有海思和昇腾计算双线双维度的更新适配。每次同步主线更新困难重重，必须重新设计方案，故有此设计。

sglang的Engine和vllm的AsyncLLM直接进行Engine引擎的桥接，针对moe/layer层等omniinfer的很多优化采用monkey patch和子类化工厂注册的方式插入更新。
```
#vllm
engine_client = AsyncLLM.from_vllm_config(
    vllm_config=vllm_config,
    usage_context=usage_context,
    start_logger=factory,
    ...
)

#sglang上述已列出
```
将tokenizer prompt转ids统一放在proxy完成，采用monkey patch的修改各自engine的generate针对请求跳过词表转换，同时也支持omni-proxy动态热加载配置不同的tokenizer.json和vocab.json。


以sglang举例，在scheduler内部开有metrics_ipc_name专用的上报metrics通路，该消息在sglang内部并没有被消费，通过封装SglnagPublishe/VllmPublisher指定PULL该zmq socket管道内消息，从而获取该进程scheduler的kv metrics event

```
kv_metrics = KvMetrics()
...
if not self.send_metrics_from_scheduler.closed:
    self.send_metrics_from_scheduler.send_pyobj(kv_metrics)
```

