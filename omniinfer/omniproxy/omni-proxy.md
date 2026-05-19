
### radix-tree设计
# step1
sequence进入proxy后先进行tokenizer序列化计算拿到完整的token_ids序列，此时sequence转换为token_ids，根据配置的block_size逐个计算block hash，如：
block_size = 8
Hash算法 = XXHash64
H0 = hash(t0,....t7)
H1 = hash(H0, t8,....,t17) // attention计算中transformers的计算方式前向观察，下一个block hash以前一个block hash为基准
.....
Hn = hash(Hn-1, t8*n-8,...t8*n-1)
不满足block_size的尾块token_ids不满足匹配不参与hash计算

# step2
zeroMQ、nats（需另外配置部署nats-server）作为kvevent时间广播传递事件更新：
* BLOCK_STORED: 插入节点；
* BLOCK_REMOVED: LRU释放删除节点/整棵树；
* BLOCKS_CLEARED: 销毁整个Radix tree，如：推理框架vllm/sglang触发全量kvcache清空（gpu/npu device下线等）、rl权重重载、显存回收、worker心跳超时失联节点下线；

# step3
节点插入算法
1. 加锁ngx_shmtx_lock
2. current = root
3. 

读写锁设计

插入节点写操作 悲观锁 读写锁
删除节点/树操作 悲观锁 读写锁
hash匹配 悲观锁 读写锁
hash匹配 乐观锁 写锁

为什么要有两个匹配查询锁
写操作（插入/删除节点/树）为计算复杂操作，避免高并发场景锁被占用长时等待悲观锁；
在真是集群内部署场景中，基本为读多写少场景，所以读锁乐观锁避免推理请求长时等待；
参考raft核心的同步任期号设计，version号为omni-proxy内的任期，每次写操作自增；

运行逻辑如下
遍历写入/删除逻辑
lock --> 遍历找到位置/node --> 插入/删除 --> unlock

查询逻辑

乐观锁lock --> 遍历查找 --> 记录version id --> unlock -->
若此时有写操作，version号不同，则进入悲观锁重新查找重复上述逻辑。