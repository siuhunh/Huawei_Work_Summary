#### omni-env

sandbox 3层网络架构模型
- 北向流量：sandbox内sdk与管控面通信（创建沙箱、续约、hmac-sha256路由签名）
- 南向流量：管控面向Worker集群内的sandbox agent发起execute请求（/chat/completions）
- 网管层：OpenResty Gateway（北向）+ 集群Ingress（南向）分开处理


```mermaid
sequenceDiagram
    participant A as SDK Client
    participant B as ControlPlane (Port 8081)
    participant C as Cluster Ingress (Port 8082)
    participant D as SandBox Pod Agent (Port 9001)
    participant E as SandBox Runtime (Port 17901)
    
    A->>B: X-API-KEY
    B-->>C: 签名路由<br/>HMAC验证+待机检查(Generation Check)
    C-->>D: 
    D-->>E: Execute invoke /chat/completions
    B->>A: Signed Route已签名路由(HMAC-SHA256)

```

### 创建沙箱实例流程图
```mermaid
graph TD
    A[SDK] -->|POST: /api/v1/sandboxes <br> Header: X-API-KEY| B[OpenResty Gateway]
    B -->|哈希一致性验证 <br> 选定Control Plane实例| C[Control Plane]
    C --> |1. create_sandbox <br> 2. create signed_route <br> 3. 调用k8s接口拉起pod,等待STATUS=READY <br> 4. return Signed_route |C
    C --> |记录sandbox_id, signed_route, lease_ttl| A


```

### websocket调用执行

```mermaid
graph TD
    A[SDK] -->|POST: /exec <br> Header: X-Sandbox-Signed-Route| B[Cluster Ingress/Omni-proxy :8082]
    B -->|LUA验证: <br> 1.HMAC-SHA256签名验证 <br> 2.路由过期检查 <br> 3.路由重放检查 <br> 4. 路由到指定Pod IP| C[Sandbox-Agent :9001, sidecar容器]
    C --> |1. create_sandbox <br> 2. create signed_route <br> 3. 调用k8s接口拉起pod,等待STATUS=READY <br> 4. return Signed_route |D[Runtime-Agent :17901, runtime容器]
    D --> |原路返回|C --> B --> A


```

SDK(外部、发起调用方) --> Control Plane (管控面、集群管控) --> SandBox Pod(k8s Pod资源类型，sidecar容器运行agent，runtime容器运行推理服务)

TTL 签名续签，检测