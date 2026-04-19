package tracker

import (
    "context"
    "fmt"      // 添加这一行
    "sync"
    "time"
    
    pb "dfs-mini/internal/proto"
    "dfs-mini/pkg/consistent"
)

// Server Tracker服务器
type Server struct {
    pb.UnimplementedTrackerServer
    mu         sync.RWMutex
    nodes      map[string]*pb.NodeInfo      // 节点ID -> 节点信息
    consistent *consistent.Consistent       // 一致性哈希环
    fileMeta   map[string]*pb.FileMetadata  // 文件ID -> 元数据
    ctx        context.Context              // 用于优雅关闭
    cancel     context.CancelFunc           // 取消函数
}

// NewServer 创建Tracker服务器
func NewServer() *Server {
    ctx, cancel := context.WithCancel(context.Background())
    s := &Server{
        nodes:      make(map[string]*pb.NodeInfo),
        consistent: consistent.NewConsistent(100), // 100个虚拟节点
        fileMeta:   make(map[string]*pb.FileMetadata),
        ctx:        ctx,
        cancel:     cancel,
    }
    
    // 启动后台 goroutine 清理死节点
    go s.cleanupDeadNodes()
    
    return s
}

// Stop 停止服务器，清理后台任务
func (s *Server) Stop() {
    s.cancel()
}

// cleanupDeadNodes 定期清理心跳超时的节点
func (s *Server) cleanupDeadNodes() {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            s.mu.Lock()
            now := time.Now()
            for id, node := range s.nodes {
                // 超过15秒未收到心跳则剔除
                if now.Sub(time.Unix(node.LastHeartbeat, 0)) > 15*time.Second {
                    delete(s.nodes, id)
                    s.consistent.RemoveNode(id)
                    // 注意：这里用 println 因为 goroutine 里不能用 log 的并发安全
                    println("Node removed due to heartbeat timeout:", id)
                }
            }
            s.mu.Unlock()
        case <-s.ctx.Done():
            return
        }
    }
}

// RegisterNode 节点注册
func (s *Server) RegisterNode(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    // 存储节点信息
    s.nodes[req.NodeId] = &pb.NodeInfo{
        NodeId:         req.NodeId,
        Address:        req.Address,
        Status:         "active",
        AvailableSpace: req.AvailableSpace,
        LastHeartbeat:  time.Now().Unix(),
    }

    // 加入一致性哈希环
    s.consistent.AddNode(req.NodeId)

    return &pb.RegisterResponse{
        Success: true,
        Message: "node registered successfully",
    }, nil
}

// Heartbeat 心跳上报
func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    if node, ok := s.nodes[req.NodeId]; ok {
        node.Status = "active"
        node.AvailableSpace = req.AvailableSpace
        node.LastHeartbeat = time.Now().Unix()
        return &pb.HeartbeatResponse{Success: true}, nil
    }

    return &pb.HeartbeatResponse{Success: false}, nil
}

// GetUploadNodes 获取上传节点列表
// GetUploadNodes 获取上传节点列表（支持多副本和调度策略）
func (s *Server) GetUploadNodes(ctx context.Context, req *pb.GetUploadNodesRequest) (*pb.GetUploadNodesResponse, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    replicaCount := int(req.ReplicaCount)
    if replicaCount <= 0 {
        replicaCount = 2 // 默认2副本
    }

    var nodes []*pb.NodeInfo

    // 为每个分片分配存储节点
    for i := 0; i < int(req.ChunkCount); i++ {
        key := fmt.Sprintf("%s_chunk_%d", req.FileId, i)
        
        // 获取多个不重复的节点（副本）
        replicaNodes := s.consistent.GetReplicas(key, replicaCount)
        
        for _, nodeId := range replicaNodes {
            if node, ok := s.nodes[nodeId]; ok && node.Status == "active" {
                nodes = append(nodes, node)
            } else {
                // 节点不可用时的fallback：选可用空间最大的健康节点
                fallback := s.selectBestNode()
                if fallback != nil {
                    nodes = append(nodes, fallback)
                }
            }
        }
    }

    return &pb.GetUploadNodesResponse{Nodes: nodes, ReplicaCount: int32(replicaCount)}, nil
}

// selectBestNode 选择可用空间最大的健康节点（调度策略）
func (s *Server) selectBestNode() *pb.NodeInfo {
    var bestNode *pb.NodeInfo
    var maxSpace int64 = -1
    
    for _, node := range s.nodes {
        if node.Status == "active" && node.AvailableSpace > maxSpace {
            maxSpace = node.AvailableSpace
            bestNode = node
        }
    }
    return bestNode
}

// GetNode 获取单个节点信息
func (s *Server) GetNode(ctx context.Context, req *pb.GetNodeRequest) (*pb.GetNodeResponse, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    node, ok := s.nodes[req.NodeId]
    if !ok {
        return &pb.GetNodeResponse{Node: nil}, nil
    }

    return &pb.GetNodeResponse{Node: node}, nil
}

// ListNodes 列出所有节点
func (s *Server) ListNodes(ctx context.Context, req *pb.ListNodesRequest) (*pb.ListNodesResponse, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    nodes := make([]*pb.NodeInfo, 0, len(s.nodes))
    for _, node := range s.nodes {
        nodes = append(nodes, node)
    }

    return &pb.ListNodesResponse{Nodes: nodes}, nil
}