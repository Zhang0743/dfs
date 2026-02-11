package tracker

import (
    "context"
    "sync"
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
}

// NewServer 创建Tracker服务器
func NewServer() *Server {
    return &Server{
        nodes:      make(map[string]*pb.NodeInfo),
        consistent: consistent.NewConsistent(100), // 100个虚拟节点
        fileMeta:   make(map[string]*pb.FileMetadata),
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
    }
    
    return &pb.HeartbeatResponse{Success: true}, nil
}

// GetUploadNodes 获取上传节点列表
func (s *Server) GetUploadNodes(ctx context.Context, req *pb.GetUploadNodesRequest) (*pb.GetUploadNodesResponse, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    var nodes []*pb.NodeInfo
    
    // 为每个分片分配存储节点
    for i := 0; i < int(req.ChunkCount); i++ {
        // 生成分片key: fileId_chunk_index
        key := req.FileId + "_chunk_" + string(rune(i))
        nodeId := s.consistent.GetNode(key)
        
        if node, ok := s.nodes[nodeId]; ok {
            nodes = append(nodes, node)
        } else {
            // 如果节点不存在，使用第一个可用节点
            for _, n := range s.nodes {
                nodes = append(nodes, n)
                break
            }
        }
    }
    
    return &pb.GetUploadNodesResponse{Nodes: nodes}, nil
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
