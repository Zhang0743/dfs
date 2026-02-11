package main

import (
    "context"
    "flag"
    "log"
    "net"
    "path/filepath"
    "time"
    
    pb "dfs-mini/internal/proto"
    "dfs-mini/internal/storage"
    
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

var (
    nodeId      = flag.String("node-id", "", "storage node id (required)")
    port        = flag.String("port", "50052", "storage service port")
    dataDir     = flag.String("data", "./data", "data directory")
    trackerAddr = flag.String("tracker", "localhost:50051", "tracker address")
)

func main() {
    flag.Parse()
    if *nodeId == "" {
        log.Fatal("❌ -node-id is required")
    }
    
    // 1. 启动 Storage gRPC 服务
    go startStorageServer(*nodeId, *port, *dataDir)
    
    // 2. 向 Tracker 注册并发送心跳
    registerAndHeartbeat(*nodeId, *port, *trackerAddr, *dataDir)
}

func startStorageServer(nodeId, port, dataDir string) {
    lis, err := net.Listen("tcp", ":"+port)
    if err != nil {
        log.Fatalf("failed to listen: %v", err)
    }
    
    s := grpc.NewServer()
    storageServer := storage.NewServer(nodeId, dataDir)
    pb.RegisterStorageServer(s, storageServer)
    
    log.Printf("🚀 Storage node %s started on :%s", nodeId, port)
    log.Printf("📁 Data directory: %s", dataDir)
    
    if err := s.Serve(lis); err != nil {
        log.Fatalf("failed to serve: %v", err)
    }
}

func registerAndHeartbeat(nodeId, port, trackerAddr, dataDir string) {
    // 连接 Tracker
    conn, err := grpc.Dial(trackerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        log.Fatalf("failed to connect tracker: %v", err)
    }
    defer conn.Close()
    
    client := pb.NewTrackerClient(conn)
    ctx := context.Background()
    
    // 首次注册
    resp, err := client.RegisterNode(ctx, &pb.RegisterRequest{
        NodeId:         nodeId,
        Address:        getLocalAddress() + ":" + port,
        AvailableSpace: getAvailableSpace(dataDir),
    })
    if err != nil {
        log.Fatalf("register failed: %v", err)
    }
    log.Printf("✅ Registered to tracker: %v", resp.Success)
    
    // 定期心跳
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()
    
    for range ticker.C {
        heartbeat, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{
            NodeId:         nodeId,
            AvailableSpace: getAvailableSpace(dataDir),
            ChunkCount:     getChunkCount(dataDir),
        })
        if err != nil {
            log.Printf("❌ Heartbeat failed: %v", err)
            continue
        }
        if heartbeat.Success {
            log.Printf("💓 Heartbeat sent - node: %s", nodeId)
        }
    }
}

func getLocalAddress() string {
    return "localhost"
}

func getAvailableSpace(path string) int64 {
    // 简化实现，返回1GB
    return 1 * 1024 * 1024 * 1024
}

func getChunkCount(path string) int32 {
    files, err := filepath.Glob(filepath.Join(path, "*"))
    if err != nil {
        return 0
    }
    return int32(len(files))
}
