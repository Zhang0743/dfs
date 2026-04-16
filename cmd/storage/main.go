package main

import (
    "context"
    "flag"
    "fmt"
    "log"
    "net"
    "os"
    "os/signal"
    "path/filepath"
    "syscall"
    "time"

    pb "dfs-mini/internal/proto"
    "dfs-mini/internal/storage"
    "dfs-mini/internal/common"

    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

func main() {
    // 命令行参数（优先级高于配置文件）
    nodeId := flag.String("node-id", "", "storage node id (required)")
    port := flag.Int("port", 0, "storage service port")
    dataDir := flag.String("data", "", "data directory")
    trackerAddr := flag.String("tracker", "", "tracker address")
    configPath := flag.String("config", "configs/storage.yaml", "配置文件路径")
    flag.Parse()

    if *nodeId == "" {
        log.Fatal("❌ -node-id is required")
    }

    // 加载配置文件
    cfg, err := common.LoadStorageConfig(*configPath)
    if err != nil {
        log.Printf("警告: 无法加载配置文件，使用默认配置: %v", err)
        cfg = &common.StorageConfig{}
        cfg.Storage.DataDir = "./data"
        cfg.Storage.MaxDiskUsage = 0.8
        cfg.Heartbeat.Interval = 5
    }

    // 命令行参数覆盖配置文件
    finalPort := cfg.Server.Port
    if *port != 0 {
        finalPort = *port
    }
    finalDataDir := cfg.Storage.DataDir
    if *dataDir != "" {
        finalDataDir = *dataDir
    }
    finalTrackerAddr := cfg.Server.TrackerAddress
    if *trackerAddr != "" {
        finalTrackerAddr = *trackerAddr
    }

    // 1. 启动 Storage gRPC 服务
    go startStorageServer(*nodeId, finalPort, finalDataDir)

    // 2. 向 Tracker 注册并发送心跳
    registerAndHeartbeat(*nodeId, finalPort, finalTrackerAddr, finalDataDir, cfg.Heartbeat.Interval)
}

func startStorageServer(nodeId string, port int, dataDir string) {
    lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
    if err != nil {
        log.Fatalf("failed to listen: %v", err)
    }

    s := grpc.NewServer()
    storageServer := storage.NewServer(nodeId, dataDir)
    pb.RegisterStorageServer(s, storageServer)

    log.Printf("🚀 Storage node %s started on :%d", nodeId, port)
    log.Printf("📁 Data directory: %s", dataDir)

    // 优雅关闭
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    
    go func() {
        if err := s.Serve(lis); err != nil {
            log.Fatalf("failed to serve: %v", err)
        }
    }()
    
    <-quit
    log.Println("🛑 Shutting down storage server...")
    s.GracefulStop()
}

func registerAndHeartbeat(nodeId string, port int, trackerAddr, dataDir string, heartbeatInterval int) {
    // 等待 tracker 启动
    time.Sleep(2 * time.Second)
    
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
        Address:        fmt.Sprintf("localhost:%d", port),
        AvailableSpace: getAvailableSpace(dataDir),
    })
    if err != nil {
        log.Fatalf("register failed: %v", err)
    }
    log.Printf("✅ Registered to tracker: %v", resp.Success)

    // 定期心跳
    ticker := time.NewTicker(time.Duration(heartbeatInterval) * time.Second)
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
            log.Printf("💓 Heartbeat sent - node: %s, chunks: %d", nodeId, getChunkCount(dataDir))
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
