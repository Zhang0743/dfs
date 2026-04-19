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
    nodeId := flag.String("node-id", "", "storage node id (required)")
    port := flag.Int("port", 0, "storage service port")
    dataDir := flag.String("data", "", "data directory")
    trackerAddr := flag.String("tracker", "", "tracker address")
    configPath := flag.String("config", "configs/storage.yaml", "配置文件路径")
    flag.Parse()

    if *nodeId == "" {
        log.Fatal("-node-id is required")
    }

    cfg, err := common.LoadStorageConfig(*configPath)
    if err != nil {
        log.Printf("警告: 无法加载配置文件，使用默认配置: %v", err)
        cfg = &common.StorageConfig{}
        cfg.Storage.DataDir = "./data"
        cfg.Storage.MaxDiskUsage = 0.8
        cfg.Heartbeat.Interval = 5
    }

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

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    go startStorageServer(ctx, *nodeId, finalPort, finalDataDir)
    go registerAndHeartbeat(ctx, *nodeId, finalPort, finalTrackerAddr, finalDataDir, cfg.Heartbeat.Interval)

    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    <-quit

    log.Println("🛑 Shutting down gracefully...")
    cancel()
    time.Sleep(2 * time.Second)
    log.Println("✅ Storage node stopped")
}

func startStorageServer(ctx context.Context, nodeId string, port int, dataDir string) {
    lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
    if err != nil {
        log.Fatalf("failed to listen: %v", err)
    }

    // ✅ 设置最大接收消息大小为 64MB
    s := grpc.NewServer(
        grpc.MaxRecvMsgSize(64*1024*1024),
    )
    storageServer := storage.NewServer(nodeId, dataDir)
    pb.RegisterStorageServer(s, storageServer)

    log.Printf("🚀 Storage node %s started on :%d", nodeId, port)
    log.Printf("📁 Data directory: %s", dataDir)

    go func() {
        <-ctx.Done()
        log.Println("🛑 Shutting down gRPC server...")
        s.GracefulStop()
    }()

    if err := s.Serve(lis); err != nil {
        log.Fatalf("failed to serve: %v", err)
    }
}

func registerAndHeartbeat(ctx context.Context, nodeId string, port int, trackerAddr, dataDir string, heartbeatInterval int) {
    time.Sleep(2 * time.Second)

    // 使用带超时的 DialContext
    dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
    conn, err := grpc.DialContext(dialCtx, trackerAddr,
        grpc.WithTransportCredentials(insecure.NewCredentials()),
        grpc.WithBlock())
    cancel()

    if err != nil {
        log.Printf("Failed to connect tracker: %v", err)
        return
    }
    defer conn.Close()

    client := pb.NewTrackerClient(conn)

    // 首次注册（带超时）
    regCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
    resp, err := client.RegisterNode(regCtx, &pb.RegisterRequest{
        NodeId:         nodeId,
        Address:        fmt.Sprintf("localhost:%d", port),
        AvailableSpace: getAvailableSpace(dataDir),
    })
    cancel()

    if err != nil || !resp.Success {
        log.Printf("Register failed: %v", err)
        return
    }
    log.Printf("✅ Registered to tracker: %v", resp.Success)

    // 心跳循环
    ticker := time.NewTicker(time.Duration(heartbeatInterval) * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            log.Println("💔 Heartbeat stopped")
            return
        case <-ticker.C:
            hbCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
            heartbeat, err := client.Heartbeat(hbCtx, &pb.HeartbeatRequest{
                NodeId:         nodeId,
                AvailableSpace: getAvailableSpace(dataDir),
                ChunkCount:     getChunkCount(dataDir),
            })
            cancel()

            if err != nil {
                log.Printf("❌ Heartbeat failed: %v", err)
                continue
            }
            if heartbeat.Success {
                log.Printf("💓 Heartbeat sent - node: %s, chunks: %d", nodeId, getChunkCount(dataDir))
            }
        }
    }
}

func getAvailableSpace(path string) int64 {
    return 1 * 1024 * 1024 * 1024
}

func getChunkCount(path string) int32 {
    files, err := filepath.Glob(filepath.Join(path, "*"))
    if err != nil {
        return 0
    }
    return int32(len(files))
}