package main

import (
    "flag"
    "fmt"
    "log"
    "net"
    "os"
    "os/signal"
    "syscall"

    pb "dfs-mini/internal/proto"
    "dfs-mini/internal/tracker"
    "dfs-mini/internal/common"

    "google.golang.org/grpc"
)

func main() {
    configPath := flag.String("config", "configs/tracker.yaml", "配置文件路径")
    flag.Parse()

    // 加载配置
    cfg, err := common.LoadTrackerConfig(*configPath)
    if err != nil {
        log.Printf("警告: 无法加载配置文件 %s，使用默认配置: %v", *configPath, err)
        cfg = &common.TrackerConfig{
            Server:    common.ServerConfig{Host: "0.0.0.0", Port: 50050},
            Heartbeat: common.HeartbeatConfig{Interval: 5, Timeout: 15},
            Chunk:     common.ChunkConfig{Size: 64 * 1024 * 1024},
        }
    }

    // 监听端口
    addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
    lis, err := net.Listen("tcp", addr)
    if err != nil {
        log.Fatalf("failed to listen: %v", err)
    }

    // 创建 gRPC 服务器
    s := grpc.NewServer()

    // 注册 Tracker 服务
    trackerServer := tracker.NewServer()
    pb.RegisterTrackerServer(s, trackerServer)

    log.Printf("🚀 Tracker server started on %s", addr)
    log.Printf("📋 Config: heartbeat_interval=%ds, timeout=%ds, chunk_size=%dMB",
        cfg.Heartbeat.Interval, cfg.Heartbeat.Timeout, cfg.Chunk.Size/(1024*1024))

    // 优雅关闭
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

    go func() {
        if err := s.Serve(lis); err != nil {
            log.Fatalf("failed to serve: %v", err)
        }
    }()

    <-quit
    log.Println("🛑 Shutting down gracefully...")
    trackerServer.Stop()
    s.GracefulStop()
    log.Println("✅ Server stopped")
}