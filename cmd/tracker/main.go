package main

import (
    "log"
    "net"
    
    pb "dfs-mini/internal/proto"
    "dfs-mini/internal/tracker"
    
    "google.golang.org/grpc"
)

func main() {
    // 监听端口
    lis, err := net.Listen("tcp", ":50051")
    if err != nil {
        log.Fatalf("failed to listen: %v", err)
    }
    
    // 创建 gRPC 服务器
    s := grpc.NewServer()
    
    // 注册 Tracker 服务
    trackerServer := tracker.NewServer()
    pb.RegisterTrackerServer(s, trackerServer)
    
    log.Println("🚀 Tracker server started on :50051")
    log.Println("✅ Registered services: Tracker")
    
    if err := s.Serve(lis); err != nil {
        log.Fatalf("failed to serve: %v", err)
    }
}
