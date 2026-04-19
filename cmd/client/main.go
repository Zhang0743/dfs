package main

import (
    "context"
    "flag"
    "fmt"
    "io"
    "log"
    "os"
    "path/filepath"
    "sync"
    "time"

    pb "dfs-mini/internal/proto"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

var (
    trackerAddr = flag.String("tracker", "localhost:50050", "Tracker 地址")
)

func main() {
    flag.Parse()
    args := flag.Args()
    if len(args) < 1 {
        fmt.Println("用法: client <command> [options]")
        fmt.Println("命令: upload, download, ls")
        return
    }

    switch args[0] {
    case "upload":
        uploadCmd := flag.NewFlagSet("upload", flag.ExitOnError)
        filePath := uploadCmd.String("file", "", "文件路径")
        uploadCmd.Parse(args[1:])
        if *filePath == "" {
            log.Fatal("请指定文件路径: --file")
        }
        upload(*filePath)
    case "download":
        fmt.Println("下载功能待实现")
    case "ls":
        fmt.Println("文件列表功能待实现")
    default:
        fmt.Printf("未知命令: %s\n", args[0])
    }
}

func upload(filePath string) {
    // 连接 Tracker
    conn, err := grpc.Dial(*trackerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        log.Fatalf("连接 Tracker 失败: %v", err)
    }
    defer conn.Close()
    client := pb.NewTrackerClient(conn)

    // 打开文件
    file, err := os.Open(filePath)
    if err != nil {
        log.Fatalf("打开文件失败: %v", err)
    }
    defer file.Close()

    fileInfo, _ := file.Stat()
    fileSize := fileInfo.Size()
    fileName := filepath.Base(filePath)

    // 分片大小：4MB
    const chunkSize = 4 * 1024 * 1024
    chunkCount := int((fileSize + chunkSize - 1) / chunkSize)

    fmt.Printf("📤 开始上传文件: %s (大小: %.2f MB, 分片数: %d)\n", fileName, float64(fileSize)/(1024*1024), chunkCount)

    startTime := time.Now()

    // 1. 向 Tracker 请求分配存储节点（含副本）
    ctx := context.Background()
    resp, err := client.GetUploadNodes(ctx, &pb.GetUploadNodesRequest{
        FileId:       fileName,
        ChunkCount:   int32(chunkCount),
        ReplicaCount: 2, // 2 副本
    })
    if err != nil {
        log.Fatalf("获取上传节点失败: %v", err)
    }

    replicaCount := int(resp.ReplicaCount)
    if replicaCount == 0 {
        replicaCount = 1
    }

    // 2. 并发上传分片
    var wg sync.WaitGroup
    for i := 0; i < chunkCount; i++ {
        // 读取分片数据
        offset := int64(i) * chunkSize
        readSize := chunkSize
        if offset+chunkSize > fileSize {
            readSize = int(fileSize - offset)
        }
        buffer := make([]byte, readSize)
        _, err := file.ReadAt(buffer, offset)
        if err != nil && err != io.EOF {
            log.Fatalf("读取分片 %d 失败: %v", i, err)
        }

        // 该分片对应的副本节点
        startIdx := i * replicaCount
        endIdx := startIdx + replicaCount
        if endIdx > len(resp.Nodes) {
            endIdx = len(resp.Nodes)
        }
        chunkNodes := resp.Nodes[startIdx:endIdx]

        for _, node := range chunkNodes {
            wg.Add(1)
            go func(chunkIndex int, nodeAddr string, data []byte) {
                defer wg.Done()
                err := storeChunkToStorage(nodeAddr, fileName, chunkIndex, data)
                if err != nil {
                    log.Printf("❌ 上传分片 %d 到 %s 失败: %v", chunkIndex, nodeAddr, err)
                } else {
                    fmt.Printf("✅ 分片 %d 已上传到 %s\n", chunkIndex, nodeAddr)
                }
            }(i, node.Address, buffer)
        }
    }
    wg.Wait()

    elapsed := time.Since(startTime)
    throughput := float64(fileSize) / elapsed.Seconds() / (1024 * 1024) // MB/s

    fmt.Printf("\n🎉 文件上传完成！\n")
    fmt.Printf("   总耗时: %v\n", elapsed)
    fmt.Printf("   吞吐量: %.2f MB/s\n", throughput)
}

func startStorageServer(ctx context.Context, nodeId string, port int, dataDir string) {
    lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
    if err != nil {
        log.Fatalf("failed to listen: %v", err)
    }

    // 增加最大接收消息限制为 64MB
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