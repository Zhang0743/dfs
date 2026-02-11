package main

import (
    "context"
    "flag"
    "fmt"
    "io"
    "log"
    "os"
    "path/filepath"
    "time"
    
    pb "dfs-mini/internal/proto"
    
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

const (
    chunkSize = 4 * 1024 * 1024 // 4MB
)

var (
    trackerAddr = flag.String("tracker", "localhost:50051", "tracker address")
    filePath    = flag.String("file", "", "file to upload")
    fileId      = flag.String("file-id", "", "file id to download")
    outputPath  = flag.String("output", "", "output path for download")
)

func main() {
    flag.Parse()
    
    if *filePath != "" {
        uploadFile()
    } else if *fileId != "" && *outputPath != "" {
        downloadFile()
    } else {
        log.Fatal("❌ 请指定 -file 上传 或 -file-id/-output 下载")
    }
}

func uploadFile() {
    file, err := os.Open(*filePath)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()
    
    fileInfo, err := file.Stat()
    if err != nil {
        log.Fatal(err)
    }
    
    fileName := filepath.Base(*filePath)
    fileId := fmt.Sprintf("%s-%d", fileName, time.Now().UnixNano())
    chunkCount := (fileInfo.Size() + chunkSize - 1) / chunkSize
    
    fmt.Printf("📁 上传文件: %s (%.2f MB)\n", fileName, float64(fileInfo.Size())/1024/1024)
    fmt.Printf("📦 分片数量: %d\n", chunkCount)
    
    conn, err := grpc.Dial(*trackerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()
    
    trackerClient := pb.NewTrackerClient(conn)
    ctx := context.Background()
    
    nodesResp, err := trackerClient.GetUploadNodes(ctx, &pb.GetUploadNodesRequest{
        FileId:     fileId,
        ChunkCount: int32(chunkCount),
    })
    if err != nil {
        log.Fatal(err)
    }
    
    for i := int32(0); i < int32(chunkCount); i++ {
        offset := int64(i) * chunkSize
        buffer := make([]byte, chunkSize)
        n, err := file.ReadAt(buffer, offset)
        if err != nil && err != io.EOF {
            log.Printf("❌ 读取分片 %d 失败: %v", i, err)
            continue
        }
        
        node := nodesResp.Nodes[i%int32(len(nodesResp.Nodes))]
        chunkId := fmt.Sprintf("%s_%d", fileId, i)
        
        storageConn, err := grpc.Dial(node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
        if err != nil {
            log.Printf("❌ 连接节点 %s 失败: %v", node.NodeId, err)
            continue
        }
        
        storageClient := pb.NewStorageClient(storageConn)
        stream, err := storageClient.StoreChunk(ctx)
        if err != nil {
            log.Printf("❌ 创建流失败: %v", err)
            storageConn.Close()
            continue
        }
        
        err = stream.Send(&pb.ChunkData{
            ChunkId: chunkId,
            Data:    buffer[:n],
            Index:   i,
        })
        if err != nil {
            log.Printf("❌ 发送分片 %d 失败: %v", i, err)
            storageConn.Close()
            continue
        }
        
        resp, err := stream.CloseAndRecv()
        if err != nil {
            log.Printf("❌ 接收响应失败: %v", err)
            storageConn.Close()
            continue
        }
        
        if resp.Success {
            fmt.Printf("✅ 分片 %d/%d 上传成功 -> %s (校验: %s)\n", 
                i+1, chunkCount, node.NodeId, resp.Checksum[:8])
        }
        
        storageConn.Close()
    }
    
    fmt.Printf("\n🎉 文件上传成功！\n")
    fmt.Printf("📎 文件ID: %s\n", fileId)
}

func downloadFile() {
    fmt.Printf("📁 下载文件: %s\n", *fileId)
    fmt.Printf("📂 保存路径: %s\n", *outputPath)
    
    // 直接使用传入的fileId作为分片ID
    chunkId := *fileId
    
    // 连接 Tracker 获取节点列表
    conn, err := grpc.Dial(*trackerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()
    
    trackerClient := pb.NewTrackerClient(conn)
    ctx := context.Background()
    
    nodesResp, err := trackerClient.ListNodes(ctx, &pb.ListNodesRequest{})
    if err != nil {
        log.Fatal(err)
    }
    
    if len(nodesResp.Nodes) == 0 {
        log.Fatal("❌ 没有可用存储节点")
    }
    
    fmt.Printf("🔍 正在查找分片: %s\n", chunkId)
    
    // 轮询所有节点查找分片
    var data []byte
    var found bool
    
    for _, node := range nodesResp.Nodes {
        storageConn, err := grpc.Dial(node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
        if err != nil {
            continue
        }
        
        storageClient := pb.NewStorageClient(storageConn)
        chunk, err := storageClient.RetrieveChunk(ctx, &pb.ChunkRequest{
            ChunkId: chunkId,
            Index:   0,
        })
        
        if err == nil && chunk != nil {
            data = chunk.Data
            found = true
            fmt.Printf("✅ 找到分片在节点: %s\n", node.NodeId)
            storageConn.Close()
            break
        }
        storageConn.Close()
    }
    
    if !found {
        log.Fatal("❌ 未找到文件分片")
    }
    
    // 写入文件
    err = os.WriteFile(*outputPath, data, 0644)
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("✅ 文件下载成功！大小: %d 字节\n", len(data))
}
