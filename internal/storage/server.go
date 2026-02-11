package storage

import (
    "context"
    "io"
    "os"
    "path/filepath"
    "crypto/md5"
    "encoding/hex"
    
    pb "dfs-mini/internal/proto"
)

// Server 存储节点服务器
type Server struct {
    pb.UnimplementedStorageServer
    nodeId   string
    dataDir  string
}

// NewServer 创建存储节点
func NewServer(nodeId, dataDir string) *Server {
    // 确保数据目录存在
    os.MkdirAll(dataDir, 0755)
    
    return &Server{
        nodeId:  nodeId,
        dataDir: dataDir,
    }
}

// StoreChunk 存储文件分片（流式接收）
func (s *Server) StoreChunk(stream pb.Storage_StoreChunkServer) error {
    var chunkId string
    var file *os.File
    hasher := md5.New()
    
    for {
        chunk, err := stream.Recv()
        if err == io.EOF {
            break
        }
        if err != nil {
            return err
        }
        
        // 第一个分片，创建文件
        if chunkId == "" {
            chunkId = chunk.ChunkId
            chunkPath := filepath.Join(s.dataDir, chunkId)
            file, err = os.Create(chunkPath)
            if err != nil {
                return err
            }
            defer file.Close()
        }
        
        // 写入数据
        if _, err := file.Write(chunk.Data); err != nil {
            return err
        }
        
        // 计算MD5用于校验
        hasher.Write(chunk.Data)
    }
    
    // 计算校验和
    checksum := hex.EncodeToString(hasher.Sum(nil))
    
    // 返回存储结果
    return stream.SendAndClose(&pb.StoreResponse{
        Success:  true,
        Checksum: checksum,
    })
}

// RetrieveChunk 读取文件分片
func (s *Server) RetrieveChunk(ctx context.Context, req *pb.ChunkRequest) (*pb.ChunkData, error) {
    chunkPath := filepath.Join(s.dataDir, req.ChunkId)
    
    // 读取整个文件
    data, err := os.ReadFile(chunkPath)
    if err != nil {
        return nil, err
    }
    
    return &pb.ChunkData{
        ChunkId: req.ChunkId,
        Data:    data,
        Index:   req.Index,
    }, nil
}
