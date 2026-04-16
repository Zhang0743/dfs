package main

import (
    "flag"
    "fmt"
    "log"
    "os"
)

func main() {
    // 子命令解析
    uploadCmd := flag.NewFlagSet("upload", flag.ExitOnError)
    downloadCmd := flag.NewFlagSet("download", flag.ExitOnError)
    lsCmd := flag.NewFlagSet("ls", flag.ExitOnError)

    // 上传命令参数
    uploadFile := uploadCmd.String("file", "", "文件路径")

    // 下载命令参数
    downloadFile := downloadCmd.String("file", "", "文件名")
    outputPath := downloadCmd.String("output", "", "输出路径")

    if len(os.Args) < 2 {
        fmt.Println("用法: client <command> [options]")
        fmt.Println("命令: upload, download, ls")
        return
    }

    switch os.Args[1] {
    case "upload":
        uploadCmd.Parse(os.Args[2:])
        if *uploadFile == "" {
            log.Fatal("请指定文件路径: --file")
        }
        fmt.Printf("上传文件: %s\n", *uploadFile)
        // TODO: 实现上传逻辑，调用 Tracker gRPC 服务
    case "download":
        downloadCmd.Parse(os.Args[2:])
        if *downloadFile == "" || *outputPath == "" {
            log.Fatal("请指定文件名和输出路径: --file --output")
        }
        fmt.Printf("下载文件: %s -> %s\n", *downloadFile, *outputPath)
        // TODO: 实现下载逻辑
    case "ls":
        lsCmd.Parse(os.Args[2:])
        fmt.Println("文件列表:")
        // TODO: 调用 Tracker 获取文件列表
    default:
        fmt.Printf("未知命令: %s\n", os.Args[1])
    }
}
