package common

import (
    "github.com/spf13/viper"
)

type ServerConfig struct {
    Host string `mapstructure:"host"`
    Port int    `mapstructure:"port"`
}

type HeartbeatConfig struct {
    Interval int `mapstructure:"interval"`
    Timeout  int `mapstructure:"timeout"`
}

type ChunkConfig struct {
    Size int64 `mapstructure:"size"`
}

type TrackerConfig struct {
    Server    ServerConfig    `mapstructure:"server"`
    Heartbeat HeartbeatConfig `mapstructure:"heartbeat"`
    Chunk     ChunkConfig     `mapstructure:"chunk"`
}

type StorageConfig struct {
    Server struct {
        Port           int    `mapstructure:"port"`
        TrackerAddress string `mapstructure:"tracker_address"`
    } `mapstructure:"server"`
    Storage struct {
        DataDir      string  `mapstructure:"data_dir"`
        MaxDiskUsage float64 `mapstructure:"max_disk_usage"`
    } `mapstructure:"storage"`
    Heartbeat struct {
        Interval int `mapstructure:"interval"`
    } `mapstructure:"heartbeat"`
}

func LoadTrackerConfig(path string) (*TrackerConfig, error) {
    viper.SetConfigFile(path)
    viper.SetConfigType("yaml")
    
    if err := viper.ReadInConfig(); err != nil {
        return nil, err
    }
    
    var cfg TrackerConfig
    if err := viper.Unmarshal(&cfg); err != nil {
        return nil, err
    }
    
    if cfg.Server.Host == "" {
        cfg.Server.Host = "0.0.0.0"
    }
    if cfg.Server.Port == 0 {
        cfg.Server.Port = 50050
    }
    if cfg.Heartbeat.Interval == 0 {
        cfg.Heartbeat.Interval = 5
    }
    if cfg.Heartbeat.Timeout == 0 {
        cfg.Heartbeat.Timeout = 15
    }
    if cfg.Chunk.Size == 0 {
        cfg.Chunk.Size = 64 * 1024 * 1024
    }
    
    return &cfg, nil
}

func LoadStorageConfig(path string) (*StorageConfig, error) {
    viper.SetConfigFile(path)
    viper.SetConfigType("yaml")
    
    if err := viper.ReadInConfig(); err != nil {
        return nil, err
    }
    
    var cfg StorageConfig
    if err := viper.Unmarshal(&cfg); err != nil {
        return nil, err
    }
    
    if cfg.Storage.DataDir == "" {
        cfg.Storage.DataDir = "./data"
    }
    if cfg.Storage.MaxDiskUsage == 0 {
        cfg.Storage.MaxDiskUsage = 0.8
    }
    if cfg.Heartbeat.Interval == 0 {
        cfg.Heartbeat.Interval = 5
    }
    
    return &cfg, nil
}