package consistent

import (
    "hash/crc32"
    "sort"
    "strconv"
    "sync"
)

// Consistent 一致性哈希环
type Consistent struct {
    nodes        map[uint32]string // 虚拟节点哈希 -> 真实节点
    sortedHashes []uint32          // 排序后的哈希环
    replicas     int              // 虚拟节点倍数
    mu           sync.RWMutex
}

// NewConsistent 创建一致性哈希环
func NewConsistent(replicas int) *Consistent {
    return &Consistent{
        nodes:    make(map[uint32]string),
        replicas: replicas,
    }
}

// AddNode 添加节点
func (c *Consistent) AddNode(node string) {
    c.mu.Lock()
    defer c.mu.Unlock()
    
    c.addNode(node)
}

func (c *Consistent) addNode(node string) {
    for i := 0; i < c.replicas; i++ {
        virtualKey := node + "#" + strconv.Itoa(i)
        hash := crc32.ChecksumIEEE([]byte(virtualKey))
        c.nodes[hash] = node
        c.sortedHashes = append(c.sortedHashes, hash)
    }
    sort.Slice(c.sortedHashes, func(i, j int) bool {
        return c.sortedHashes[i] < c.sortedHashes[j]
    })
}

// GetNode 获取key对应的节点
func (c *Consistent) GetNode(key string) string {
    c.mu.RLock()
    defer c.mu.RUnlock()
    
    if len(c.nodes) == 0 {
        return ""
    }
    
    hash := crc32.ChecksumIEEE([]byte(key))
    
    // 二分查找第一个 >= hash 的虚拟节点
    idx := sort.Search(len(c.sortedHashes), func(i int) bool {
        return c.sortedHashes[i] >= hash
    })
    
    // 如果超出环尾，回到环首
    if idx == len(c.sortedHashes) {
        idx = 0
    }
    
    return c.nodes[c.sortedHashes[idx]]
}

// RemoveNode 移除节点
func (c *Consistent) RemoveNode(node string) {
    c.mu.Lock()
    defer c.mu.Unlock()
    
    for i := 0; i < c.replicas; i++ {
        virtualKey := node + "#" + strconv.Itoa(i)
        hash := crc32.ChecksumIEEE([]byte(virtualKey))
        delete(c.nodes, hash)
    }
    
    // 重新构建排序哈希环
    c.sortedHashes = make([]uint32, 0, len(c.nodes))
    for k := range c.nodes {
        c.sortedHashes = append(c.sortedHashes, k)
    }
    sort.Slice(c.sortedHashes, func(i, j int) bool {
        return c.sortedHashes[i] < c.sortedHashes[j]
    })
}

// GetNodes 获取所有节点
func (c *Consistent) GetNodes() []string {
    c.mu.RLock()
    defer c.mu.RUnlock()
    
    nodeMap := make(map[string]bool)
    for _, node := range c.nodes {
        nodeMap[node] = true
    }
    
    nodes := make([]string, 0, len(nodeMap))
    for node := range nodeMap {
        nodes = append(nodes, node)
    }
    return nodes
}
