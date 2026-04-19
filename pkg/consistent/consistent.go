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
    replicas     int               // 虚拟节点倍数
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
    
    idx := sort.Search(len(c.sortedHashes), func(i int) bool {
        return c.sortedHashes[i] >= hash
    })
    
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

// GetReplicas 获取指定数量的不重复节点（用于副本分配）
// key: 数据分片的key
// count: 需要的副本数量
func (c *Consistent) GetReplicas(key string, count int) []string {
    c.mu.RLock()
    defer c.mu.RUnlock()

    if len(c.nodes) == 0 {
        return []string{}
    }

    uniqueNodes := make(map[string]bool)
    for _, node := range c.nodes {
        uniqueNodes[node] = true
    }

    availableCount := len(uniqueNodes)
    if count > availableCount {
        count = availableCount
    }

    hash := crc32.ChecksumIEEE([]byte(key))
    idx := sort.Search(len(c.sortedHashes), func(i int) bool {
        return c.sortedHashes[i] >= hash
    })

    result := make([]string, 0, count)
    seen := make(map[string]bool)

    for i := 0; i < len(c.sortedHashes) && len(result) < count; i++ {
        currentIdx := (idx + i) % len(c.sortedHashes)
        node := c.nodes[c.sortedHashes[currentIdx]]
        if !seen[node] {
            seen[node] = true
            result = append(result, node)
        }
    }

    return result
}