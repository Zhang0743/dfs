package consistent

import (
    "testing"
)

func TestConsistent_AddNode(t *testing.T) {
    c := NewConsistent(100)
    c.AddNode("node1")
    c.AddNode("node2")
    c.AddNode("node3")
    
    nodes := c.GetNodes()
    if len(nodes) != 3 {
        t.Errorf("expected 3 nodes, got %d", len(nodes))
    }
}

func TestConsistent_GetNode(t *testing.T) {
    c := NewConsistent(100)
    c.AddNode("node1")
    c.AddNode("node2")
    c.AddNode("node3")
    
    // 同一个key应该路由到同一个节点
    key := "test-file-123"
    node1 := c.GetNode(key)
    node2 := c.GetNode(key)
    
    if node1 != node2 {
        t.Errorf("same key routed to different nodes: %s vs %s", node1, node2)
    }
}

func TestConsistent_RemoveNode(t *testing.T) {
    c := NewConsistent(100)
    c.AddNode("node1")
    c.AddNode("node2")
    
    key := "test-key"
    oldNode := c.GetNode(key)
    
    c.RemoveNode("node1")
    newNode := c.GetNode(key)
    
    if newNode == "" {
        t.Error("expected non-empty node after removal")
    }
    
    t.Logf("key %s routed from %s to %s", key, oldNode, newNode)
}
