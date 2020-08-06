package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
)

// Node holds the data about a backend server
type Node struct {
	URL          *url.URL
	Active       bool
	mutex        sync.RWMutex
	ReverseProxy *httputil.ReverseProxy
}

// NodePool holds slice of nodes and most recently used node index
type NodePool struct {
	nodes   []*Node
	current uint64
}

// AddNode new node to NodePool
func (np *NodePool) AddNode(n *Node) {
	np.nodes = append(np.nodes, n)
}

// NextIdx atomically increase the counter and return an index
func (np *NodePool) NextIdx() int {
	return int(atomic.AddUint64(&np.current, uint64(1)) % uint64(len(np.nodes)))
}

// isActive returns whether node is active or dead
func (n *Node) isActive() bool {
	var active bool
	n.mutex.RLock()
	active = n.Active
	n.mutex.RUnlock()
	return active
}

// NextNode find next active node
func (np *NodePool) NextNode() *Node {
	next := np.NextIdx()
	// Round Robin algorithm
	for i := next; i < len(np.nodes)+next; i++ {
		idx := i % len(np.nodes)
		if np.nodes[idx].isActive() {
			atomic.StoreUint64(&np.current, uint64(idx))
			return np.nodes[idx]
		}
	}
	return nil
}

// Balance incoming requests
func loadBalancer(w http.ResponseWriter, r *http.Request) {
	node := nodePool.NextNode()
	if node != nil {
		node.ReverseProxy.ServeHTTP(w, r)
		return
	}
	// 0 active nodes available
	http.Error(w, "Downtime: No nodes available", http.StatusServiceUnavailable)
}

// Check health of nodes periodically
func healthCheck() {

}

var nodePool NodePool

func main() {
	var nodeList string
	var port int
	flag.StringVar(&nodeList, "nodeList", "", "List of avaiable nodes comma-separated")
	flag.IntVar(&port, "port", 3030, "Port to serve load-balancer")
	flag.Parse()

	if len(nodeList) == 0 {
		log.Fatal("Please provide one or more nodes to load balance")
	}

	for _, nodeURL := range strings.Split(nodeList, ",") {
		nodeURLParsed, err := url.Parse(nodeURL)
		if err != nil {
			log.Fatal(err)
		}
		proxy := httputil.NewSingleHostReverseProxy(nodeURLParsed)
		nodePool.AddNode(&Node{
			URL:          nodeURLParsed,
			Active:       true,
			ReverseProxy: proxy,
		})

		log.Printf("Configured node: %s\n", nodeURLParsed)
	}

	// Create LB server
	server := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: http.HandlerFunc(loadBalancer),
	}

	go healthCheck()

	log.Printf("Load Balancer started on port: %d", port)
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
