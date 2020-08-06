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

type Node struct {
	URL          *url.URL
	Active       bool
	mutex        sync.RWMutex
	ReverseProxy *httputil.ReverseProxy
}

type NodePool struct {
	nodes   []*Node
	current uint64
}

func (np *NodePool) AddNode(n *Node) {
	np.nodes = append(np.nodes, n)
}

func (np *NodePool) NextIdx() int {
	return int(atomic.AddUint64(&np.current, uint64(1)) % uint64(len(np.nodes)))
}

func (n *Node) isActive() bool {
	var active bool
	n.mutex.RLock()
	active = n.Active
	n.mutex.RUnlock()
	return active
}

// Find next active node
func (np *NodePool) NextNode() *Node {
	next := np.NextIdx()
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
	} else {
		// 0 active nodes available
		http.Error(w, "Downtime: No nodes available", http.StatusServiceUnavailable)
	}
}

func healthCheck() {

}

var nodePool NodePool

func main() {
	var nodeList string
	var port int
	flag.StringVar(&nodeList, "nodeList", "http://localhost:3031,http://localhost:3032", "List of avaiable nodes comma-separated")
	flag.IntVar(&port, "port", 3030, "Port to serve load-balancer")
	flag.Parse()

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

		log.Printf("Node %s configured\n", nodeURLParsed)
	}

	// Create LB server
	server := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: http.HandlerFunc(loadBalancer),
	}

	// Check health of nodes periodically
	go healthCheck()

	log.Printf("Load Balancer started")
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
