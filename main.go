package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Attempts int = iota // Unique keys
	Retry
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
	attempts := GetAttemptsFromContext(r)
	if attempts > 3 {
		log.Printf("%s(%s) Max attempts reached, terminating\n", r.RemoteAddr, r.URL.Path)
		http.Error(w, "Service not available", http.StatusServiceUnavailable)
		return
	}
	node := nodePool.NextNode()
	if node != nil {
		node.ReverseProxy.ServeHTTP(w, r)
		return
	}
	// 0 active nodes available
	http.Error(w, "Downtime: No nodes available", http.StatusServiceUnavailable)
}

// Status check node status by establishing TCP connection
func (n *Node) Status() bool {
	conn, err := net.DialTimeout("tcp", n.URL.Host, 2*time.Second)
	if err != nil {
		log.Println("Node unreachable: ", err)
		return false
	}
	_ = conn.Close()
	return true
}

// SetStatus sets node's status
func (n *Node) SetStatus(status bool) {
	n.mutex.Lock()
	n.Active = status
	n.mutex.Unlock()
}

// HealthCheck pings the node and update status
func (np *NodePool) HealthCheck() {
	for _, n := range np.nodes {
		status := n.Status()
		n.SetStatus(status)
		msg := "active"
		if !status {
			msg = "dead"
		}
		log.Printf("%s [%s]\n", n.URL, msg)
	}
}

// SetNodeStatus sets status of the given nodeURL
func (np *NodePool) SetNodeStatus(url *url.URL, status bool) {
	for _, n := range np.nodes {
		if n.URL.String() == url.String() {
			n.SetStatus(status)
			break
		}
	}
}

// GetAttemptsFromContext returns the attempts for request
func GetAttemptsFromContext(r *http.Request) int {
	if attempts, ok := r.Context().Value(Attempts).(int); ok {
		return attempts
	}
	return 1
}

// GetRetryFromContext returns the retry for request
func GetRetryFromContext(r *http.Request) int {
	if retry, ok := r.Context().Value(Retry).(int); ok {
		return retry
	}
	return 0
}

// Check health of nodes periodically
func healthCheck() {
	t := time.NewTicker(time.Minute * 2)
	for {
		select {
		case <-t.C:
			log.Printf("Starting health check...")
			nodePool.HealthCheck()

		}
	}
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
		proxy.ErrorHandler = func(w http.ResponseWriter, request *http.Request, e error) {
			log.Printf("[%s] %s\n", nodeURLParsed.Host, e.Error())
			retries := GetRetryFromContext(request)
			if retries < 3 {
				select {
				case <-time.After(10 * time.Millisecond):
					ctx := context.WithValue(request.Context(), Retry, retries+1)
					proxy.ServeHTTP(w, request.WithContext(ctx))
				}
				return
			}
			// After 3 retries, set this node as dead
			nodePool.SetNodeStatus(nodeURLParsed, false)

			// Try diff node
			attempts := GetAttemptsFromContext(request)
			log.Printf("%s(%s) Attempting retry %d\n", request.RemoteAddr, request.URL.Path, attempts)
			ctx := context.WithValue(request.Context(), Attempts, attempts+1)
			loadBalancer(w, request.WithContext(ctx))

		}
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
