package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/rakunlabs/alan"
	"github.com/rakunlabs/bw"
	"github.com/rakunlabs/bw/cluster"
)

// Environment variables:
//   NODE_NAME   - unique node identifier (default: "node1")
//   DATA_DIR    - database directory (default: "/tmp/bw-<NODE_NAME>")
//   HTTP_PORT   - HTTP API port (default: "8080")
//   ALAN_PORT   - cluster communication port (default: "7946")
//   PEERS       - comma-separated peer addresses or DNS name (default: "localhost")
//   REPLICAS    - expected cluster size (default: "2")

type User struct {
	ID   string `bw:"id,pk"`
	Name string `bw:"name,index"`
	Age  int    `bw:"age,index"`
}

func main() {
	nodeName := envOr("NODE_NAME", "node1")
	dataDir := envOr("DATA_DIR", "/tmp/bw-"+nodeName)
	httpPort := envOr("HTTP_PORT", "8080")
	alanPort := envOrInt("ALAN_PORT", 5000)
	alanBindAddr := envOr("ALAN_BIND_ADDR", "127.0.1.1")
	peers := envOr("PEERS", "alan-chat.local")
	replicas := envOrInt("REPLICAS", 2)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// 1. Open database.
	db, err := bw.Open(dataDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	users, err := bw.RegisterBucket[User](db, "users")
	if err != nil {
		log.Fatal(err)
	}

	// 2. Create alan instance.
	a, err := alan.New(alan.Config{
		DNSAddr:  peers,
		BindAddr: alanBindAddr,
		Port:     alanPort,
		Replicas: replicas,
	})
	if err != nil {
		log.Fatal(err)
	}

	// 3. Create cluster with forward handler.
	var c *cluster.Cluster
	c = cluster.New(db, a,
		cluster.WithLockKey("example-leader"),
		cluster.WithSyncInterval(5*time.Second), // short interval for demo
		cluster.WithOnLeaderChange(func(isLeader bool) {
			log.Printf("[%s] leader=%v", nodeName, isLeader)
		}),
		cluster.WithForwardHandler(func(ctx context.Context, data []byte) []byte {
			var u User
			if err := json.Unmarshal(data, &u); err != nil {
				return []byte(`{"error":"invalid json"}`)
			}
			if err := users.Insert(ctx, &u); err != nil {
				return []byte(fmt.Sprintf(`{"error":%q}`, err.Error()))
			}
			c.NotifySync()
			return []byte(`{"ok":true}`)
		}),
	)

	if err := c.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer c.Stop()

	// 4. HTTP API.
	mux := http.NewServeMux()

	// GET /users - list all users
	mux.HandleFunc("GET /users", func(w http.ResponseWriter, r *http.Request) {
		all, err := users.Find(r.Context(), nil)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(all)
	})

	// POST /users - create a user (forwarded to leader automatically)
	mux.HandleFunc("POST /users", func(w http.ResponseWriter, r *http.Request) {
		var u User
		if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
			http.Error(w, "invalid json", 400)
			return
		}

		data, _ := json.Marshal(u)
		resp, err := c.Forward(r.Context(), data)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(resp)
	})

	// GET /status - node info
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"node":    nodeName,
			"leader":  c.IsLeader(),
			"version": db.Version(),
		})
	})

	srv := &http.Server{Addr: ":" + httpPort, Handler: mux}

	go func() {
		log.Printf("[%s] HTTP listening on :%s", nodeName, httpPort)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Printf("[%s] shutting down...", nodeName)
	srv.Shutdown(context.Background())
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return fallback
}
