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

	"github.com/dgraph-io/badger/v4"
	"github.com/rakunlabs/alan"
	"github.com/rakunlabs/bw"
	"github.com/rakunlabs/bw/cluster"
)

// Environment variables:
//   NODE_NAME       - unique node identifier (default: "node1")
//   DATA_DIR        - parent directory for the two databases (default: "/tmp/bw-<NODE_NAME>")
//   HTTP_PORT       - HTTP API port (default: "8080")
//   ALAN_PORT       - cluster communication port (default: 5000)
//   ALAN_BIND_ADDR  - bind address for alan (default: "127.0.1.1")
//   PEERS           - DNS name resolved to peer addresses (default: "alan.local")
//   REPLICAS        - expected cluster size (default: 2)

type User struct {
	ID   string `bw:"id,pk"`
	Name string `bw:"name,index"`
	Age  int    `bw:"age,index"`
}

type Order struct {
	ID     string `bw:"id,pk"`
	UserID string `bw:"user_id,index"`
	Total  int    `bw:"total"`
}

func main() {
	nodeName := envOr("NODE_NAME", "node1")
	dataDir := envOr("DATA_DIR", "/tmp/bw-"+nodeName)
	httpPort := envOr("HTTP_PORT", "8080")
	alanPort := envOrInt("ALAN_PORT", 5000)
	alanBindAddr := envOr("ALAN_BIND_ADDR", "127.0.1.1")
	peers := envOr("PEERS", "alan.local")
	replicas := envOrInt("REPLICAS", 2)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// 1. Open two independent databases. The new WithBadgerTune option
	//    lets us tweak per-field badger settings without rebuilding the
	//    whole badger.Options object: bw still applies its lighter cache
	//    and log-size defaults underneath us, and the path argument is
	//    respected as usual.
	tune := bw.WithBadgerTune(func(bo *badger.Options) {
		bo.NumVersionsToKeep = 3 // keep a few historical versions
		bo.NumGoroutines = 4     // smaller pool for the demo
	})

	users, err := bw.Open(dataDir+"/users", tune)
	if err != nil {
		log.Fatal(err)
	}
	defer users.Close()

	orders, err := bw.Open(dataDir+"/orders", tune)
	if err != nil {
		log.Fatal(err)
	}
	defer orders.Close()

	usersBkt, err := bw.RegisterBucket[User](users, "users")
	if err != nil {
		log.Fatal(err)
	}
	ordersBkt, err := bw.RegisterBucket[Order](orders, "orders")
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

	// 3. Create cluster with TWO databases under one leader election.
	//    The primary DB is passed to New and named via WithDBName; the
	//    secondary is added with AddDB. The same cluster lock governs
	//    both, but versions/streams are tracked independently on the
	//    wire.
	var c *cluster.Cluster
	c = cluster.New(users, a,
		cluster.WithLockKey("example-leader"),
		cluster.WithSyncInterval(5*time.Second), // short for demo
		cluster.WithDBName("users"),
		cluster.WithOnLeaderChange(func(isLeader bool) {
			log.Printf("[%s] leader=%v", nodeName, isLeader)
		}),
		cluster.WithForwardHandler(func(ctx context.Context, data []byte) []byte {
			// Tiny tagged-envelope: first byte selects which kind of
			// write this is. Keeps the wire format simple while
			// allowing a single Forward channel to route to both
			// buckets.
			if len(data) < 1 {
				return []byte(`{"error":"empty payload"}`)
			}
			switch data[0] {
			case 'u':
				var u User
				if err := json.Unmarshal(data[1:], &u); err != nil {
					return []byte(`{"error":"invalid user json"}`)
				}
				if err := usersBkt.Insert(ctx, &u); err != nil {
					return []byte(fmt.Sprintf(`{"error":%q}`, err.Error()))
				}
				if err := c.NotifySyncDB(ctx, "users"); err != nil {
					log.Printf("[%s] notify sync users: %v", nodeName, err)
				}
				return []byte(`{"ok":true,"db":"users"}`)
			case 'o':
				var o Order
				if err := json.Unmarshal(data[1:], &o); err != nil {
					return []byte(`{"error":"invalid order json"}`)
				}
				if err := ordersBkt.Insert(ctx, &o); err != nil {
					return []byte(fmt.Sprintf(`{"error":%q}`, err.Error()))
				}
				if err := c.NotifySyncDB(ctx, "orders"); err != nil {
					log.Printf("[%s] notify sync orders: %v", nodeName, err)
				}
				return []byte(`{"ok":true,"db":"orders"}`)
			default:
				return []byte(`{"error":"unknown payload tag"}`)
			}
		}),
	)
	if err := c.AddDB("orders", orders); err != nil {
		log.Fatal(err)
	}

	if err := c.Start(ctx); err != nil {
		log.Fatal(err)
	}
	defer c.Stop()

	// 4. HTTP API.
	mux := http.NewServeMux()

	mux.HandleFunc("GET /users", func(w http.ResponseWriter, r *http.Request) {
		all, err := usersBkt.Find(r.Context(), nil)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(all)
	})

	mux.HandleFunc("GET /orders", func(w http.ResponseWriter, r *http.Request) {
		all, err := ordersBkt.Find(r.Context(), nil)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(all)
	})

	mux.HandleFunc("POST /users", func(w http.ResponseWriter, r *http.Request) {
		var u User
		if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
			http.Error(w, "invalid json", 400)
			return
		}
		body, _ := json.Marshal(u)
		payload := append([]byte{'u'}, body...)
		resp, err := c.Forward(r.Context(), payload)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(resp)
	})

	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		var o Order
		if err := json.NewDecoder(r.Body).Decode(&o); err != nil {
			http.Error(w, "invalid json", 400)
			return
		}
		body, _ := json.Marshal(o)
		payload := append([]byte{'o'}, body...)
		resp, err := c.Forward(r.Context(), payload)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(resp)
	})

	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		st := c.Status()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"node":           nodeName,
			"leader":         st.IsLeader,
			"leader_healthy": st.LeaderHealthy,
			"versions":       st.Versions,
			"dbs":            c.DBNames(),
		})
	})

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if !c.LeaderHealthy() {
			http.Error(w, "not leader or no quorum", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok"))
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
