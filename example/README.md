# Example BW Cluster

Run 3 nodes

```sh
NODE_NAME=node1 HTTP_PORT=8081 ALAN_BIND_ADDR=127.0.1.1 REPLICAS=3 go run ./example
NODE_NAME=node2 HTTP_PORT=8082 ALAN_BIND_ADDR=127.0.1.2 REPLICAS=3 go run ./example
NODE_NAME=node3 HTTP_PORT=8083 ALAN_BIND_ADDR=127.0.1.3 REPLICAS=3 go run ./example
```

Create a user (works on any node — auto-forwards to leader)

```
curl -X POST http://localhost:8082/users -d '{"id":"1","name":"Ayse","age":28}'
```

Read from any node (local read)

```sh
curl http://localhost:8083/users
```

Check status

```sh
curl http://localhost:8081/status
```

The endpoints:
- POST /users — creates a user via Forward() (works on any node)
- GET /users — lists all users (local read)
- GET /status — shows node name, leader status, db version
