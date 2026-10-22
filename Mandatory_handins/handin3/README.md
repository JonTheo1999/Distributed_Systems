# Chit Chat

Mandatory Activity 3 – Distributed Systems, group "the GO GO langers".

## Project structure

```
handin3/
├── client/
│   └── client.go          # client process
├── grpc/
│   ├── chitchat.proto     # service and message definitions
│   ├── chitchat.pb.go     # generated (messages)
│   └── chitchat_grpc.pb.go# generated (service)
├── server/
│   └── server.go          # server process
├── go.mod
├── go.sum
└── readme.md
```

## Requirements

- Go 1.27 or newer (see `go.mod`)
- Only needed if you change the `.proto` file: `protoc` plus the plugins
  `protoc-gen-go` and `protoc-gen-go-grpc`

## How to run

All commands are run from the `handin3` folder.

### 1. Start the server

```
go run ./server
```

The server listens on `localhost:5000`.

### 2. Start clients

Open a new terminal for each client and give each client a unique username:

```
go run ./client Dave
```

```
go run ./client Evan
```

```
go run ./client Sarah
```

### 3. Chat

- Type a message and press **Enter** to publish it to everyone.
- Messages must be between 1 and 128 characters.
- Type **`/leave`** to leave the chat, or press **Ctrl+C**. Both make the client leave properly.

### 4. Stop the server

Press **Ctrl+C** in the server terminal.

## Logs

- **Client logs** are written to `client_<username>.log` in the folder you run the client from
  (e.g. `client_Alice.log`).
- **Server logs** are written to the terminal. To save them to a file, start the server with:

```
  cmd /c "go run ./server 2> server.log"
```

Every log line contains a timestamp, the component (`[Server]` / `[Client]`),
the event type (e.g. `[Join]`, `[Publish]`, `[Broadcast]`, `[Delivery]`, `[Leave]`)
and the relevant username.

## Regenerating the gRPC code

Only needed after changing `grpc/chitchat.proto`:

```
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative grpc/chitchat.proto
```
