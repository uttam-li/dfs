.PHONY: build client master chunkserver proto clean cleanProto help all

build:
	@echo "🔨 Building DFS components..."
	@go build -o bin/client cmd/client/main.go
	@echo "✅ Client binary created successfully"
	@go build -o bin/master cmd/master/main.go
	@echo "✅ Master binary created successfully"
	@go build -o bin/chunkserver cmd/chunkserver/main.go
	@echo "✅ Chunkserver binary created successfully"
	@echo "🎉 All DFS components built successfully!"

client:
	@echo "🚀 Starting FUSE Client..."
	@bin/client

master:
	@echo "🚀 Starting Master Service..."
	@bin/master

chunkserver:
	@if [ -z "$(PORT)" ] || [ -z "$(STORAGE)" ]; then \
		echo "❌ Error: PORT and STORAGE are required"; \
		echo ""; \
		echo "Usage:"; \
		echo "  make chunkserver PORT=<port> STORAGE=<storage_name> [MASTER=<master_addr>]"; \
		echo ""; \
		echo "Examples:"; \
		echo "  make chunkserver PORT=8081 STORAGE=chunk_1"; \
		echo "  make chunkserver PORT=8082 STORAGE=chunk_2"; \
		echo "  make chunkserver PORT=8083 STORAGE=chunk_3 MASTER=localhost:8000"; \
		echo ""; \
		echo "This will create storage in storage/<storage_name>/ directory"; \
		exit 1; \
	fi
	@echo "🚀 Starting ChunkServer on port $(PORT) with storage $(STORAGE)..."
	@MASTER_ADDR=$${MASTER:-localhost:8000}; \
	bin/chunkserver -port $(PORT) -storage $(STORAGE) -master $$MASTER_ADDR

proto:
	@echo "⚙️ Generating protobuf code..."
	@protoc -I. --go_out=module=github.com/uttam-li/dfs:. --go-grpc_out=module=github.com/uttam-li/dfs:. api/proto/common.proto
	@echo "  📄 Generated common.proto"
	@protoc -I. --go_out=module=github.com/uttam-li/dfs:. --go-grpc_out=module=github.com/uttam-li/dfs:. api/proto/master.proto
	@echo "  📄 Generated master.proto"
	@protoc -I. --go_out=module=github.com/uttam-li/dfs:. --go-grpc_out=module=github.com/uttam-li/dfs:. api/proto/chunkserver.proto
	@echo "  📄 Generated chunkserver.proto"
	@protoc -I. --go_out=module=github.com/uttam-li/dfs:. --go-grpc_out=module=github.com/uttam-li/dfs:. api/proto/persistence.proto
	@echo "  📄 Generated persistence.proto"
	@echo "✅ Protobuf code generation completed!"

clean:
	@echo "🧹 Cleaning up build artifacts..."
	@rm -rf bin logs mnt storage checkpoints
	@echo "✅ Cleanup completed successfully!"

help:
	@echo "DFS - Distributed File System"
	@echo ""
	@echo "Available targets:"
	@echo "  build          - Build all DFS components (client, master, chunkserver)"
	@echo "  client         - Start FUSE client"
	@echo "  master         - Start master service"
	@echo "  chunkserver    - Start chunkserver with PORT and STORAGE arguments"
	@echo "  proto          - Generate protobuf code"
	@echo "  clean          - Clean build artifacts and storage"
	@echo "  cleanProto     - Clean generated protobuf files"
	@echo "  all     - Start complete DFS system (master + 3 chunkservers + client)"
	@echo "  help           - Show this help message"
	@echo ""
	@echo "ChunkServer Usage:"
	@echo "  make chunkserver PORT=<port> STORAGE=<storage_name> [MASTER=<master_addr>]"
	@echo ""
	@echo "ChunkServer Examples:"
	@echo "  make chunkserver PORT=8081 STORAGE=chunk_1"
	@echo "  make chunkserver PORT=8082 STORAGE=chunk_2"
	@echo "  make chunkserver PORT=8083 STORAGE=chunk_3 MASTER=localhost:8000"
	@echo ""
	@echo "Complete DFS System:"
	@echo "  scripts/start_all.sh   - Start complete DFS (master + 5 chunkservers + client)"
	@echo "  make all                - Same as above via make"
	@echo ""
	@echo "This creates separate storage directories: storage/chunk_1/, storage/chunk_2/, etc."

cleanProto:
	@echo "🧹 Cleaning up proto files"
	@rm -rf api/generated
	@echo "✅ Cleanup completed successfully!"

all:
	@echo "🚀 Starting complete DFS system..."
	@scripts/start_all.sh