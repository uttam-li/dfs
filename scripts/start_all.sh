#!/bin/bash

# Script to start the complete DFS system: Master + Multiple ChunkServers + Client
# This demonstrates the full distributed file system setup

echo "🚀 DFS Complete System Test"
echo "============================"

# Service PIDs for tracking
MASTER_PID=""
CHUNK1_PID=""
CHUNK2_PID=""
CHUNK3_PID=""
CHUNK4_PID=""
CHUNK5_PID=""
CLIENT_PID=""

# Function to cleanup all background processes gracefully
cleanup() {
    echo ""
    echo "🛑 Graceful shutdown initiated..."
    echo "================================="
    
    # Stop client first
    if [ ! -z "$CLIENT_PID" ] && kill -0 $CLIENT_PID 2>/dev/null; then
        echo "🔄 Stopping FUSE Client (PID: $CLIENT_PID)..."
        kill -TERM $CLIENT_PID 2>/dev/null
        sleep 3
        if kill -0 $CLIENT_PID 2>/dev/null; then
            echo "⚠️  Force killing client..."
            kill -KILL $CLIENT_PID 2>/dev/null
        fi
        echo "✅ Client stopped"
    fi
    
    # Stop chunkservers
    for pid_var in CHUNK1_PID CHUNK2_PID CHUNK3_PID CHUNK4_PID CHUNK5_PID; do
        pid=${!pid_var}
        if [ ! -z "$pid" ] && kill -0 $pid 2>/dev/null; then
            echo "🔄 Stopping ChunkServer ($pid_var: $pid)..."
            kill -TERM $pid 2>/dev/null
            sleep 2
            if kill -0 $pid 2>/dev/null; then
                echo "⚠️  Force killing chunkserver..."
                kill -KILL $pid 2>/dev/null
            fi
        fi
    done
    echo "✅ All ChunkServers stopped"
    
    # Stop master last
    if [ ! -z "$MASTER_PID" ] && kill -0 $MASTER_PID 2>/dev/null; then
        echo "🔄 Stopping Master Service (PID: $MASTER_PID)..."
        kill -TERM $MASTER_PID 2>/dev/null
        sleep 3
        if kill -0 $MASTER_PID 2>/dev/null; then
            echo "⚠️  Force killing master..."
            kill -KILL $MASTER_PID 2>/dev/null
        fi
        echo "✅ Master stopped"
    fi
    
    # Clean up any remaining processes
    pkill -f "bin/master" 2>/dev/null
    pkill -f "bin/chunkserver" 2>/dev/null
    pkill -f "bin/client" 2>/dev/null
    
    echo ""
    echo "🧹 Cleanup completed successfully!"
    echo "📊 Final status: All DFS services stopped"
    exit 0
}

# Setup signal handler for cleanup
trap cleanup SIGINT SIGTERM

# Check if binaries are built
echo "🔍 Checking DFS binaries..."
missing_binaries=()

if [ ! -f "bin/master" ]; then
    missing_binaries+=("master")
fi
if [ ! -f "bin/chunkserver" ]; then
    missing_binaries+=("chunkserver")
fi
if [ ! -f "bin/client" ]; then
    missing_binaries+=("client")
fi

if [ ${#missing_binaries[@]} -gt 0 ]; then
    echo "❌ Missing binaries: ${missing_binaries[*]}"
    echo "🔨 Building DFS components..."
    make build
    if [ $? -ne 0 ]; then
        echo "❌ Build failed"
        exit 1
    fi
fi
echo "✅ All binaries present"

echo "✅ All binaries present"
echo ""

# Start services in proper order
echo "🚀 Starting DFS services in order..."
echo "===================================="

# 1. Start Master Service
echo "1️⃣ Starting Master Service..."
make master > /tmp/master.log 2>&1 &
MASTER_PID=$!
echo "   Master PID: $MASTER_PID"
echo "   Waiting for master to initialize..."
sleep 1

# Check if master started successfully
if ! kill -0 $MASTER_PID 2>/dev/null; then
    echo "❌ Master failed to start. Check /tmp/master.log"
    tail -10 /tmp/master.log
    exit 1
fi
echo "✅ Master service started successfully"

# 2. Start ChunkServers
echo ""
echo "2️⃣ Starting ChunkServers..."

echo "   Starting ChunkServer 1 (Port: 9081, Storage: chunk_1)"
make chunkserver PORT=9081 STORAGE=chunk_1 > /tmp/chunkserver1.log 2>&1 &
CHUNK1_PID=$!
echo "   ChunkServer 1 PID: $CHUNK1_PID"
sleep 1

echo "   Starting ChunkServer 2 (Port: 9082, Storage: chunk_2)"
make chunkserver PORT=9082 STORAGE=chunk_2 > /tmp/chunkserver2.log 2>&1 &
CHUNK2_PID=$!
echo "   ChunkServer 2 PID: $CHUNK2_PID"
sleep 1

echo "   Starting ChunkServer 3 (Port: 9083, Storage: chunk_3)"
make chunkserver PORT=9083 STORAGE=chunk_3 > /tmp/chunkserver3.log 2>&1 &
CHUNK3_PID=$!
echo "   ChunkServer 3 PID: $CHUNK3_PID"
sleep 1

echo "   Starting ChunkServer 4 (Port: 9084, Storage: chunk_4)"
make chunkserver PORT=9084 STORAGE=chunk_4 > /tmp/chunkserver4.log 2>&1 &
CHUNK4_PID=$!
echo "   ChunkServer 4 PID: $CHUNK4_PID"
sleep 1

echo "   Starting ChunkServer 5 (Port: 9085, Storage: chunk_5)"
make chunkserver PORT=9085 STORAGE=chunk_5 > /tmp/chunkserver5.log 2>&1 &
CHUNK5_PID=$!
echo "   ChunkServer 5 PID: $CHUNK5_PID"
sleep 1

# Verify chunkservers started
failed_chunks=()
for i in 1 2 3 4 5; do
    pid_var="CHUNK${i}_PID"
    pid=${!pid_var}
    if ! kill -0 $pid 2>/dev/null; then
        failed_chunks+=("ChunkServer $i")
    fi
done

if [ ${#failed_chunks[@]} -gt 0 ]; then
    echo "❌ Failed to start: ${failed_chunks[*]}"
    echo "Check log files in /tmp/ for details"
    cleanup
    exit 1
fi
echo "✅ All ChunkServers started successfully"

# 3. Start Client (FUSE)
echo ""
echo "3️⃣ Starting FUSE Client..."
echo "   Creating mount point if it doesn't exist..."
mkdir -p mnt
make client > /tmp/client.log 2>&1 &
CLIENT_PID=$!
echo "   Client PID: $CLIENT_PID"
echo "   Waiting for FUSE mount..."
sleep 1

# Check if client started successfully
if ! kill -0 $CLIENT_PID 2>/dev/null; then
    echo "❌ Client failed to start. Check /tmp/client.log"
    tail -10 /tmp/client.log
    cleanup
    exit 1
fi
echo "✅ FUSE Client started successfully"

echo ""
echo "🎉 All DFS services are running!"
echo "==============================="

echo "🎉 All DFS services are running!"
echo "==============================="

# Display service status
echo ""
echo "📊 Service Status:"
echo "=================="
echo "  🟢 Master Service    - PID: $MASTER_PID (Port: 8000)"
echo "  🟢 ChunkServer 1     - PID: $CHUNK1_PID (Port: 9081, Storage: .storage/chunk_1)"
echo "  🟢 ChunkServer 2     - PID: $CHUNK2_PID (Port: 9082, Storage: .storage/chunk_2)"
echo "  🟢 ChunkServer 3     - PID: $CHUNK3_PID (Port: 9083, Storage: .storage/chunk_3)"
echo "  🟢 ChunkServer 4     - PID: $CHUNK4_PID (Port: 9084, Storage: .storage/chunk_4)"
echo "  🟢 ChunkServer 5     - PID: $CHUNK5_PID (Port: 9085, Storage: .storage/chunk_5)"
echo "  🟢 FUSE Client       - PID: $CLIENT_PID (Mount: ./mnt)"

echo ""
echo "📁 Storage Directories:"
echo "======================"
ls -la .storage/ 2>/dev/null | head -10 || echo "No storage directories found"

echo ""
echo "🔍 Storage Structure:"
echo "===================="
for dir in .storage/*/; do
    if [ -d "$dir" ]; then
        files_count=$(ls -1 "$dir" 2>/dev/null | wc -l)
        echo "  $(basename "$dir")/ - $files_count files"
    fi
done

echo ""
echo "📋 Log Files:"
echo "============="
echo "  Master:        /tmp/master.log"
echo "  ChunkServer 1: /tmp/chunkserver1.log"
echo "  ChunkServer 2: /tmp/chunkserver2.log" 
echo "  ChunkServer 3: /tmp/chunkserver3.log"
echo "  ChunkServer 4: /tmp/chunkserver4.log"
echo "  ChunkServer 5: /tmp/chunkserver5.log"
echo "  Client:        /tmp/client.log"

echo ""
echo "🎯 Network Connectivity Test:"
echo "============================="

# Test master
if nc -z localhost 8000 2>/dev/null; then
    echo "  ✅ Master (port 8000) is accessible"
else
    echo "  ❌ Master (port 8000) is not accessible"
fi

# Test chunkservers
for port in 9081 9082 9083 9084 9085; do
    if nc -z localhost $port 2>/dev/null; then
        echo "  ✅ ChunkServer on port $port is accessible"
    else
        echo "  ❌ ChunkServer on port $port is not accessible"
    fi
done

echo ""
echo "💡 Usage Instructions:"
echo "======================"
echo "  • Files created in './mnt/' will be stored in the DFS"
echo "  • Each file will be chunked and distributed across chunkservers"
echo "  • Storage is located in .storage/chunk_1/, .storage/chunk_2/, .storage/chunk_3/, .storage/chunk_4/, .storage/chunk_5/"
echo "  • Monitor logs in /tmp/ for debugging"
echo ""
echo "🔧 Test Commands:"
echo "=================="
echo "  echo 'Hello DFS!' > mnt/test.txt    # Create a file"
echo "  ls -la mnt/                         # List files"
echo "  cat mnt/test.txt                   # Read file content"
echo "  rm mnt/test.txt                    # Delete file"

echo ""
echo "⏰ System is running..."
echo "Press Ctrl+C for graceful shutdown of all services"
echo ""

# Monitor services and provide periodic status updates
status_interval=0
while true; do
    sleep 10
    status_interval=$((status_interval + 10))
    current_time=$(date "+%H:%M:%S")
    
    # Count running services
    running_services=0
    service_status=""
    
    if kill -0 $MASTER_PID 2>/dev/null; then
        running_services=$((running_services + 1))
        service_status="$service_status Master"
    fi
    
    for i in 1 2 3 4 5; do
        pid_var="CHUNK${i}_PID"
        pid=${!pid_var}
        if kill -0 $pid 2>/dev/null; then
            running_services=$((running_services + 1))
            service_status="$service_status Chunk$i"
        fi
    done
    
    if kill -0 $CLIENT_PID 2>/dev/null; then
        running_services=$((running_services + 1))
        service_status="$service_status Client"
    fi
    
    echo "[$current_time] 📈 Services running: $running_services/7 ($service_status)"
    
    # Show detailed status every minute
    if [ $((status_interval % 60)) -eq 0 ]; then
        echo "[$current_time] 🔍 Detailed status check..."
        
        # Check for dead processes
        if ! kill -0 $MASTER_PID 2>/dev/null; then
            echo "  ⚠️  Master Service (PID: $MASTER_PID) stopped unexpectedly"
        fi
        
        for i in 1 2 3 4 5; do
            pid_var="CHUNK${i}_PID"
            pid=${!pid_var}
            if ! kill -0 $pid 2>/dev/null; then
                echo "  ⚠️  ChunkServer $i (PID: $pid) stopped unexpectedly"
            fi
        done
        
        if ! kill -0 $CLIENT_PID 2>/dev/null; then
            echo "  ⚠️  FUSE Client (PID: $CLIENT_PID) stopped unexpectedly"
        fi
        
        # Show mount status
        if mount | grep -q "./mnt"; then
            echo "  ✅ FUSE mount is active"
        else
            echo "  ❌ FUSE mount is not active"
        fi
        
        echo "[$current_time] 📊 Storage usage:"
        du -sh .storage/* 2>/dev/null | sed 's/^/    /' || echo "    No storage data"
    fi
done
