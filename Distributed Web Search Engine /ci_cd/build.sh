set -euo pipefail

# ---- Detect environment ----
detect_environment() {
  # Check for EC2 instance metadata service
  if curl -s -m 1 http://169.254.169.254/latest/meta-data/instance-id >/dev/null 2>&1; then
    echo "ec2"
  else
    echo "local"
  fi
}

ENV_TYPE=$(detect_environment)

# ---- Configuration ----
JAR="${JAR:-lib/megasearch.jar}"

LOG_DIR="${LOG_DIR:-logs}"
PID_DIR="${PID_DIR:-pids}"

KVS_COORD_PORT="${KVS_COORD_PORT:-8000}"
KVS_WORKER_BASE_PORT="${KVS_WORKER_BASE_PORT:-8001}"
FL_COORD_PORT="${FL_COORD_PORT:-9000}"
FL_WORKER_BASE_PORT="${FL_WORKER_BASE_PORT:-9001}"

# Default webapp port (HTTP). HTTPS binds 443 internally.
DEFAULT_WEBAPP_PORT=8080

mkdir -p "$LOG_DIR" "$PID_DIR"

# ---- Helpers ----
require_jar() {
  if [[ ! -f "$JAR" ]]; then
    echo "Error: JAR not found at $JAR"
    if [[ "$ENV_TYPE" == "ec2" ]]; then
      echo "Upload it first (standard layout):"
      echo "  ssh -i <key>.pem ec2-user@<ip> 'mkdir -p lib ci_cd'"
      echo "  scp -i <key>.pem lib/megasearch.jar ec2-user@<ip>:lib/"
      echo "  scp -i <key>.pem ci_cd/build.sh ec2-user@<ip>:ci_cd/"
    else
      echo "Build it first:"
      echo "  ci_cd/build.sh build"
    fi
    exit 1
  fi
}

is_port_listening() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
  elif command -v ss >/dev/null 2>&1; then
    ss -ltn "sport = :$port" | grep -q LISTEN
  else
    netstat -ltn 2>/dev/null | grep -q ":$port "
  fi
}

kill_by_port() {
  local port="$1"
  local pids=""
  
  if command -v lsof >/dev/null 2>&1; then
    pids=$(lsof -ti:"$port" 2>/dev/null || true)
  elif command -v fuser >/dev/null 2>&1; then
    pids=$(fuser "$port/tcp" 2>/dev/null | tr -d ' ' || true)
  fi
  
  if [[ -n "$pids" ]]; then
    echo "Killing processes on port $port: $pids"
    echo "$pids" | xargs kill 2>/dev/null || true
    sleep 1
    # Force kill if still present
    if command -v lsof >/dev/null 2>&1; then
      local still
      still=$(lsof -ti:"$port" 2>/dev/null || true)
      if [[ -n "$still" ]]; then
        echo "$still" | xargs kill -9 2>/dev/null || true
      fi
    fi
  fi
}

wait_for_port() {
  local port="$1"; local name="$2"; local tries=30
  echo "Waiting for $name on port $port..."
  local i=1
  while [[ $i -le $tries ]]; do
    if is_port_listening "$port"; then
      echo "✓ $name is listening on $port"
      return 0
    fi
    sleep 0.5
    ((i++))
  done
  echo "⚠️ Timed out waiting for $name on $port"
  return 1
}

start_one() {
  local name="$1"; shift
  local log_file="$LOG_DIR/$name.log"
  echo "Starting $name ..."
  
  # On EC2, use sudo for webapp on port 443
  if [[ "$ENV_TYPE" == "ec2" && "$name" == "webapp" ]]; then
    sudo nohup "$@" >"$log_file" 2>&1 &
  else
    nohup "$@" >"$log_file" 2>&1 &
  fi
  
  local pid=$!
  echo $pid >"$PID_DIR/$name.pid"
  echo "✓ $name (pid $pid) → logs: $log_file"
}

stop_one() {
  local name="$1"
  local pid_file="$PID_DIR/$name.pid"
  if [[ -f "$pid_file" ]]; then
    local pid
    pid=$(cat "$pid_file")
    if kill -0 "$pid" >/dev/null 2>&1; then
      echo "Stopping $name (pid $pid)..."
      
      # On EC2, use sudo for webapp
      if [[ "$ENV_TYPE" == "ec2" && "$name" == "webapp" ]]; then
        sudo kill "$pid" 2>/dev/null || true
        sleep 1
        if kill -0 "$pid" >/dev/null 2>&1; then
          sudo kill -9 "$pid" 2>/dev/null || true
        fi
      else
        kill "$pid" 2>/dev/null || true
        sleep 1
        if kill -0 "$pid" >/dev/null 2>&1; then
          kill -9 "$pid" 2>/dev/null || true
        fi
      fi
    else
      echo "PID stale for $name; removing."
    fi
    rm -f "$pid_file"
  else
    echo "$name not running (no PID file)."
  fi
}

status_one() {
  local name="$1"; local port="$2"
  local pid_file="$PID_DIR/$name.pid"
  if [[ -f "$pid_file" ]]; then
    local pid
    pid=$(cat "$pid_file")
    if kill -0 "$pid" >/dev/null 2>&1; then
      echo "✓ $name running (pid $pid)"
    else
      echo "⚠️ $name PID file exists but process not running"
    fi
  else
    echo "✗ $name not running"
  fi
  if is_port_listening "$port"; then
    echo "  • Port $port: LISTEN"
  else
    echo "  • Port $port: free"
  fi
}

build_jar() {
  echo "🧹 Cleaning and preparing output directories..."
  rm -rf bin
  mkdir -p bin lib
  
  echo "🧪 Compiling all sources..."
  javac -d bin $(find src -name '*.java')
  
  echo "📦 Copying static resources..."
  mkdir -p bin/frontend/static
  cp -r src/frontend/static/* bin/frontend/static/
  
  echo "📦 Creating JAR file..."
  pushd bin >/dev/null
  local all_classes=$(find . -name '*.class' | wc -l | tr -d ' ')
  if [ "$all_classes" != "0" ]; then
    jar cf ../lib/megasearch.jar .
    echo "✓ Created lib/megasearch.jar (complete, with static files)"
  fi
  popd >/dev/null
  
  echo "✅ Build complete"
}

fresh_clean() {
  echo "🧹 Fresh clean: stopping prior services and freeing ports..."
  
  # Bound how many worker ports we try to clean (defaults to 20)
  local max_workers=20
  if [[ -n "${WORKER_COUNT:-}" ]]; then
    max_workers=$WORKER_COUNT
  fi
  
  # Stop by PID files first
  for ((i=1; i<=max_workers; i++)); do
    stop_one "flame-worker$i" 2>/dev/null || true
    stop_one "kvs-worker$i" 2>/dev/null || true
  done
  stop_one "flame-coordinator" 2>/dev/null || true
  stop_one "kvs-coordinator" 2>/dev/null || true
  stop_one "webapp" 2>/dev/null || true

  # Also kill by port (coordinators, workers, webapp, and 443 on EC2)
  local ports_to_clean="$KVS_COORD_PORT $FL_COORD_PORT 8080"
  
  # Add KVS worker ports
  for ((i=1; i<=max_workers; i++)); do
    ports_to_clean="$ports_to_clean $((KVS_WORKER_BASE_PORT + i - 1))"
  done
  
  # Add Flame worker ports
  for ((i=1; i<=max_workers; i++)); do
    ports_to_clean="$ports_to_clean $((FL_WORKER_BASE_PORT + i - 1))"
  done
  
  # Add 443 only for EC2
  if [[ "$ENV_TYPE" == "ec2" ]]; then
    ports_to_clean="$ports_to_clean 443"
  fi
  
  for p in $ports_to_clean; do
    kill_by_port "$p" 2>/dev/null || true
  done

  # Remove any stale PID files
  rm -f "$PID_DIR"/*.pid 2>/dev/null || true
}

optional_wipe_db() {
  echo "🗑️ Wiping database state..."
  rm -rf database/kvs_workers/worker* 2>/dev/null || true
  rm -rf database/flame_workers 2>/dev/null || true
}

start_all() {
  # Validate WORKER_COUNT is set
  if [[ -z "${WORKER_COUNT:-}" ]]; then
    echo "Error: WORKER_COUNT not set"
    echo "Set it first: export WORKER_COUNT=<your worker count>"
    exit 1
  fi
  
  require_jar
  fresh_clean
  if [[ "${WIPE_DB:-false}" == "true" ]]; then
    optional_wipe_db
  fi

  echo "Starting services with $WORKER_COUNT worker(s) on $ENV_TYPE environment..."

  # Start KVS Coordinator
  start_one "kvs-coordinator" java -cp "$JAR" kvs.Coordinator "$KVS_COORD_PORT"
  wait_for_port "$KVS_COORD_PORT" "KVS Coordinator" || true

  # Start KVS Workers
  for ((i=1; i<=WORKER_COUNT; i++)); do
    local kvs_port=$((KVS_WORKER_BASE_PORT + i - 1))
    local worker_name="worker$i"

    # Increase KVS Worker heap to reduce OOM when handling large rows
    start_one "kvs-worker$i" java -Xmx6g -cp "$JAR" kvs.Worker "$kvs_port" "$worker_name" "localhost:$KVS_COORD_PORT"
    wait_for_port "$kvs_port" "KVS Worker $i" || true
  done

  # Start Flame Coordinator
  start_one "flame-coordinator" java -cp "$JAR" flame.Coordinator "$FL_COORD_PORT" "localhost:$KVS_COORD_PORT"
  wait_for_port "$FL_COORD_PORT" "Flame Coordinator" || true

  # Start Flame Workers (with 2GB heap to prevent OOM during crawling)
  for ((i=1; i<=WORKER_COUNT; i++)); do
    local fl_port=$((FL_WORKER_BASE_PORT + i - 1))
    start_one "flame-worker$i" java -Xmx2g -cp "$JAR" flame.Worker "$fl_port" "localhost:$FL_COORD_PORT"
    wait_for_port "$fl_port" "Flame Worker $i" || true
  done

  echo
  echo "✅ All services started ($WORKER_COUNT worker(s)) on $ENV_TYPE."
  echo "Logs in: $LOG_DIR/"
  echo "KVS Coordinator: http://localhost:$KVS_COORD_PORT"
  echo "Flame Coordinator: http://localhost:$FL_COORD_PORT"
}

stop_all() {
  # Stop webapp if running
  stop_one "webapp" 2>/dev/null || true
  
  # Stop workers (check up to 20)
  for ((i=1; i<=20; i++)); do
    stop_one "flame-worker$i" 2>/dev/null || true
    stop_one "kvs-worker$i" 2>/dev/null || true
  done
  
  # Stop coordinators
  stop_one "flame-coordinator" 2>/dev/null || true
  stop_one "kvs-coordinator" 2>/dev/null || true
  
  # Fallback: alternative pkill approach on EC2
  if [[ "$ENV_TYPE" == "ec2" ]]; then
    sudo pkill -f "kvs.Coordinator" 2>/dev/null || true
    sudo pkill -f "kvs.Worker" 2>/dev/null || true
    sudo pkill -f "flame.Coordinator" 2>/dev/null || true
    sudo pkill -f "flame.Worker" 2>/dev/null || true
    sudo pkill -f "frontend.app.WebApp" 2>/dev/null || true
  fi
  
  echo "✅ All services stopped."
}

status_all() {
  echo "=== Environment: $ENV_TYPE ==="
  echo
  echo "=== Coordinators ==="
  status_one "kvs-coordinator" "$KVS_COORD_PORT"
  status_one "flame-coordinator" "$FL_COORD_PORT"
  
  echo
  echo "=== KVS Workers ==="
  local found_kvs=false
  for ((i=1; i<=20; i++)); do
    local kvs_port=$((KVS_WORKER_BASE_PORT + i - 1))
    if [[ -f "$PID_DIR/kvs-worker$i.pid" ]]; then
      status_one "kvs-worker$i" "$kvs_port"
      found_kvs=true
    fi
  done
  if [[ "$found_kvs" == "false" ]]; then
    echo "No KVS workers running"
  fi
  
  echo
  echo "=== Flame Workers ==="
  local found_flame=false
  for ((i=1; i<=20; i++)); do
    local fl_port=$((FL_WORKER_BASE_PORT + i - 1))
    if [[ -f "$PID_DIR/flame-worker$i.pid" ]]; then
      status_one "flame-worker$i" "$fl_port"
      found_flame=true
    fi
  done
  if [[ "$found_flame" == "false" ]]; then
    echo "No Flame workers running"
  fi
  
  echo
  echo "=== WebApp ==="
  if [[ -f "$PID_DIR/webapp.pid" ]]; then
    # Try to determine actual port from PID
    status_one "webapp" "$DEFAULT_WEBAPP_PORT"
  else
    echo "✗ webapp not running"
  fi
}

submit_job() {
  local job_class="$1"
  local job_name="$2"
  shift 2  # Remove job class and name from arguments
  
  require_jar
  
  if ! is_port_listening "$FL_COORD_PORT"; then
    echo "❌ Flame Coordinator not running on port $FL_COORD_PORT"
    echo "Start services first: $0 start"
    exit 1
  fi
  
  echo "📤 Submitting $job_name job to Flame..."
  # Use "$@" directly to handle empty args properly with set -u
  java -cp "$JAR" flame.FlameSubmit "localhost:$FL_COORD_PORT" "$JAR" "$job_class" "$@"
  local exit_code=$?
  
  if [[ $exit_code -eq 0 ]]; then
    echo "✅ $job_name completed successfully"
  else
    echo "❌ $job_name failed with exit code $exit_code"
    exit $exit_code
  fi
}

deploy_files() {
  # Use environment variables if no args provided, otherwise use positional args
  local ip="${1:-$EC2_IP}"
  local key_file="${2:-$EC2_KEY}"
  
  if [[ -z "$ip" || -z "$key_file" ]]; then
    echo "Error: deploy requires <ec2-ip> <key.pem>"
    echo "Usage: $0 deploy [ec2-ip] [key.pem]"
    echo "  Or set: export EC2_IP=<ip> EC2_KEY=<key.pem>"
    exit 1
  fi
  
  require_jar
  if [[ ! -f "$key_file" ]]; then
    echo "Error: key file '$key_file' not found"
    exit 1
  fi
  chmod 400 "$key_file" 2>/dev/null || true

  # Hardcoded files to deploy: JAR and build script
  local items=("lib/megasearch.jar" "ci_cd/build.sh")

  # Validate all items exist before any copy
  local missing=0
  for it in "${items[@]}"; do
    if [[ ! -e "$it" ]]; then
      echo "Missing: $it"
      missing=1
    fi
  done
  if [[ $missing -eq 1 ]]; then
    echo "Aborting deploy due to missing file(s)."
    exit 1
  fi

  echo "Deploying ${#items[@]} item(s) to ec2-user@${ip} using $key_file"
  
  # Create required directories on EC2
  ssh -i "$key_file" ec2-user@"$ip" "mkdir -p lib ci_cd" || { echo "Failed to create directories on EC2"; exit 1; }
  
  # Deploy hardcoded files
  echo "  • (file) lib/megasearch.jar"
  scp -i "$key_file" "lib/megasearch.jar" ec2-user@"$ip":lib/megasearch.jar || { echo "Deploy failed copying JAR"; exit 1; }
  
  echo "  • (file) ci_cd/build.sh"
  scp -i "$key_file" "ci_cd/build.sh" ec2-user@"$ip":ci_cd/build.sh || { echo "Deploy failed copying build script"; exit 1; }
  
  # Set execute permissions on the build script
  ssh -i "$key_file" ec2-user@"$ip" "chmod +x ci_cd/build.sh" || { echo "Failed to set execute permissions"; exit 1; }
  
  echo "Deploy success. SSH: ssh ec2-user@${ip} -i $key_file"
}

remote_start() {
  # Use environment variables if no args provided, otherwise use positional args
  local ip="${1:-$EC2_IP}"
  local key_file="${2:-$EC2_KEY}"
  
  if [[ -z "$ip" || -z "$key_file" ]]; then
    echo "Error: remote-start requires EC2_IP and EC2_KEY environment variables"
    echo "Usage: $0 remote-start [--wipe-db]"
    echo "  Or set: export EC2_IP=<ip> EC2_KEY=<key.pem>"
    exit 1
  fi
  
  # Shift past ip and key if they were provided as args
  [[ $# -ge 1 ]] && shift
  [[ $# -ge 1 ]] && shift

  if [[ ! -f "$key_file" ]]; then
    echo "Error: key file '$key_file' not found"
    exit 1
  fi
  chmod 400 "$key_file" 2>/dev/null || true

  # Ensure local jar exists; build if needed
  if [[ ! -f "$JAR" ]]; then
    echo "Local JAR missing; building before remote start..."
    "$0" build || { echo "Build failed"; exit 1; }
  fi

  echo "Copying JAR and script to EC2 ($ip) in standard layout..."
  ssh -i "$key_file" ec2-user@"$ip" "mkdir -p lib ci_cd" || { echo "Failed to create directories on EC2"; exit 1; }
  scp -i "$key_file" "$JAR" ec2-user@"$ip":lib/megasearch.jar || { echo "Failed to copy JAR"; exit 1; }
  scp -i "$key_file" ci_cd/build.sh ec2-user@"$ip":ci_cd/build.sh || { echo "Failed to copy script"; exit 1; }

  # Pass through any remaining args to start command
  local start_opts=("$@")
  local remote_args=""
  if (( ${#start_opts[@]} > 0 )); then
    remote_args="${start_opts[*]}"
  fi
  echo "Starting services remotely..."
  ssh -i "$key_file" ec2-user@"$ip" "chmod +x ci_cd/build.sh && ci_cd/build.sh start $remote_args" || { echo "Remote start failed"; exit 1; }
  echo "Remote start complete. Check with: ssh -i $key_file ec2-user@$ip 'ci_cd/build.sh status'"
}

start_webapp() {
  local port="${1:-$DEFAULT_WEBAPP_PORT}"
  require_jar
  
  # Prevent double-binding 443 (HTTP and HTTPS) which causes 'Address already in use'.
  if [[ "$port" == "443" ]]; then
    echo "❌ Refusing to start HTTP on 443: this conflicts with the HTTPS listener."
    echo "   Use: $0 webapp 8080 (recommended); HTTPS will still bind 443 if keystore exists."
    echo "   On EC2, run with sudo: sudo $0 webapp 8080"
    exit 1
  fi
  
  # Check if already running
  if [[ -f "$PID_DIR/webapp.pid" ]]; then
    local pid
    pid=$(cat "$PID_DIR/webapp.pid")
    if kill -0 "$pid" 2>/dev/null; then
      echo "⚠️ WebApp already running (pid $pid)"
      echo "Stop it first: $0 stop"
      exit 1
    fi
  fi
  
  # Check if KVS is running
  if ! is_port_listening "$KVS_COORD_PORT"; then
    echo "⚠️ Warning: KVS Coordinator not running. WebApp may not function correctly."
  fi
  
  # On EC2 with port 443, remind about keystore
  if [[ "$ENV_TYPE" == "ec2" && "$port" == "443" ]]; then
    if [[ ! -f "keystore.jks" ]]; then
      echo "⚠️ Warning: keystore.jks not found. HTTPS on 443 requires SSL certificate."
      echo "Run certbot and generate keystore first. Continuing anyway..."
    fi
  fi
  
  echo "🚀 Starting WebApp on port $port ($ENV_TYPE)..."
  start_one "webapp" java -cp "$JAR" frontend.app.WebApp "$port"
  wait_for_port "$port" "WebApp" || true
  
  echo "✅ WebApp started"
  if [[ "$ENV_TYPE" == "ec2" ]]; then
    echo "   HTTP:  http://localhost:$port"
    echo "   HTTPS: https://<your-domain> (443)"
  else
    echo "   HTTP:  http://localhost:$port"
    echo "   HTTPS: https://localhost (443)"
  fi
}

run_pipeline() {
  echo "🔄 Running full pipeline: Crawler → Indexer → PageRank → TF-IDF"
  submit_job "jobs.Crawler" "Crawler"
  submit_job "jobs.Indexer" "Indexer"
  submit_job "jobs.PageRank" "PageRank"
  submit_job "jobs.TfIdf" "TF-IDF"
  echo "✅ Pipeline completed successfully"
}

print_usage() {
  echo "Usage: $0 <command> [options]"
  echo ""
  echo "Environment: $ENV_TYPE"
  echo ""
  echo "Commands:"
  echo "  build                             Compile sources and create JAR"
  echo "  start [--wipe-db]                 Start all services (uses \$WORKER_COUNT)"
  echo "  stop                              Stop all services"
  echo "  status                            Show service status"
  echo "  restart                           Restart all services (uses \$WORKER_COUNT)"
  echo "  crawler [URL]                     Submit crawler job (optional seed URL)"
  echo "  indexer                           Submit indexer job"
  echo "  pagerank                          Submit pagerank job"
   echo "  tfidf                             Submit tf-idf job"
   echo "  pipeline                          Run crawler→indexer→pagerank→tf-idf"
  echo "  deploy                            Copy JAR and build script to EC2 (uses \$EC2_IP \$EC2_KEY)"
  echo "  remote-start [opts]               Copy & start on EC2 (uses \$EC2_IP \$EC2_KEY)"
  if [[ "$ENV_TYPE" == "ec2" ]]; then
    echo "  webapp [port]                     Start webapp (default 8080; HTTPS also on 443)"
  else
    echo "  webapp [port]                     Start webapp (default 8080)"
  fi
  echo ""
  echo "Environment Variables:"
  echo "  WORKER_COUNT  Number of workers (required, set via: export WORKER_COUNT=N)"
  echo ""
  echo "Options:"
  echo "  --wipe-db     Remove database state before starting"
}

# ---- Entry ----
# Require at least one parameter (command)
if [[ $# -lt 1 ]]; then
  echo "Error: command required."
  print_usage
  exit 1
fi

cmd="$1"
shift || true

# Parse options only for commands that support them (start, restart, webapp)
if [[ "$cmd" == "start" || "$cmd" == "restart" || "$cmd" == "webapp" ]]; then
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --wipe-db)
        export WIPE_DB=true
        shift
        ;;
      *)
        # For webapp command, this might be the port
        if [[ "$cmd" == "webapp" && "$1" =~ ^[0-9]+$ ]]; then
          WEBAPP_PORT="$1"
          shift
        else
          echo "Unknown option: $1"
          print_usage
          exit 1
        fi
        ;;
    esac
  done
fi

case "$cmd" in
  build)    build_jar ;;
  start)    start_all ;;
  stop)     stop_all ;;
  status)   status_all ;;
  restart)  stop_all; start_all ;;
  crawler)  submit_job "jobs.Crawler" "Crawler" "$@" ;;
  indexer)  submit_job "jobs.Indexer" "Indexer" "$@" ;;
  pagerank) submit_job "jobs.PageRank" "PageRank" "$@" ;;
  tfidf)    submit_job "jobs.TfIdf" "TF-IDF" "$@" ;;
  pipeline) run_pipeline ;;
  deploy)   deploy_files "$@" ;;
  remote-start) remote_start "$@" ;;
  webapp)   start_webapp "${WEBAPP_PORT:-$DEFAULT_WEBAPP_PORT}" ;;
  *) print_usage; exit 1 ;;
esac
