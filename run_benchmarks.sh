#!/bin/bash

# ZVerse Benchmark Runner
# Comprehensive benchmark suite for different test scenarios

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default configuration
BENCH_TYPE="quick"
OUTPUT_DIR="target/criterion"
RUSTFLAGS="-C target-cpu=native -C opt-level=3"
THREADS=$(nproc)

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    cat << EOF
ZVerse Benchmark Runner

Usage: $0 [OPTIONS]

OPTIONS:
    -t, --type TYPE         Benchmark type: quick, standard, comprehensive, stress, all
    -o, --output DIR        Output directory for results (default: target/criterion)
    -j, --threads NUM       Number of threads to use (default: $(nproc))
    -m, --memory-check      Include memory usage monitoring
    -p, --profile           Enable CPU profiling
    -c, --compare BASELINE  Compare results with baseline
    -f, --format FORMAT     Output format: html, json, csv (default: html)
    -h, --help              Show this help message

BENCHMARK TYPES:
    quick           Fast benchmarks for development (5-10 minutes)
    standard        Standard benchmark suite (30-60 minutes)
    comprehensive   Full mixed-size benchmark suite (2-4 hours)
    stress          Memory and load stress tests (4-8 hours)
    all             All benchmarks (6-12 hours)

EXAMPLES:
    $0 --type quick                    # Quick development benchmarks
    $0 --type comprehensive --threads 16  # Full suite with 16 threads
    $0 --type stress --memory-check    # Stress tests with memory monitoring
    $0 --compare baseline.json         # Compare with previous results

EOF
}

# Function to check system requirements
check_requirements() {
    print_info "Checking system requirements..."
    
    # Check available memory
    if command -v free >/dev/null 2>&1; then
        AVAILABLE_MEM=$(free -g | awk '/^Mem:/{print $7}')
    elif command -v vm_stat >/dev/null 2>&1; then
        # macOS memory check
        AVAILABLE_MEM=$(vm_stat | awk '/Pages free:/{free=$3} /Pages inactive:/{inactive=$3} END{print int((free+inactive)*4096/1024/1024/1024)}')
    else
        AVAILABLE_MEM=8  # Default assumption
    fi
    if [[ $AVAILABLE_MEM -lt 4 ]]; then
        print_warning "Available memory is ${AVAILABLE_MEM}GB. Some tests may fail with less than 4GB."
    fi
    
    # Check disk space
    if command -v df >/dev/null 2>&1; then
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS df output is different
            AVAILABLE_DISK=$(df -g . | awk 'NR==2{print int($4)}')
        else
            AVAILABLE_DISK=$(df . | awk 'NR==2{print int($4/1024/1024)}')
        fi
        if [[ $AVAILABLE_DISK -lt 2 ]]; then
            print_warning "Available disk space is ${AVAILABLE_DISK}GB. Consider freeing up space."
        fi
    fi
    
    # Check if running in release mode
    if [[ ! "$RUSTFLAGS" =~ "opt-level=3" ]]; then
        print_warning "Not running with full optimizations. Performance may be suboptimal."
    fi
    
    print_success "System requirements check complete"
}

# Function to run specific benchmark
run_benchmark() {
    local bench_name=$1
    local description=$2
    
    print_info "Running $description..."
    echo "Command: RUSTFLAGS=\"$RUSTFLAGS\" cargo bench --bench $bench_name"
    
    if RUSTFLAGS="$RUSTFLAGS" cargo bench --bench "$bench_name"; then
        print_success "$description completed"
    else
        print_error "$description failed"
        return 1
    fi
}

# Function to monitor memory usage
monitor_memory() {
    local pid=$1
    local output_file="memory_usage.log"
    
    print_info "Monitoring memory usage (PID: $pid)"
    
    (
        echo "timestamp,rss_mb,vms_mb,cpu_percent" > "$output_file"
        while kill -0 "$pid" 2>/dev/null; do
            local stats=$(ps -p "$pid" -o rss=,vsz=,%cpu= 2>/dev/null || echo "0 0 0")
            local rss_mb=$(($(echo "$stats" | awk '{print $1}') / 1024))
            local vms_mb=$(($(echo "$stats" | awk '{print $2}') / 1024))
            local cpu_percent=$(echo "$stats" | awk '{print $3}')
            local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
            
            echo "$timestamp,$rss_mb,$vms_mb,$cpu_percent" >> "$output_file"
            sleep 1
        done
    ) &
    
    local monitor_pid=$!
    echo "$monitor_pid" > .monitor_pid
}

# Function to run quick benchmarks
run_quick_benchmarks() {
    print_info "Running quick benchmark suite (estimated time: 5-10 minutes)"
    
    # Basic operations only
    RUSTFLAGS="$RUSTFLAGS" cargo bench --bench database_bench -- \
        --filter "basic_operations|mixed_workload" 2>&1 | tee quick_results.log
    
    print_success "Quick benchmarks completed"
}

# Function to run standard benchmarks
run_standard_benchmarks() {
    print_info "Running standard benchmark suite (estimated time: 30-60 minutes)"
    
    run_benchmark "database_bench" "Core database operations"
    
    print_success "Standard benchmarks completed"
}

# Function to run comprehensive benchmarks
run_comprehensive_benchmarks() {
    print_info "Running comprehensive benchmark suite (estimated time: 2-4 hours)"
    
    run_benchmark "database_bench" "Core database operations"
    run_benchmark "large_scale_bench" "Large scale operations"
    run_benchmark "comprehensive_bench" "Mixed-size comprehensive tests"
    
    print_success "Comprehensive benchmarks completed"
}

# Function to run stress tests
run_stress_benchmarks() {
    print_info "Running stress test suite (estimated time: 4-8 hours)"
    
    # Set memory limits for stress testing
    export MALLOC_ARENA_MAX=2
    export MALLOC_MMAP_THRESHOLD_=131072
    
    run_benchmark "large_scale_bench" "Large scale stress tests"
    run_benchmark "comprehensive_bench" "Memory stress tests"
    
    unset MALLOC_ARENA_MAX
    unset MALLOC_MMAP_THRESHOLD_
    
    print_success "Stress benchmarks completed"
}

# Function to generate summary report
generate_summary() {
    local start_time=$1
    local end_time=$2
    local duration=$((end_time - start_time))
    
    print_info "Generating benchmark summary..."
    
    cat > benchmark_summary.md << EOF
# ZVerse Benchmark Results

**Date:** $(date)
**Duration:** ${duration} seconds
**System:** $(uname -a)
**CPU:** $(if [[ "$OSTYPE" == "darwin"* ]]; then sysctl -n machdep.cpu.brand_string; else grep 'model name' /proc/cpuinfo | head -1 | cut -d: -f2 | xargs; fi)
**Memory:** $(if command -v free >/dev/null 2>&1; then free -h | grep '^Mem:' | awk '{print $2}'; elif [[ "$OSTYPE" == "darwin"* ]]; then echo "$(( $(sysctl -n hw.memsize) / 1024 / 1024 / 1024 ))GB"; else echo "Unknown"; fi)
**Rust Version:** $(rustc --version)

## Configuration
- Benchmark Type: $BENCH_TYPE
- Threads: $THREADS
- RUSTFLAGS: $RUSTFLAGS

## Results Location
Results are available in: $OUTPUT_DIR

EOF

    if [[ -f "memory_usage.log" ]]; then
        echo "## Memory Usage" >> benchmark_summary.md
        if [[ -s "memory_usage.log" ]]; then
            echo "Peak memory usage: $(sort -t, -k2 -nr memory_usage.log | head -1 | cut -d, -f2) MB" >> benchmark_summary.md
        else
            echo "Memory usage monitoring was enabled but no data was collected." >> benchmark_summary.md
        fi
    fi

    print_success "Summary report generated: benchmark_summary.md"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--type)
            BENCH_TYPE="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -j|--threads)
            THREADS="$2"
            shift 2
            ;;
        -m|--memory-check)
            MEMORY_CHECK=1
            shift
            ;;
        -p|--profile)
            RUSTFLAGS="$RUSTFLAGS -g"
            shift
            ;;
        -c|--compare)
            COMPARE_BASELINE="$2"
            shift 2
            ;;
        -f|--format)
            OUTPUT_FORMAT="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate benchmark type
case $BENCH_TYPE in
    quick|standard|comprehensive|stress|all)
        ;;
    *)
        print_error "Invalid benchmark type: $BENCH_TYPE"
        show_usage
        exit 1
        ;;
esac

# Main execution
main() {
    local start_time=$(date +%s)
    
    print_info "Starting ZVerse benchmarks"
    print_info "Benchmark type: $BENCH_TYPE"
    print_info "Using $THREADS threads"
    print_info "Output directory: $OUTPUT_DIR"
    
    # Check requirements
    check_requirements
    
    # Ensure we're in the right directory
    if [[ ! -f "Cargo.toml" ]] || ! grep -q "name = \"zverse\"" Cargo.toml; then
        print_error "Must be run from the zverse project root directory"
        exit 1
    fi
    
    # Clean previous results
    print_info "Cleaning previous results..."
    cargo clean --release
    rm -rf "$OUTPUT_DIR"
    
    # Set environment variables
    export CARGO_TARGET_DIR="target"
    export RAYON_NUM_THREADS="$THREADS"
    
    # Start memory monitoring if requested
    if [[ $MEMORY_CHECK == 1 ]]; then
        monitor_memory $$ &
    fi
    
    # Run benchmarks based on type
    case $BENCH_TYPE in
        quick)
            run_quick_benchmarks
            ;;
        standard)
            run_standard_benchmarks
            ;;
        comprehensive)
            run_comprehensive_benchmarks
            ;;
        stress)
            run_stress_benchmarks
            ;;
        all)
            run_quick_benchmarks
            run_standard_benchmarks
            run_comprehensive_benchmarks
            run_stress_benchmarks
            ;;
    esac
    
    # Stop memory monitoring
    if [[ -f .monitor_pid ]]; then
        local monitor_pid=$(cat .monitor_pid)
        kill "$monitor_pid" 2>/dev/null || true
        rm -f .monitor_pid
    fi
    
    local end_time=$(date +%s)
    generate_summary "$start_time" "$end_time"
    
    print_success "All benchmarks completed successfully!"
    print_info "Total time: $((end_time - start_time)) seconds"
    print_info "Results available in: $OUTPUT_DIR"
}

# Run main function
main "$@"