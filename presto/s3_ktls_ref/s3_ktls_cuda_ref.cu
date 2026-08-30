/*
 * s3_ktls_cuda_ref.cu
 *
 * Database-engine reference implementation of this exact data path:
 *
 *   S3 HTTPS -> ENA/kernel TCP -> software RX kTLS
 *       -> bounded NUMA-local CUDA-pinned host slots
 *       -> same-reactor batched asynchronous H2D from those same slots
 *       -> final cudaMalloc() object allocations at exact byte offsets
 *
 * This is deliberately not an AWS CRT benchmark, a count-and-discard test, or
 * a production S3 library.  It shows one implementation, not a menu of old
 * experiments.  The only payload switch is --receive-only, which preserves
 * the receive path but omits CUDA copies to establish the network/kTLS ceiling.
 * The normal path always uses one same-reactor batcher: cudaMemcpyBatchAsync
 * for groups and cudaMemcpyAsync for a singleton tail, with CUDA events and
 * final cudaMalloc() object allocations.  There is no whole-object host
 * allocation, hot-path pin/unpin, Unified Memory, or payload staging memcpy.
 *
 * PERFORMANCE PREREQUISITES
 * -------------------------
 * 1. Use a current ENA driver.  The measurements accompanying this file used
 *    Amazon ENA 2.17.2g.  Print and retain the driver version with every run.
 * 2. Configure MTU 9001 on every selected ENA and verify that the complete
 *    route to the selected S3 frontends is jumbo-capable.  A local MTU of 9001
 *    is necessary but cannot by itself prove path MTU; the measured runner
 *    uses a previously validated frontend-IP set while retaining normal Host,
 *    SNI, certificate verification, and SigV4 semantics.
 * 3. Associate an available regional S3 gateway endpoint with the effective
 *    route table for every selected ENA subnet.  When its instance role has
 *    EC2 read permissions, the companion runner verifies and reports the
 *    control-plane association and active prefix-list route.  This check is
 *    advisory because a data-plane benchmark role need not have EC2 access.
 * 4. Set ENA queue counts and pin active Tx-Rx IRQs outside this process.  IRQ
 *    CPUs and reactor CPUs must not overlap.  Queue count and connection count
 *    are instance/workload tuning parameters, not portable constants.
 * 5. Preserve TCP receive autotuning.  This program never sets SO_RCVBUF.
 * 6. Use CUDA 13.x, a kernel with TLS 1.3 RX kTLS, sufficient memlock/NOFILE
 *    limits, an instance role or environment credentials, and explicit
 *    NIC/GPU/CPU topology in --lane.
 *
 * EXPERIMENTAL ENA RX PAGE OPTIMIZATION
 * -------------------------------------
 * The published g7e.48xlarge measurements also used an experimental ENA
 * large_rx_page=1 patch: each RX allocation is an order-2 (16 KiB) compound
 * page, so a jumbo packet normally consumes one RX descriptor instead of
 * roughly three 4 KiB descriptors.  This reduces receive-side descriptor and
 * page-pool work, but it is not upstream, requires 4 KiB base pages, excludes
 * XDP/AF_XDP, and is not required for correctness.  Treat it as a separately
 * reported A/B.  If the driver exposes the module parameter, CONFIG prints
 * large_rx_page=enabled|disabled; otherwise it prints unavailable.
 *
 * IMPORTANT OPENSSL 3.x RX NOTE
 * --------------------------------
 * OpenSSL 3.0 can negotiate/authenticate TLS 1.3 and can enable TLS 1.3 kTLS
 * transmit, but it does not enable TLS 1.3 kTLS receive.  Its ordinary kTLS
 * receive path also reads plaintext into OpenSSL's record buffer before
 * SSL_read_ex() copies it to the caller.  That is not the direct placement
 * this reference is intended to measure.
 *
 * We therefore use OpenSSL for the handshake, certificate/hostname
 * validation, SNI, key logging, and TLS writes.  At the clean post-handshake
 * record boundary, this file derives the server application traffic key from
 * OpenSSL's key-log callback, installs Linux TLS_RX, and receives with
 * recvmsg() directly into the CUDA-pinned slot.  TLS_GET_RECORD_TYPE keeps
 * alerts and post-handshake messages off the HTTP payload path.  This is the
 * same essential direct-RX transition proven by the accompanying s2n work,
 * without adding s2n or another TLS library as a dependency.
 *
 * BUILD (CUDA 13.x / OpenSSL 3.x; no device kernels are compiled):
 *
 *   /usr/local/cuda/bin/nvcc -O3 -std=c++17 \
 *       -Xcompiler=-Wall,-Wextra,-Wconversion,-Wshadow \
 *       s3_ktls_cuda_ref.cu -o s3_ktls_cuda_ref \
 *       -lssl -lcrypto -lpthread
 *
 * The source invokes mbind(2) through SYS_mbind, so libnuma is intentionally
 * not a link dependency.  This preserves explicit NUMA placement on machines
 * that have the Linux headers but not the libnuma development package.
 *
 * REPRODUCE THE MEASURED 8-GPU / 4-ENA DEFAULT:
 *
 *   ./run_s3_ktls_cuda_ref.sh
 *
 * The companion runner builds this file, converts a saved catalog export to
 * the strict snapshot format below, installs the measured
 * C192/R16/Q12/256-KiB/512-MiB profile, pins IRQs, establishes the complete
 * TCP/TLS/kTLS connection pool, primes every socket with one bodyless signed
 * HEAD Object, performs two measured transfers, and restores machine state.
 * That runner is an explicitly host-specific profile; the C++ source
 * discovers and validates the topology named by --lane rather than assuming
 * GPU numbering.
 *
 * MANUAL BENCHMARK RUN: first inspect the program's printed PCI/NUMA mapping,
 * then replace NIC, GPU, and CPU_LIST below.  A NIC may appear in several lanes.
 *
 *   sudo -E ./s3_ktls_cuda_ref \
 *       --catalog-snapshot objects.tsv --region us-east-2 \
 *       --lane NIC0:GPU0:CPU_LIST0 --lane NIC1:GPU1:CPU_LIST1 \
 *       --connections-per-lane 192 \
 *       --slot-kib 256 --range-mib 64 --pinned-hwm-mib 512 \
 *       --h2d-batch 32
 *
 * CONNECTION-POOL MEASUREMENT CONTRACT
 * ------------------------------------
 * Before the first measured query, the program establishes every configured
 * TCP/TLS 1.3/RX-kTLS transport without HTTP, then sends one signed HEAD Object
 * on every retained socket.  PRECONNECT_RESULT proves transport setup moved
 * nothing; PRIME_RESULT proves the S3-ready pool consumed no Range task, object
 * body byte, pinned payload slot, or H2D copy.  This avoids a prior data scan
 * while removing the first-use S3 request path from the measurement.  Two
 * measured iterations are the default so variance is visible; later iterations
 * reuse the same pool and overwrite the same final GPU allocations.
 *
 * CATALOG SNAPSHOT
 * ----------------
 * This file deliberately models the metadata a database metastore already
 * has before query execution.  It is not an object-discovery input.
 * Every nonempty line must contain an exact, nonzero object size:
 *
 *   size=BYTES<TAB>etag="ETAG"<TAB>s3://bucket/key
 *   size=BYTES<TAB>s3://bucket/key
 *
 * The required size permits immediate GPU assignment, final-HBM allocation,
 * and Range planning with no HEAD request on the query path.  A catalog ETag
 * is signed into If-Match from the first GET.  If the metastore has only size,
 * the first Range response establishes the ETag snapshot and every concurrent
 * and subsequent response must match it.  Every response still must exactly
 * match the catalog size through Content-Range and Content-Length.
 *
 * FRONTEND-IP OVERRIDE (Host, SNI, and certificate name remain the DNS name):
 *
 *   sudo -E ./s3_ktls_cuda_ref ... --endpoint-ip-file s3_frontends.txt
 *
 * Normal output aggregates per-object, per-connection, and per-reactor
 * details.  Hot-path telemetry is per-reactor and cache-line isolated; the
 * reporting thread aggregates it.
 *
 * HOST-ONLY PARSER/CRYPTO SELF-TEST (does not initialize CUDA or use network):
 *
 *   ./s3_ktls_cuda_ref --self-test
 *
 * Environment credentials have priority.  If they are absent, a minimal
 * IMDSv2 instance-role lookup is attempted:
 * AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and optional AWS_SESSION_TOKEN.
 *
 * SO_BINDTODEVICE is mandatory on every S3 socket.  It normally requires
 * suitable privilege (hence sudo in the examples).  The process never changes
 * ENA queue counts, IRQ affinity, MTU, sysctls, or the ENA driver.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <cuda_runtime.h>

#if CUDART_VERSION < 13000
#error "s3_ktls_cuda_ref requires CUDA 13 for cudaMemcpyBatchAsync"
#endif

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/opensslv.h>
#include <openssl/params.h>
#include <openssl/ssl.h>
#include <openssl/x509_vfy.h>
#include <openssl/core_names.h>

#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <linux/mempolicy.h>
#include <linux/tls.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cctype>
#include <cinttypes>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <exception>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ref {

using Clock = std::chrono::steady_clock;
using Nanoseconds = std::chrono::nanoseconds;

constexpr uint64_t KiB = 1024ULL;
constexpr uint64_t MiB = 1024ULL * KiB;
constexpr uint64_t GiB = 1024ULL * MiB;
constexpr size_t kMaxHttpHeader = 64U * 1024U;
constexpr size_t kTlsPlaintextRecordMax = 16U * 1024U;
constexpr size_t kTlsControlBufferMax = 128U * 1024U;
constexpr const char *kEmptySha256 =
    "e3b0c44298fc1c149afbf4c8996fb924"
    "27ae41e4649b934ca495991b7852b855";

std::mutex g_print_mu;

template <typename... Ts>
void print_line(Ts &&...parts) {
    std::lock_guard<std::mutex> lock(g_print_mu);
    (std::cout << ... << std::forward<Ts>(parts)) << '\n';
    std::cout.flush();
}

class Error : public std::runtime_error {
public:
    explicit Error(const std::string &message) : std::runtime_error(message) {}
};

[[noreturn]] void fail(const std::string &message) { throw Error(message); }

std::string errno_string(int value = errno) {
    return std::string(std::strerror(value)) + " (errno=" +
           std::to_string(value) + ")";
}

std::string cuda_string(cudaError_t status) {
    const char *name = cudaGetErrorName(status);
    const char *text = cudaGetErrorString(status);
    return std::string(name != nullptr ? name : "cudaErrorUnknown") + ": " +
           (text != nullptr ? text : "unknown CUDA error");
}

void cuda_check(cudaError_t status, const char *what) {
    if (status != cudaSuccess) {
        fail(std::string(what) + ": " + cuda_string(status));
    }
}

void cuda_log_if_error(cudaError_t status, const std::string &what) {
    if (status != cudaSuccess) print_line("ERROR ", what, ": ", cuda_string(status));
}

std::string openssl_errors() {
    std::ostringstream out;
    bool first = true;
    for (unsigned long e = ERR_get_error(); e != 0; e = ERR_get_error()) {
        std::array<char, 256> buf{};
        ERR_error_string_n(e, buf.data(), buf.size());
        if (!first) out << " | ";
        first = false;
        out << buf.data();
    }
    return first ? "OpenSSL error stack empty" : out.str();
}

class Fd {
public:
    Fd() = default;
    explicit Fd(int value) : value_(value) {}
    ~Fd() { reset(); }
    Fd(const Fd &) = delete;
    Fd &operator=(const Fd &) = delete;
    Fd(Fd &&other) noexcept : value_(other.release()) {}
    Fd &operator=(Fd &&other) noexcept {
        if (this != &other) reset(other.release());
        return *this;
    }
    int get() const { return value_; }
    explicit operator bool() const { return value_ >= 0; }
    int release() {
        const int old = value_;
        value_ = -1;
        return old;
    }
    void reset(int value = -1) {
        if (value_ >= 0) (void)::close(value_);
        value_ = value;
    }
private:
    int value_ = -1;
};

template <typename Function>
class ScopeExit {
public:
    explicit ScopeExit(Function function) : function_(std::move(function)) {}
    ~ScopeExit() { if (active_) function_(); }
    ScopeExit(const ScopeExit &) = delete;
    ScopeExit &operator=(const ScopeExit &) = delete;
    void release() { active_ = false; }
private:
    Function function_;
    bool active_ = true;
};

template <typename Function>
ScopeExit<Function> make_scope_exit(Function function) {
    return ScopeExit<Function>(std::move(function));
}

std::string read_text_file(const std::string &path, bool required = true) {
    std::ifstream in(path);
    if (!in) {
        if (required) fail("cannot open " + path + ": " + errno_string());
        return {};
    }
    std::ostringstream out;
    out << in.rdbuf();
    if (!in.good() && !in.eof()) fail("cannot read " + path);
    return out.str();
}

std::string trim(std::string value) {
    auto is_space = [](unsigned char c) { return std::isspace(c) != 0; };
    while (!value.empty() && is_space(static_cast<unsigned char>(value.front())))
        value.erase(value.begin());
    while (!value.empty() && is_space(static_cast<unsigned char>(value.back())))
        value.pop_back();
    return value;
}

std::string lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return value;
}

bool starts_with(std::string_view value, std::string_view prefix) {
    return value.size() >= prefix.size() &&
           value.substr(0, prefix.size()) == prefix;
}

template <typename T>
T parse_unsigned(std::string_view text, const std::string &label) {
    static_assert(std::is_unsigned<T>::value, "T must be unsigned");
    if (text.empty()) fail("empty value for " + label);
    T result{};
    const char *begin = text.data();
    const char *end = begin + text.size();
    const auto parsed = std::from_chars(begin, end, result);
    if (parsed.ec != std::errc{} || parsed.ptr != end)
        fail("invalid unsigned integer for " + label + ": " +
             std::string(text));
    return result;
}

int parse_nonnegative_int(std::string_view text, const std::string &label) {
    const unsigned value = parse_unsigned<unsigned>(text, label);
    if (value > static_cast<unsigned>(INT_MAX)) fail(label + " is too large");
    return static_cast<int>(value);
}

uint64_t checked_mib(uint64_t mib, const std::string &label) {
    if (mib > std::numeric_limits<uint64_t>::max() / MiB)
        fail(label + " overflows bytes");
    return mib * MiB;
}

uint64_t checked_kib(uint64_t kib, const std::string &label) {
    if (kib > std::numeric_limits<uint64_t>::max() / KiB)
        fail(label + " overflows bytes");
    return kib * KiB;
}

std::vector<int> parse_cpu_list(const std::string &text) {
    std::set<int> values;
    size_t pos = 0;
    while (pos < text.size()) {
        const size_t comma = text.find(',', pos);
        const std::string token = text.substr(
            pos, comma == std::string::npos ? std::string::npos : comma - pos);
        const size_t dash = token.find('-');
        int first = 0;
        int last = 0;
        if (dash == std::string::npos) {
            first = last = parse_nonnegative_int(token, "CPU list");
        } else {
            first = parse_nonnegative_int(token.substr(0, dash), "CPU list");
            last = parse_nonnegative_int(token.substr(dash + 1), "CPU list");
            if (last < first) fail("descending CPU range: " + token);
        }
        for (int cpu = first; cpu <= last; ++cpu) {
            values.insert(cpu);
            if (cpu == INT_MAX) break;
        }
        if (comma == std::string::npos) break;
        pos = comma + 1;
    }
    if (values.empty()) fail("CPU list is empty");
    return {values.begin(), values.end()};
}

std::string join_ints(const std::vector<int> &values) {
    std::ostringstream out;
    for (size_t i = 0; i < values.size(); ++i) {
        if (i != 0) out << ',';
        out << values[i];
    }
    return out.str();
}

std::vector<std::string> list_directory(const std::string &path) {
    DIR *raw = ::opendir(path.c_str());
    if (raw == nullptr) return {};
    std::unique_ptr<DIR, int (*)(DIR *)> dir(raw, ::closedir);
    std::vector<std::string> result;
    while (dirent *entry = ::readdir(dir.get())) {
        const std::string name(entry->d_name);
        if (name != "." && name != "..") result.push_back(name);
    }
    std::sort(result.begin(), result.end());
    return result;
}

uint64_t read_u64_file(const std::string &path) {
    return parse_unsigned<uint64_t>(trim(read_text_file(path)), path);
}

int read_int_file(const std::string &path, int fallback = -1) {
    const std::string value = trim(read_text_file(path, false));
    if (value.empty()) return fallback;
    bool negative = value.front() == '-';
    const std::string_view digits(value.data() + (negative ? 1 : 0),
                                  value.size() - (negative ? 1U : 0U));
    const unsigned parsed = parse_unsigned<unsigned>(digits, path);
    if (parsed > static_cast<unsigned>(INT_MAX)) return fallback;
    return negative ? -static_cast<int>(parsed) : static_cast<int>(parsed);
}

uint64_t clock_time_ns(Clock::time_point time) {
    return static_cast<uint64_t>(std::chrono::duration_cast<Nanoseconds>(
        time.time_since_epoch()).count());
}

uint64_t now_ns() {
    return clock_time_ns(Clock::now());
}

double gbps(uint64_t bytes, double seconds) {
    return seconds > 0.0 ? static_cast<double>(bytes) * 8.0 / seconds / 1.0e9
                         : 0.0;
}

double decimal_gb(uint64_t bytes) {
    return static_cast<double>(bytes) / 1.0e9;
}

/* ================================================================
 * Configuration / CLI
 * ================================================================ */

struct LaneArg {
    std::string nic;
    int gpu = -1;
    std::vector<int> cpus;
};

struct Options {
    std::string catalog_snapshot;
    std::string region = "us-east-2";
    std::vector<LaneArg> lane_args;
    unsigned connections_per_lane = 192;
    unsigned h2d_batch_size = 32;
    uint64_t slot_bytes = 256 * KiB;
    uint64_t range_bytes = 64 * MiB;
    uint64_t pinned_hwm_bytes = 512 * MiB;
    uint64_t gpu_reserve_bytes = 2048 * MiB;
    unsigned max_retries = 3;
    unsigned iterations = 2;
    std::string endpoint_ip_file;
    bool receive_only = false;
    bool help = false;
    bool self_test = false;
};

void usage(const char *argv0) {
    std::cout
        << "Usage:\n"
        << "  " << argv0
        << " --catalog-snapshot FILE --region R --lane NIC:GPU:CPUS ...\n\n"
        << "Required inputs:\n"
        << "  --catalog-snapshot FILE   Metastore snapshot: size=BYTES<TAB>"
           "[etag=VALUE<TAB>]s3://...\n"
        << "  --lane NIC:GPU:CPU_LIST   Lane and reactor CPUs; repeatable\n"
        << "\nInstance-tuned sizing (g7e.48xlarge measurements shown as defaults):\n"
        << "  --connections-per-lane N  Persistent HTTP/1.1 connections (default 192)\n"
        << "  --h2d-batch N             Copies per H2D batch, 1-64 (default 32)\n"
        << "  --slot-kib N              Slot size in KiB (default 256)\n"
        << "  --range-mib N             S3 Range task size (default 64)\n"
        << "  --pinned-hwm-mib N        Strict total registered-host limit (default 512)\n"
        << "  --gpu-reserve-mib N       Free HBM retained per GPU (default 2048)\n"
        << "  --max-retries N           Whole-range retries (default 3)\n"
        << "\nQuery/pool execution:\n"
        << "  Transport-only preconnect is followed by one bodyless HEAD per socket.\n"
        << "  --iterations N            Measured in-process transfers (default 2)\n"
        << "  --region R                SigV4/S3 region (default us-east-2)\n"
        << "  --endpoint-ip-file FILE   One frontend IP per line\n"
        << "  --receive-only            DIAGNOSTIC: registered receive; skip CUDA copies\n"
        << "  --self-test               Host-only parser/crypto checks\n"
        << "  --help\n";
}

LaneArg parse_lane_arg(const std::string &text) {
    const size_t first = text.find(':');
    const size_t second = first == std::string::npos
                              ? std::string::npos
                              : text.find(':', first + 1);
    if (first == std::string::npos || second == std::string::npos ||
        first == 0 || second == first + 1 || second + 1 >= text.size()) {
        fail("invalid --lane; expected NIC:GPU:CPU_LIST, got " + text);
    }
    LaneArg lane;
    lane.nic = text.substr(0, first);
    lane.gpu = parse_nonnegative_int(
        std::string_view(text).substr(first + 1, second - first - 1), "lane GPU");
    lane.cpus = parse_cpu_list(text.substr(second + 1));
    return lane;
}

Options parse_cli(int argc, char **argv) {
    Options opt;
    auto need = [&](int &i, const std::string &name) -> std::string {
        if (++i >= argc) fail("missing value for " + name);
        return argv[i];
    };
    for (int i = 1; i < argc; ++i) {
        const std::string arg(argv[i]);
        if (arg == "--catalog-snapshot") opt.catalog_snapshot = need(i, arg);
        else if (arg == "--region") opt.region = need(i, arg);
        else if (arg == "--lane") opt.lane_args.push_back(parse_lane_arg(need(i, arg)));
        else if (arg == "--connections-per-lane")
            opt.connections_per_lane = parse_unsigned<unsigned>(need(i, arg), arg);
        else if (arg == "--h2d-batch")
            opt.h2d_batch_size = parse_unsigned<unsigned>(need(i, arg), arg);
        else if (arg == "--slot-kib")
            opt.slot_bytes = checked_kib(parse_unsigned<uint64_t>(need(i, arg), arg), arg);
        else if (arg == "--range-mib")
            opt.range_bytes = checked_mib(parse_unsigned<uint64_t>(need(i, arg), arg), arg);
        else if (arg == "--pinned-hwm-mib")
            opt.pinned_hwm_bytes = checked_mib(parse_unsigned<uint64_t>(need(i, arg), arg), arg);
        else if (arg == "--gpu-reserve-mib")
            opt.gpu_reserve_bytes = checked_mib(parse_unsigned<uint64_t>(need(i, arg), arg), arg);
        else if (arg == "--max-retries")
            opt.max_retries = parse_unsigned<unsigned>(need(i, arg), arg);
        else if (arg == "--iterations")
            opt.iterations = parse_unsigned<unsigned>(need(i, arg), arg);
        else if (arg == "--endpoint-ip-file") opt.endpoint_ip_file = need(i, arg);
        else if (arg == "--receive-only") opt.receive_only = true;
        else if (arg == "--help" || arg == "-h") opt.help = true;
        else if (arg == "--self-test") opt.self_test = true;
        else fail("unknown option: " + arg);
    }
    if (opt.help || opt.self_test) return opt;
    if (opt.connections_per_lane == 0)
        fail("--connections-per-lane must be nonzero");
    if (opt.iterations == 0)
        fail("--iterations must be nonzero");
    if (opt.h2d_batch_size == 0 || opt.h2d_batch_size > 64)
        fail("--h2d-batch must be in the range 1-64");
    if (opt.slot_bytes == 0 || opt.range_bytes == 0 || opt.pinned_hwm_bytes == 0)
        fail("slot, range, and pinned-HWM sizes must be nonzero");
    if (opt.slot_bytes < kTlsPlaintextRecordMax)
        fail("staging slots must be at least 16 KiB so direct kTLS receive "
             "can accept any complete TLS plaintext record");
    if (opt.slot_bytes > opt.pinned_hwm_bytes)
        fail("one staging slot exceeds --pinned-hwm-mib");
    if (opt.catalog_snapshot.empty()) fail("--catalog-snapshot is required");
    if (opt.lane_args.empty()) fail("at least one explicit --lane is required");
    if (opt.region.empty()) fail("--region must not be empty");
    if (!std::all_of(opt.region.begin(), opt.region.end(), [](unsigned char c) {
            return std::isalnum(c) != 0 || c == '-';
        }))
        fail("--region contains characters invalid in an S3 hostname");
    return opt;
}

/* ================================================================
 * Topology discovery and lane validation
 * ================================================================ */

struct NicInfo {
    std::string name;
    int numa = -1;
    uint64_t mtu = 0;
    unsigned rx_queues = 0;
    unsigned tx_queues = 0;
    bool has_pci_device = false;
    std::string driver;
    std::string driver_version;
    std::string large_rx_page;
};

struct GpuInfo {
    int id = -1;
    std::string name;
    std::string pci_bus_id;
    int numa = -1;
    uint64_t total_bytes = 0;
    uint64_t free_bytes = 0;
};

struct CpuInfo {
    std::map<int, std::vector<int>> by_node;
    std::set<int> allowed;
};

unsigned count_queue_dirs(const std::string &nic, std::string_view prefix) {
    unsigned count = 0;
    for (const std::string &name : list_directory("/sys/class/net/" + nic + "/queues"))
        if (starts_with(name, prefix)) ++count;
    return count;
}

std::string normalize_bool_module_parameter(std::string value) {
    value = lower(trim(std::move(value)));
    if (value == "1" || value == "y" || value == "yes" ||
        value == "true" || value == "on")
        return "enabled";
    if (value == "0" || value == "n" || value == "no" ||
        value == "false" || value == "off")
        return "disabled";
    return value.empty() ? "unavailable" : "unknown(" + value + ")";
}

std::vector<NicInfo> discover_nics() {
    std::vector<NicInfo> result;
    for (const std::string &name : list_directory("/sys/class/net")) {
        if (name == "lo") continue;
        NicInfo nic;
        nic.name = name;
        const std::string base = "/sys/class/net/" + name;
        nic.numa = read_int_file(base + "/device/numa_node", -1);
        const std::string mtu = trim(read_text_file(base + "/mtu", false));
        if (!mtu.empty()) nic.mtu = parse_unsigned<uint64_t>(mtu, base + "/mtu");
        nic.rx_queues = count_queue_dirs(name, "rx-");
        nic.tx_queues = count_queue_dirs(name, "tx-");
        struct stat st {};
        nic.has_pci_device = ::stat((base + "/device").c_str(), &st) == 0;
        std::array<char, PATH_MAX> link{};
        const ssize_t link_size = ::readlink((base + "/device/driver").c_str(),
                                             link.data(), link.size() - 1);
        if (link_size > 0) {
            link[static_cast<size_t>(link_size)] = '\0';
            const std::string path(link.data());
            const size_t slash = path.find_last_of('/');
            nic.driver = slash == std::string::npos ? path : path.substr(slash + 1);
        } else {
            nic.driver = "unknown";
        }
        if (nic.driver != "unknown") {
            nic.driver_version = trim(read_text_file(
                "/sys/module/" + nic.driver + "/version", false));
            const std::string large_rx_page = read_text_file(
                "/sys/module/" + nic.driver +
                    "/parameters/large_rx_page",
                false);
            nic.large_rx_page = normalize_bool_module_parameter(large_rx_page);
        } else {
            nic.large_rx_page = "unavailable";
        }
        if (nic.driver_version.empty()) nic.driver_version = "unavailable";
        result.push_back(std::move(nic));
    }
    return result;
}

std::string normalize_pci_id(std::string id) {
    id = lower(trim(std::move(id)));
    // cudaDeviceGetPCIBusId normally includes the domain.  Add it for older
    // runtimes so the value maps directly to /sys/bus/pci/devices.
    if (std::count(id.begin(), id.end(), ':') == 1) id = "0000:" + id;
    return id;
}

std::vector<GpuInfo> discover_gpus(int &driver_version, int &runtime_version) {
    cuda_check(cudaDriverGetVersion(&driver_version), "cudaDriverGetVersion");
    cuda_check(cudaRuntimeGetVersion(&runtime_version), "cudaRuntimeGetVersion");
    int count = 0;
    cuda_check(cudaGetDeviceCount(&count), "cudaGetDeviceCount");
    if (count <= 0) fail("CUDA reports no GPUs");
    std::vector<GpuInfo> result;
    result.reserve(static_cast<size_t>(count));
    for (int id = 0; id < count; ++id) {
        cudaDeviceProp prop{};
        cuda_check(cudaGetDeviceProperties(&prop, id), "cudaGetDeviceProperties");
        std::array<char, 32> pci{};
        cuda_check(cudaDeviceGetPCIBusId(pci.data(), static_cast<int>(pci.size()), id),
                   "cudaDeviceGetPCIBusId");
        cuda_check(cudaSetDevice(id), "cudaSetDevice(topology)");
        size_t free_bytes = 0;
        size_t total_bytes = 0;
        cuda_check(cudaMemGetInfo(&free_bytes, &total_bytes), "cudaMemGetInfo(topology)");
        GpuInfo gpu;
        gpu.id = id;
        gpu.name = prop.name;
        gpu.pci_bus_id = normalize_pci_id(pci.data());
        gpu.numa = read_int_file(
            "/sys/bus/pci/devices/" + gpu.pci_bus_id + "/numa_node", -1);
        gpu.total_bytes = static_cast<uint64_t>(total_bytes);
        gpu.free_bytes = static_cast<uint64_t>(free_bytes);
        result.push_back(std::move(gpu));
    }
    return result;
}

CpuInfo discover_cpus() {
    CpuInfo info;
    cpu_set_t allowed_set;
    CPU_ZERO(&allowed_set);
    if (::sched_getaffinity(0, sizeof(allowed_set), &allowed_set) != 0)
        fail("sched_getaffinity: " + errno_string());
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu)
        if (CPU_ISSET(cpu, &allowed_set)) info.allowed.insert(cpu);

    const std::string node_root = "/sys/devices/system/node";
    for (const std::string &entry : list_directory(node_root)) {
        if (!starts_with(entry, "node") || entry.size() <= 4) continue;
        const std::string suffix = entry.substr(4);
        if (!std::all_of(suffix.begin(), suffix.end(), [](unsigned char c) {
                return std::isdigit(c) != 0;
            })) continue;
        const int node = parse_nonnegative_int(suffix, "NUMA node");
        const std::string cpulist = trim(
            read_text_file(node_root + "/" + entry + "/cpulist", false));
        if (cpulist.empty()) continue;
        for (int cpu : parse_cpu_list(cpulist))
            if (info.allowed.count(cpu) != 0) info.by_node[node].push_back(cpu);
    }
    if (info.by_node.empty()) {
        info.by_node[0] = {info.allowed.begin(), info.allowed.end()};
    }
    return info;
}

const NicInfo &find_nic(const std::vector<NicInfo> &nics, const std::string &name) {
    const auto it = std::find_if(nics.begin(), nics.end(), [&](const NicInfo &n) {
        return n.name == name;
    });
    if (it == nics.end()) fail("NIC does not exist: " + name);
    return *it;
}

const GpuInfo &find_gpu(const std::vector<GpuInfo> &gpus, int id) {
    const auto it = std::find_if(gpus.begin(), gpus.end(), [&](const GpuInfo &g) {
        return g.id == id;
    });
    if (it == gpus.end()) fail("GPU does not exist: " + std::to_string(id));
    return *it;
}

int cpu_numa_node(const CpuInfo &cpus, int cpu) {
    for (const auto &[node, values] : cpus.by_node)
        if (std::find(values.begin(), values.end(), cpu) != values.end()) return node;
    return -1;
}

struct Lane {
    int id = -1;
    std::string nic;
    int nic_numa = -1;
    uint64_t nic_mtu = 0;
    unsigned nic_rx_queues = 0;
    unsigned nic_tx_queues = 0;
    int gpu = -1;
    int gpu_numa = -1;
    int numa = -1;
    std::vector<int> reactor_cpus;
    unsigned connections = 0;
    size_t slots = 0;
};

std::vector<Lane> build_lanes(const Options &opt,
                              const std::vector<NicInfo> &nics,
                              const std::vector<GpuInfo> &gpus,
                              const CpuInfo &cpus) {
    std::set<int> used_cpus;
    std::vector<Lane> lanes;
    lanes.reserve(opt.lane_args.size());
    for (size_t i = 0; i < opt.lane_args.size(); ++i) {
        const LaneArg &arg = opt.lane_args[i];
        const size_t lane_reactor_count = arg.cpus.size();
        if (opt.connections_per_lane < lane_reactor_count)
            fail("--connections-per-lane must be at least the reactor count "
                 "for lane " + std::to_string(i));
        const NicInfo &nic = find_nic(nics, arg.nic);
        const GpuInfo &gpu = find_gpu(gpus, arg.gpu);
        if (::if_nametoindex(arg.nic.c_str()) == 0)
            fail("NIC has no interface index: " + arg.nic);
        if (nic.driver != "ena")
            fail("selected NIC is not driven by ENA: " + arg.nic);
        if (nic.mtu < 9001)
            fail("selected NIC " + arg.nic + " has MTU " +
                 std::to_string(nic.mtu) +
                 "; this reference requires local MTU 9001 and a validated "
                 "end-to-end jumbo path");
        if (nic.numa < 0 || gpu.numa < 0)
            fail("selected NIC/GPU must expose NUMA topology: lane " +
                 std::to_string(i));
        if (nic.numa != gpu.numa)
            fail("selected NIC/GPU are cross-NUMA in lane " +
                 std::to_string(i));
        Lane lane;
        lane.id = static_cast<int>(i);
        lane.nic = arg.nic;
        lane.nic_numa = nic.numa;
        lane.nic_mtu = nic.mtu;
        lane.nic_rx_queues = nic.rx_queues;
        lane.nic_tx_queues = nic.tx_queues;
        lane.gpu = arg.gpu;
        lane.gpu_numa = gpu.numa;
        lane.connections = opt.connections_per_lane;
        lane.reactor_cpus = arg.cpus;
        lane.numa = lane.nic_numa;
        for (int cpu : lane.reactor_cpus) {
            if (cpus.allowed.count(cpu) == 0)
                fail("reactor CPU " + std::to_string(cpu) +
                     " is outside this process's allowed affinity mask");
            if (cpu_numa_node(cpus, cpu) != lane.numa)
                fail("reactor CPU " + std::to_string(cpu) +
                     " is not NUMA-local to lane " + std::to_string(i));
            if (!used_cpus.insert(cpu).second)
                fail("reactor CPU is reused by multiple lanes: " + std::to_string(cpu));
        }
        lanes.push_back(std::move(lane));
    }

    uint64_t reactor_count = 0;
    for (const Lane &lane : lanes)
        reactor_count += static_cast<uint64_t>(lane.reactor_cpus.size());
    if (reactor_count == 0) fail("no reactors configured");
    const uint64_t slots_per_reactor =
        opt.pinned_hwm_bytes / opt.slot_bytes / reactor_count;
    if (slots_per_reactor < 2)
        fail("pinned HWM permits fewer than two slots per reactor");
    if (slots_per_reactor > std::numeric_limits<size_t>::max())
        fail("slot count is too large for this process");
    const uint64_t requested_slots = slots_per_reactor * reactor_count;
    if (requested_slots > std::numeric_limits<uint64_t>::max() / opt.slot_bytes)
        fail("pinned staging size overflows");
    const uint64_t requested_bytes = requested_slots * opt.slot_bytes;
    if (requested_bytes > opt.pinned_hwm_bytes) {
        fail("requested slots require " + std::to_string(requested_bytes) +
             " bytes, exceeding pinned HWM " +
             std::to_string(opt.pinned_hwm_bytes));
    }
    for (Lane &lane : lanes)
        lane.slots = static_cast<size_t>(slots_per_reactor) *
                     lane.reactor_cpus.size();
    return lanes;
}

void pin_this_thread(int cpu, const std::string &label) {
    if (cpu < 0 || cpu >= CPU_SETSIZE) fail(label + ": CPU outside CPU_SETSIZE");
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    const int rc = ::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set);
    if (rc != 0) fail(label + ": pthread_setaffinity_np: " + errno_string(rc));
}

/* ================================================================
 * Object input, endpoint names, and address resolution
 * ================================================================ */

int hex_nibble(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

std::string percent_decode(std::string_view encoded) {
    std::string out;
    out.reserve(encoded.size());
    for (size_t i = 0; i < encoded.size(); ++i) {
        if (encoded[i] == '%') {
            if (i + 2 >= encoded.size()) fail("truncated %XX escape in S3 key");
            const int hi = hex_nibble(encoded[i + 1]);
            const int lo = hex_nibble(encoded[i + 2]);
            if (hi < 0 || lo < 0) fail("invalid %XX escape in S3 key");
            out.push_back(static_cast<char>((hi << 4) | lo));
            i += 2;
        } else {
            out.push_back(encoded[i]);
        }
    }
    return out;
}

std::string uri_encode_path(std::string_view raw_key) {
    static constexpr char hex[] = "0123456789ABCDEF";
    std::string out;
    out.reserve(raw_key.size() + 1);
    out.push_back('/');
    for (unsigned char c : raw_key) {
        const bool unreserved = (c >= 'A' && c <= 'Z') ||
                                (c >= 'a' && c <= 'z') ||
                                (c >= '0' && c <= '9') || c == '-' ||
                                c == '_' || c == '.' || c == '~';
        if (unreserved || c == '/') {
            out.push_back(static_cast<char>(c));
        } else {
            out.push_back('%');
            out.push_back(hex[c >> 4]);
            out.push_back(hex[c & 0x0f]);
        }
    }
    return out;
}

struct CatalogObjectSpec {
    uint64_t size = 0;
    std::optional<std::string> etag;
    std::string bucket;
    std::string raw_key;
    std::string display_uri;
};

CatalogObjectSpec parse_s3_uri(std::string uri) {
    if (!starts_with(uri, "s3://"))
        fail("catalog snapshot URI must begin s3://: " + uri);
    const size_t slash = uri.find('/', 5);
    if (slash == std::string::npos || slash == 5)
        fail("S3 URI must contain bucket and key: " + uri);
    CatalogObjectSpec spec;
    spec.bucket = uri.substr(5, slash - 5);
    spec.raw_key = percent_decode(uri.substr(slash + 1));
    spec.display_uri = std::move(uri);
    if (spec.raw_key.empty()) fail("empty S3 key is not supported");
    return spec;
}

CatalogObjectSpec parse_catalog_snapshot_line(const std::string &line,
                                              size_t lineno) {
    std::vector<std::string> fields;
    size_t begin = 0;
    while (true) {
        const size_t tab = line.find('\t', begin);
        fields.push_back(trim(line.substr(
            begin, tab == std::string::npos ? std::string::npos : tab - begin)));
        if (tab == std::string::npos) break;
        begin = tab + 1;
    }
    if (fields.empty() || fields.back().empty())
        fail("catalog snapshot line " + std::to_string(lineno) +
             " has an empty URI");

    std::optional<std::string> etag;
    if (fields.size() != 2 && fields.size() != 3)
        fail("catalog snapshot line " + std::to_string(lineno) +
             " must be size=BYTES<TAB>[etag=VALUE<TAB>]s3://...");
    if (!starts_with(fields[0], "size="))
        fail("catalog snapshot line " + std::to_string(lineno) +
             " must begin with size=BYTES");
    const uint64_t size = parse_unsigned<uint64_t>(
        trim(fields[0].substr(5)),
        "catalog object size at line " + std::to_string(lineno));
    if (size == 0)
        fail("catalog snapshot excludes zero-byte objects at line " +
             std::to_string(lineno));
    if (fields.size() == 3) {
        if (!starts_with(fields[1], "etag="))
            fail("catalog snapshot line " + std::to_string(lineno) +
                 " has an unknown metadata column: " + fields[1]);
        std::string value = trim(fields[1].substr(5));
        if (value.empty() || value.find_first_of("\r\n") != std::string::npos)
            fail("invalid catalog ETag at line " + std::to_string(lineno));
        etag = std::move(value);
    }

    CatalogObjectSpec spec = parse_s3_uri(fields.back());
    spec.size = size;
    spec.etag = std::move(etag);
    return spec;
}

std::vector<CatalogObjectSpec> read_catalog_snapshot(const Options &opt) {
    std::ifstream in(opt.catalog_snapshot);
    if (!in)
        fail("cannot open catalog snapshot " + opt.catalog_snapshot + ": " +
             errno_string());
    std::vector<CatalogObjectSpec> result;
    std::string line;
    size_t lineno = 0;
    while (std::getline(in, line)) {
        ++lineno;
        line = trim(std::move(line));
        if (line.empty() || line.front() == '#') continue;
        result.push_back(parse_catalog_snapshot_line(line, lineno));
    }
    if (!in.eof())
        fail("error reading catalog snapshot " + opt.catalog_snapshot);
    if (result.empty()) fail("catalog snapshot contains no objects");
    return result;
}

struct SocketAddress {
    sockaddr_storage storage{};
    socklen_t length = 0;
    std::string numeric;
};

struct Endpoint {
    std::string hostname;
    std::vector<SocketAddress> addresses;
};

std::vector<SocketAddress> resolve_addresses(const std::string &hostname,
                                             const std::vector<std::string> &overrides) {
    std::vector<SocketAddress> result;
    std::vector<std::string> names = overrides.empty()
                                         ? std::vector<std::string>{hostname}
                                         : overrides;
    for (const std::string &name : names) {
        addrinfo hints{};
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;
        hints.ai_family = AF_UNSPEC;
        if (!overrides.empty()) hints.ai_flags = AI_NUMERICHOST;
        addrinfo *raw = nullptr;
        const int rc = ::getaddrinfo(name.c_str(), "443", &hints, &raw);
        if (rc != 0)
            fail("getaddrinfo(" + name + "): " + ::gai_strerror(rc));
        std::unique_ptr<addrinfo, void (*)(addrinfo *)> list(raw, ::freeaddrinfo);
        for (addrinfo *it = list.get(); it != nullptr; it = it->ai_next) {
            if (it->ai_addrlen > sizeof(sockaddr_storage)) continue;
            SocketAddress address;
            std::memcpy(&address.storage, it->ai_addr,
                        static_cast<size_t>(it->ai_addrlen));
            address.length = static_cast<socklen_t>(it->ai_addrlen);
            std::array<char, NI_MAXHOST> numeric{};
            const int ni = ::getnameinfo(it->ai_addr, it->ai_addrlen,
                                         numeric.data(), numeric.size(), nullptr, 0,
                                         NI_NUMERICHOST);
            address.numeric = ni == 0 ? numeric.data() : name;
            const bool duplicate = std::any_of(result.begin(), result.end(),
                [&](const SocketAddress &existing) {
                    return existing.numeric == address.numeric;
                });
            if (!duplicate) result.push_back(std::move(address));
        }
    }
    if (result.empty()) fail("no usable addresses for " + hostname);
    return result;
}

std::vector<std::string> load_endpoint_overrides(const Options &opt) {
    std::vector<std::string> values;
    if (!opt.endpoint_ip_file.empty()) {
        std::ifstream in(opt.endpoint_ip_file);
        if (!in) fail("cannot open endpoint IP file " + opt.endpoint_ip_file);
        std::string line;
        while (std::getline(in, line)) {
            line = trim(std::move(line));
            if (!line.empty() && line.front() != '#') values.push_back(line);
        }
        if (!in.eof()) fail("error reading endpoint IP file");
    }
    std::sort(values.begin(), values.end());
    values.erase(std::unique(values.begin(), values.end()), values.end());
    return values;
}

/* ================================================================
 * Credentials and AWS SigV4
 * ================================================================ */

struct Credentials {
    std::string access_key;
    std::string secret_key;
    std::string session_token;
    std::string source;
    std::string expiration;
};

std::optional<std::string> getenv_nonempty(const char *name) {
    const char *value = std::getenv(name);
    if (value == nullptr || *value == '\0') return std::nullopt;
    return std::string(value);
}

bool wait_fd(int fd, short events, Clock::time_point deadline) {
    while (true) {
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - Clock::now());
        if (remaining.count() <= 0) return false;
        pollfd pfd{fd, events, 0};
        const int timeout = static_cast<int>(std::min<int64_t>(remaining.count(), INT_MAX));
        const int rc = ::poll(&pfd, 1, timeout);
        if (rc > 0) return (pfd.revents & (events | POLLERR | POLLHUP)) != 0;
        if (rc == 0) return false;
        if (errno != EINTR) return false;
    }
}

std::string imds_request(const std::string &method, const std::string &path,
                         const std::vector<std::pair<std::string, std::string>> &headers) {
    Fd fd(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0));
    if (!fd) fail("IMDS socket: " + errno_string());
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(80);
    if (::inet_pton(AF_INET, "169.254.169.254", &address.sin_addr) != 1)
        fail("internal inet_pton failure for IMDS");
    const auto deadline = Clock::now() + std::chrono::seconds(2);
    if (::connect(fd.get(), reinterpret_cast<sockaddr *>(&address), sizeof(address)) != 0) {
        if (errno != EINPROGRESS || !wait_fd(fd.get(), POLLOUT, deadline))
            fail("IMDS connect timed out or failed");
        int error = 0;
        socklen_t len = sizeof(error);
        if (::getsockopt(fd.get(), SOL_SOCKET, SO_ERROR, &error, &len) != 0 || error != 0)
            fail("IMDS connect: " + errno_string(error != 0 ? error : errno));
    }
    std::ostringstream request;
    request << method << ' ' << path << " HTTP/1.1\r\n"
            << "Host: 169.254.169.254\r\nConnection: close\r\n";
    for (const auto &[name, value] : headers)
        request << name << ": " << value << "\r\n";
    request << "Content-Length: 0\r\n\r\n";
    const std::string wire = request.str();
    size_t sent = 0;
    while (sent < wire.size()) {
        const ssize_t n = ::send(fd.get(), wire.data() + sent, wire.size() - sent,
                                 MSG_NOSIGNAL);
        if (n > 0) {
            sent += static_cast<size_t>(n);
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (!wait_fd(fd.get(), POLLOUT, deadline)) fail("IMDS send timeout");
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        fail("IMDS send: " + errno_string());
    }
    std::string response;
    response.reserve(8192);
    std::array<char, 4096> buf{};
    while (true) {
        const ssize_t n = ::recv(fd.get(), buf.data(), buf.size(), 0);
        if (n > 0) {
            if (response.size() + static_cast<size_t>(n) > 64U * 1024U)
                fail("IMDS response exceeds 64 KiB");
            response.append(buf.data(), static_cast<size_t>(n));
            continue;
        }
        if (n == 0) break;
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            if (!wait_fd(fd.get(), POLLIN, deadline)) fail("IMDS receive timeout");
            continue;
        }
        if (errno == EINTR) continue;
        fail("IMDS recv: " + errno_string());
    }
    const size_t split = response.find("\r\n\r\n");
    if (split == std::string::npos) fail("malformed IMDS HTTP response");
    const size_t line_end = response.find("\r\n");
    if (line_end == std::string::npos) fail("malformed IMDS status line");
    const std::string status_line = response.substr(0, line_end);
    const size_t first_space = status_line.find(' ');
    if (first_space == std::string::npos) fail("malformed IMDS status line");
    const int status = parse_nonnegative_int(
        status_line.substr(first_space + 1, 3), "IMDS HTTP status");
    if (status != 200) fail("IMDS returned HTTP " + std::to_string(status));
    return response.substr(split + 4);
}

std::string json_string_field(const std::string &json, const std::string &field) {
    const std::string needle = "\"" + field + "\"";
    size_t pos = json.find(needle);
    if (pos == std::string::npos) fail("IMDS credential JSON lacks " + field);
    pos = json.find(':', pos + needle.size());
    if (pos == std::string::npos) fail("malformed IMDS credential JSON");
    pos = json.find('"', pos + 1);
    if (pos == std::string::npos) fail("malformed IMDS credential JSON");
    ++pos;
    std::string out;
    bool escaped = false;
    for (; pos < json.size(); ++pos) {
        const char c = json[pos];
        if (escaped) {
            switch (c) {
                case '"': case '\\': case '/': out.push_back(c); break;
                case 'b': out.push_back('\b'); break;
                case 'f': out.push_back('\f'); break;
                case 'n': out.push_back('\n'); break;
                case 'r': out.push_back('\r'); break;
                case 't': out.push_back('\t'); break;
                default: fail("unsupported JSON escape in IMDS credentials");
            }
            escaped = false;
        } else if (c == '\\') {
            escaped = true;
        } else if (c == '"') {
            return out;
        } else {
            out.push_back(c);
        }
    }
    fail("unterminated IMDS JSON string for " + field);
}

Credentials load_credentials() {
    const auto access = getenv_nonempty("AWS_ACCESS_KEY_ID");
    const auto secret = getenv_nonempty("AWS_SECRET_ACCESS_KEY");
    if (access.has_value() != secret.has_value())
        fail("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set together");
    if (access) {
        Credentials result;
        result.access_key = *access;
        result.secret_key = *secret;
        result.session_token = getenv_nonempty("AWS_SESSION_TOKEN").value_or("");
        result.source = "environment";
        return result;
    }
    try {
        const std::string token = trim(imds_request(
            "PUT", "/latest/api/token",
            {{"X-aws-ec2-metadata-token-ttl-seconds", "21600"}}));
        if (token.empty()) fail("IMDSv2 returned an empty token");
        const auto token_header = std::vector<std::pair<std::string, std::string>>{
            {"X-aws-ec2-metadata-token", token}};
        const std::string role = trim(imds_request(
            "GET", "/latest/meta-data/iam/security-credentials/", token_header));
        if (role.empty() || role.find('\n') != std::string::npos)
            fail("IMDS returned zero or multiple role names");
        const std::string json = imds_request(
            "GET", "/latest/meta-data/iam/security-credentials/" + role,
            token_header);
        Credentials result;
        result.access_key = json_string_field(json, "AccessKeyId");
        result.secret_key = json_string_field(json, "SecretAccessKey");
        result.session_token = json_string_field(json, "Token");
        result.expiration = json_string_field(json, "Expiration");
        result.source = "IMDSv2:" + role;
        return result;
    } catch (const std::exception &e) {
        fail(std::string("credential lookup failed: ") + e.what());
    }
}

void validate_credentials(const Credentials &credentials) {
    if (credentials.access_key.empty() || credentials.secret_key.empty())
        fail("credentials are incomplete");
    if (credentials.access_key.find_first_of("\r\n") != std::string::npos ||
        credentials.secret_key.find_first_of("\r\n") != std::string::npos ||
        credentials.session_token.find_first_of("\r\n") != std::string::npos)
        fail("credential values containing CR/LF are rejected");
}

std::array<unsigned char, 32> sha256(std::string_view data) {
    std::array<unsigned char, 32> digest{};
    EVP_MD_CTX *raw = EVP_MD_CTX_new();
    if (raw == nullptr) fail("EVP_MD_CTX_new: " + openssl_errors());
    std::unique_ptr<EVP_MD_CTX, void (*)(EVP_MD_CTX *)> ctx(raw, EVP_MD_CTX_free);
    unsigned int length = 0;
    if (EVP_DigestInit_ex(ctx.get(), EVP_sha256(), nullptr) != 1 ||
        EVP_DigestUpdate(ctx.get(), data.data(), data.size()) != 1 ||
        EVP_DigestFinal_ex(ctx.get(), digest.data(), &length) != 1 ||
        length != digest.size()) {
        fail("SHA256 failed: " + openssl_errors());
    }
    return digest;
}

std::string hex_lower(const unsigned char *data, size_t size) {
    static constexpr char hex[] = "0123456789abcdef";
    std::string out(size * 2, '\0');
    for (size_t i = 0; i < size; ++i) {
        out[i * 2] = hex[data[i] >> 4];
        out[i * 2 + 1] = hex[data[i] & 0x0f];
    }
    return out;
}

std::array<unsigned char, 32> hmac_sha256(const void *key, size_t key_size,
                                          std::string_view data) {
    EVP_MAC *raw_mac = EVP_MAC_fetch(nullptr, "HMAC", nullptr);
    if (raw_mac == nullptr) fail("EVP_MAC_fetch(HMAC): " + openssl_errors());
    std::unique_ptr<EVP_MAC, void (*)(EVP_MAC *)> mac(raw_mac, EVP_MAC_free);
    EVP_MAC_CTX *raw_ctx = EVP_MAC_CTX_new(mac.get());
    if (raw_ctx == nullptr) fail("EVP_MAC_CTX_new: " + openssl_errors());
    std::unique_ptr<EVP_MAC_CTX, void (*)(EVP_MAC_CTX *)> ctx(raw_ctx,
                                                              EVP_MAC_CTX_free);
    char digest_name[] = "SHA256";
    OSSL_PARAM params[] = {
        OSSL_PARAM_construct_utf8_string(OSSL_MAC_PARAM_DIGEST, digest_name, 0),
        OSSL_PARAM_construct_end()};
    std::array<unsigned char, 32> out{};
    size_t out_size = 0;
    if (EVP_MAC_init(ctx.get(), static_cast<const unsigned char *>(key), key_size,
                     params) != 1 ||
        EVP_MAC_update(ctx.get(),
                       reinterpret_cast<const unsigned char *>(data.data()),
                       data.size()) != 1 ||
        EVP_MAC_final(ctx.get(), out.data(), &out_size, out.size()) != 1 ||
        out_size != out.size()) {
        fail("HMAC-SHA256 failed: " + openssl_errors());
    }
    return out;
}

std::string sha256_hex(std::string_view data) {
    const auto digest = sha256(data);
    return hex_lower(digest.data(), digest.size());
}

struct AmzTime {
    std::string full;
    std::string date;
};

AmzTime current_amz_time() {
    const std::time_t now = std::time(nullptr);
    std::tm utc{};
    if (::gmtime_r(&now, &utc) == nullptr) fail("gmtime_r failed");
    std::array<char, 32> full{};
    std::array<char, 16> date{};
    if (std::strftime(full.data(), full.size(), "%Y%m%dT%H%M%SZ", &utc) == 0 ||
        std::strftime(date.data(), date.size(), "%Y%m%d", &utc) == 0)
        fail("strftime failed");
    return {full.data(), date.data()};
}

struct RequestTarget {
    std::string hostname;
    std::string canonical_uri;
};

std::string make_signed_request(const std::string &method,
                                const RequestTarget &target,
                                const std::optional<std::pair<uint64_t, uint64_t>> &range,
                                const std::optional<std::string> &if_match,
                                const std::string &region,
                                const Credentials &credentials,
                                bool keep_alive) {
    const std::string range_value = range
        ? "bytes=" + std::to_string(range->first) + "-" +
              std::to_string(range->second)
        : std::string();

    const AmzTime timestamp = current_amz_time();
    std::vector<std::pair<std::string, std::string>> signed_headers{
        {"accept-encoding", "identity"},
        {"host", target.hostname},
    };
    if (if_match) signed_headers.push_back({"if-match", *if_match});
    if (range) signed_headers.push_back({"range", range_value});
    signed_headers.push_back({"x-amz-content-sha256", kEmptySha256});
    signed_headers.push_back({"x-amz-date", timestamp.full});
    if (!credentials.session_token.empty())
        signed_headers.push_back({"x-amz-security-token", credentials.session_token});
    std::sort(signed_headers.begin(), signed_headers.end());

    std::ostringstream canonical_headers;
    std::ostringstream signed_names;
    for (size_t i = 0; i < signed_headers.size(); ++i) {
        canonical_headers << signed_headers[i].first << ':'
                          << signed_headers[i].second << '\n';
        if (i != 0) signed_names << ';';
        signed_names << signed_headers[i].first;
    }
    const std::string canonical_request =
        method + "\n" + target.canonical_uri + "\n\n" +
        canonical_headers.str() + "\n" + signed_names.str() + "\n" +
        kEmptySha256;
    const std::string scope = timestamp.date + "/" + region + "/s3/aws4_request";
    const std::string string_to_sign = "AWS4-HMAC-SHA256\n" + timestamp.full +
                                       "\n" + scope + "\n" +
                                       sha256_hex(canonical_request);
    const std::string first_key = "AWS4" + credentials.secret_key;
    const auto date_key = hmac_sha256(first_key.data(), first_key.size(), timestamp.date);
    const auto region_key = hmac_sha256(date_key.data(), date_key.size(), region);
    const auto service_key = hmac_sha256(region_key.data(), region_key.size(), "s3");
    const auto signing_key = hmac_sha256(service_key.data(), service_key.size(),
                                         "aws4_request");
    const auto signature = hmac_sha256(signing_key.data(), signing_key.size(),
                                       string_to_sign);

    std::ostringstream request;
    request << method << ' ' << target.canonical_uri << " HTTP/1.1\r\n"
            << "Host: " << target.hostname << "\r\n"
            << "Accept-Encoding: identity\r\n";
    if (range) request << "Range: " << range_value << "\r\n";
    if (if_match) request << "If-Match: " << *if_match << "\r\n";
    request << "x-amz-content-sha256: " << kEmptySha256 << "\r\n"
            << "x-amz-date: " << timestamp.full << "\r\n";
    if (!credentials.session_token.empty())
        request << "x-amz-security-token: " << credentials.session_token << "\r\n";
    request << "Authorization: AWS4-HMAC-SHA256 Credential="
            << credentials.access_key << '/' << scope
            << ", SignedHeaders=" << signed_names.str()
            << ", Signature=" << hex_lower(signature.data(), signature.size()) << "\r\n"
            << "Connection: " << (keep_alive ? "keep-alive" : "close")
            << "\r\n\r\n";
    return request.str();
}

/* ================================================================
 * Minimal HTTP response parsing
 * ================================================================ */

struct ParsedResponse {
    int status = 0;
    std::string reason;
    std::vector<std::pair<std::string, std::string>> headers;
    std::optional<uint64_t> content_length;
    bool transfer_encoding_present = false;
    bool connection_close = false;

    std::optional<std::string> get(std::string_view name) const {
        const std::string wanted = lower(std::string(name));
        for (const auto &[key, value] : headers)
            if (key == wanted) return value;
        return std::nullopt;
    }
    size_t count(std::string_view name) const {
        const std::string wanted = lower(std::string(name));
        return static_cast<size_t>(std::count_if(headers.begin(), headers.end(),
            [&](const auto &entry) { return entry.first == wanted; }));
    }
};

bool http_header_has_token(const std::string &value, std::string_view wanted) {
    size_t pos = 0;
    while (pos <= value.size()) {
        const size_t comma = value.find(',', pos);
        const std::string token = lower(trim(value.substr(
            pos, comma == std::string::npos ? std::string::npos : comma - pos)));
        if (token == wanted) return true;
        if (comma == std::string::npos) break;
        pos = comma + 1;
    }
    return false;
}

uint64_t parse_decimal_exact(std::string_view text, const std::string &label) {
    return parse_unsigned<uint64_t>(text, label);
}

ParsedResponse parse_http_headers(const std::string &header) {
    if (header.size() < 4 || header.substr(header.size() - 4) != "\r\n\r\n")
        fail("HTTP header parser called without complete CRLF terminator");
    ParsedResponse response;
    size_t pos = 0;
    size_t end = header.find("\r\n", pos);
    if (end == std::string::npos) fail("HTTP response lacks a status line");
    const std::string status_line = header.substr(pos, end - pos);
    if (!starts_with(status_line, "HTTP/1.1 "))
        fail("expected HTTP/1.1 response, got: " + status_line);
    if (status_line.size() < 12 ||
        (status_line.size() > 12 && status_line[12] != ' '))
        fail("malformed HTTP status line: " + status_line);
    response.status = parse_nonnegative_int(
        std::string_view(status_line).substr(9, 3), "HTTP status");
    response.reason = status_line.size() > 13 ? status_line.substr(13) : "";
    pos = end + 2;
    while (pos < header.size()) {
        end = header.find("\r\n", pos);
        if (end == std::string::npos) fail("malformed HTTP header line");
        if (end == pos) break;
        const std::string line = header.substr(pos, end - pos);
        if (line.front() == ' ' || line.front() == '\t')
            fail("obsolete folded HTTP header is rejected");
        const size_t colon = line.find(':');
        if (colon == std::string::npos || colon == 0)
            fail("malformed HTTP header: " + line);
        const std::string name = lower(trim(line.substr(0, colon)));
        const std::string value = trim(line.substr(colon + 1));
        if (name == "content-length") {
            const uint64_t parsed = parse_decimal_exact(value, "Content-Length");
            if (response.content_length && *response.content_length != parsed)
                fail("conflicting Content-Length headers");
            response.content_length = parsed;
        } else if (name == "transfer-encoding") {
            response.transfer_encoding_present = true;
        } else if (name == "connection" && http_header_has_token(value, "close")) {
            response.connection_close = true;
        }
        response.headers.emplace_back(name, value);
        pos = end + 2;
    }
    return response;
}

class HeaderAccumulator {
public:
    HeaderAccumulator() { bytes_.reserve(16U * 1024U); }
    // Consume only through CRLFCRLF.  Bytes after the terminator are never
    // copied into this ordinary string; they stay in the CUDA-pinned slot and
    // are submitted directly to HBM using the returned body_offset.
    bool consume(const unsigned char *data, size_t size, size_t &body_offset) {
        if (complete_) fail("HTTP header accumulator reused after completion");
        for (size_t i = 0; i < size; ++i) {
            if (bytes_.size() >= kMaxHttpHeader)
                fail("HTTP response header exceeds 64 KiB");
            bytes_.push_back(static_cast<char>(data[i]));
            const size_t n = bytes_.size();
            if (n >= 4 && bytes_[n - 4] == '\r' && bytes_[n - 3] == '\n' &&
                bytes_[n - 2] == '\r' && bytes_[n - 1] == '\n') {
                complete_ = true;
                body_offset = i + 1;
                parsed_ = parse_http_headers(bytes_);
                return true;
            }
        }
        body_offset = size;
        return false;
    }

    const ParsedResponse &parsed() const {
        if (!complete_) fail("HTTP headers are not complete");
        return parsed_;
    }

    void reset() {
        bytes_.clear();
        complete_ = false;
        parsed_ = ParsedResponse{};
    }

private:
    std::string bytes_;
    bool complete_ = false;
    ParsedResponse parsed_;
};

struct ContentRange {
    uint64_t start = 0;
    uint64_t end = 0;
    uint64_t total = 0;
};

ContentRange parse_content_range(const std::string &value) {
    if (!starts_with(value, "bytes "))
        fail("invalid Content-Range unit: " + value);
    const size_t dash = value.find('-', 6);
    const size_t slash = dash == std::string::npos
                             ? std::string::npos
                             : value.find('/', dash + 1);
    if (dash == std::string::npos || slash == std::string::npos ||
        dash == 6 || slash == dash + 1 || slash + 1 >= value.size())
        fail("malformed Content-Range: " + value);
    ContentRange range;
    range.start = parse_decimal_exact(
        std::string_view(value).substr(6, dash - 6), "Content-Range start");
    range.end = parse_decimal_exact(
        std::string_view(value).substr(dash + 1, slash - dash - 1),
        "Content-Range end");
    range.total = parse_decimal_exact(
        std::string_view(value).substr(slash + 1), "Content-Range total");
    if (range.end < range.start) fail("descending Content-Range: " + value);
    return range;
}

std::string s3_diagnostic_headers(const ParsedResponse &response) {
    std::ostringstream out;
    bool first = true;
    for (const auto &[name, value] : response.headers) {
        if (starts_with(name, "x-amz-") || name == "location") {
            if (!first) out << ", ";
            first = false;
            out << name << '=' << value;
        }
    }
    return first ? "none" : out.str();
}

/* ================================================================
 * Process-wide counters, failure propagation, and /proc TLS stats
 * ================================================================ */

struct LaneCounters {
    std::atomic<uint64_t> body_bytes{0};
    std::atomic<uint64_t> active_connections{0};
};

struct GpuCounters {
    std::atomic<uint64_t> h2d_completed_bytes{0};
    std::atomic<uint64_t> outstanding_copies{0};
};

struct RunStats {
    // Data-path milestones live in per-reactor cache-line-isolated blocks.
    std::atomic<uint64_t> body_bytes{0};
    std::atomic<uint64_t> h2d_submitted_bytes{0};
    std::atomic<uint64_t> h2d_submitted_copies{0};
    std::atomic<uint64_t> h2d_completed_bytes{0};
    std::atomic<uint64_t> h2d_completed_copies{0};
    std::atomic<uint64_t> h2d_inline_batches{0};
    std::atomic<uint64_t> h2d_inline_event_queries{0};
    std::atomic<uint64_t> active_connections{0};
    std::atomic<uint64_t> completed_ranges{0};
    std::atomic<uint64_t> retries{0};
    std::atomic<uint64_t> reconnects{0};
    std::atomic<uint64_t> http_errors{0};
    std::atomic<uint64_t> tls_errors{0};
    std::atomic<uint64_t> cuda_errors{0};
    std::atomic<uint64_t> want_read{0};
    std::atomic<uint64_t> want_write{0};
    std::atomic<uint64_t> pinned_used{0};
    std::atomic<uint64_t> pinned_peak{0};
    std::atomic<uint64_t> ring_stall_ns{0};
    std::atomic<uint64_t> tls_established{0};
    std::atomic<uint64_t> ktls_rx_enabled{0};
    std::atomic<uint64_t> ktls_rx_manual{0};
    std::atomic<uint64_t> ktls_recv_calls{0};
    std::atomic<uint64_t> ktls_recv_eagain{0};
    std::atomic<uint64_t> ktls_control_records{0};
    std::atomic<uint64_t> ktls_session_tickets{0};
    std::atomic<uint64_t> ktls_rx_rekeys{0};
    std::atomic<uint64_t> ktls_alerts{0};
    std::atomic<uint64_t> no_pad_attempted{0};
    std::atomic<uint64_t> no_pad_succeeded{0};
    std::vector<std::unique_ptr<LaneCounters>> lane;
    std::map<int, std::unique_ptr<GpuCounters>> gpu;

    RunStats(size_t lane_count, const std::set<int> &gpu_ids) {
        lane.reserve(lane_count);
        for (size_t i = 0; i < lane_count; ++i)
            lane.push_back(std::make_unique<LaneCounters>());
        for (int id : gpu_ids) gpu.emplace(id, std::make_unique<GpuCounters>());
    }

    void slot_acquired() {
        const uint64_t current = pinned_used.fetch_add(1) + 1;
        uint64_t peak = pinned_peak.load();
        while (current > peak &&
               !pinned_peak.compare_exchange_weak(peak, current)) {}
    }
    void slot_released() { pinned_used.fetch_sub(1); }

    // Called only while every reactor is joined.  Connection/TLS audit state
    // deliberately survives: it describes the live pool and every transport
    // ever admitted to it.  Transfer counters are per iteration so a measured
    // query is never contaminated by transport setup or bodyless HEAD priming.
    void reset_transfer_counters() {
        if (pinned_used.load(std::memory_order_relaxed) != 0)
            fail("cannot reset transfer counters while pinned slots are in use");
        for (const auto &[gpu_id, counters] : gpu) {
            if (counters->outstanding_copies.load(std::memory_order_relaxed) != 0)
                fail("cannot reset transfer counters with outstanding GPU " +
                     std::to_string(gpu_id) + " copies");
        }
        const auto zero = [](std::atomic<uint64_t> &value) {
            value.store(0, std::memory_order_relaxed);
        };
        zero(body_bytes);
        zero(h2d_submitted_bytes);
        zero(h2d_submitted_copies);
        zero(h2d_completed_bytes);
        zero(h2d_completed_copies);
        zero(h2d_inline_batches);
        zero(h2d_inline_event_queries);
        zero(completed_ranges);
        zero(retries);
        zero(reconnects);
        zero(http_errors);
        zero(tls_errors);
        zero(cuda_errors);
        zero(want_read);
        zero(want_write);
        zero(pinned_peak);
        zero(ring_stall_ns);
        zero(ktls_recv_calls);
        zero(ktls_recv_eagain);
        zero(ktls_control_records);
        zero(ktls_session_tickets);
        zero(ktls_rx_rekeys);
        zero(ktls_alerts);
        for (const auto &counters : lane)
            zero(counters->body_bytes);
        for (const auto &[gpu_id, counters] : gpu) {
            (void)gpu_id;
            zero(counters->h2d_completed_bytes);
        }
    }

};

class FatalState {
public:
    bool failed() const { return failed_.load(std::memory_order_acquire); }
    void set(std::string message) {
        bool expected = false;
        if (failed_.compare_exchange_strong(expected, true,
                                            std::memory_order_acq_rel)) {
            std::lock_guard<std::mutex> lock(mu_);
            message_ = std::move(message);
            print_line("FATAL: ", message_);
        }
    }
    std::string message() const {
        std::lock_guard<std::mutex> lock(mu_);
        return message_;
    }
private:
    std::atomic<bool> failed_{false};
    mutable std::mutex mu_;
    std::string message_;
};

using TlsStatMap = std::map<std::string, int64_t>;

const std::array<const char *, 7> kRequiredTlsStats{{
    "TlsRxSw", "TlsDecryptRetry", "TlsRxNoPadViolation", "TlsDecryptError",
    "TlsRxRekeyOk", "TlsRxRekeyError", "TlsRxRekeyReceived"}};

TlsStatMap read_tls_stats(bool &available) {
    std::ifstream in("/proc/net/tls_stat");
    if (!in) {
        available = false;
        return {};
    }
    available = true;
    TlsStatMap result;
    std::string name;
    int64_t value = 0;
    while (in >> name >> value) result[name] = value;
    if (!in.eof()) fail("failed to parse /proc/net/tls_stat");
    return result;
}

int64_t tls_stat_value(const TlsStatMap &stats, const std::string &name) {
    const auto it = stats.find(name);
    return it == stats.end() ? 0 : it->second;
}

/* ================================================================
 * OpenSSL / TLS 1.3 / RX kTLS
 * ================================================================ */

enum class TlsRxCipher : uint8_t {
    NONE,
    AES_128_GCM,
    AES_256_GCM
};

/*
 * Per-connection state captured by OpenSSL's standard key-log callback.
 * Secrets are never printed or written to disk.  The callback runs on the
 * connection's owning reactor thread, so this object needs no lock.
 *
 * The fixed control buffer is only for rare TLS handshake records such as
 * NewSessionTicket and KeyUpdate.  Application data never enters it.  Keeping
 * it fixed-size also preserves the no-hot-path-allocation rule.
 */
struct TlsRxState {
    bool capture_failed = false;
    bool installed = false;
    bool installed_manually = false;
    bool no_pad = false;
    TlsRxCipher cipher = TlsRxCipher::NONE;
    const char *digest_name = nullptr;
    size_t digest_size = 0;
    size_t key_size = 0;
    std::array<unsigned char, EVP_MAX_MD_SIZE> server_traffic_secret{};
    size_t server_traffic_secret_size = 0;
    uint64_t traffic_secret_generation = 0;
    std::array<unsigned char, kTlsControlBufferMax> control{};
    size_t control_size = 0;

    TlsRxState() = default;
    TlsRxState(const TlsRxState &) = delete;
    TlsRxState &operator=(const TlsRxState &) = delete;

    ~TlsRxState() { clear(); }

    void clear() {
        OPENSSL_cleanse(server_traffic_secret.data(),
                        server_traffic_secret.size());
        if (control_size != 0)
            OPENSSL_cleanse(control.data(), control_size);
        capture_failed = false;
        installed = false;
        installed_manually = false;
        no_pad = false;
        cipher = TlsRxCipher::NONE;
        digest_name = nullptr;
        digest_size = 0;
        key_size = 0;
        server_traffic_secret_size = 0;
        traffic_secret_generation = 0;
        control_size = 0;
    }

    void capture_keylog_line(std::string_view line) noexcept {
        constexpr std::string_view wanted = "SERVER_TRAFFIC_SECRET_0";
        const size_t first_space = line.find(' ');
        if (first_space == std::string_view::npos ||
            line.substr(0, first_space) != wanted) {
            return;
        }
        size_t secret_begin = line.find_first_not_of(' ', first_space);
        if (secret_begin == std::string_view::npos) {
            capture_failed = true;
            return;
        }
        // Skip the client-random token.
        secret_begin = line.find(' ', secret_begin);
        if (secret_begin == std::string_view::npos) {
            capture_failed = true;
            return;
        }
        secret_begin = line.find_first_not_of(' ', secret_begin);
        if (secret_begin == std::string_view::npos) {
            capture_failed = true;
            return;
        }
        size_t secret_end = line.find(' ', secret_begin);
        if (secret_end == std::string_view::npos) secret_end = line.size();
        const std::string_view encoded = line.substr(secret_begin,
                                                     secret_end - secret_begin);
        if (encoded.empty() || (encoded.size() & 1U) != 0 ||
            encoded.size() / 2U > server_traffic_secret.size()) {
            capture_failed = true;
            return;
        }
        std::array<unsigned char, EVP_MAX_MD_SIZE> decoded{};
        const size_t decoded_size = encoded.size() / 2U;
        for (size_t i = 0; i < decoded_size; ++i) {
            const int high = hex_nibble(encoded[i * 2U]);
            const int low = hex_nibble(encoded[i * 2U + 1U]);
            if (high < 0 || low < 0) {
                capture_failed = true;
                OPENSSL_cleanse(decoded.data(), decoded.size());
                return;
            }
            decoded[i] = static_cast<unsigned char>((high << 4) | low);
        }
        if (server_traffic_secret_size != 0 &&
            (server_traffic_secret_size != decoded_size ||
             CRYPTO_memcmp(server_traffic_secret.data(), decoded.data(),
                           decoded_size) != 0)) {
            capture_failed = true;
            OPENSSL_cleanse(decoded.data(), decoded.size());
            return;
        }
        std::memcpy(server_traffic_secret.data(), decoded.data(), decoded_size);
        server_traffic_secret_size = decoded_size;
        OPENSSL_cleanse(decoded.data(), decoded.size());
    }
};

int g_tls_rx_state_ex_index = -1;

void tls_keylog_callback(const SSL *ssl, const char *line) noexcept {
    if (ssl == nullptr || line == nullptr || g_tls_rx_state_ex_index < 0) return;
    auto *state = static_cast<TlsRxState *>(
        SSL_get_ex_data(ssl, g_tls_rx_state_ex_index));
    if (state != nullptr) state->capture_keylog_line(line);
}

/* One-shot HMAC used by TLS 1.3 HKDF-Expand-Label. */
void tls13_hmac(const char *digest_name, size_t expected_size,
                const unsigned char *key, size_t key_size,
                const unsigned char *input, size_t input_size,
                unsigned char *output, size_t output_capacity) {
    EVP_MAC *raw_mac = EVP_MAC_fetch(nullptr, "HMAC", nullptr);
    if (raw_mac == nullptr) fail("EVP_MAC_fetch(HMAC): " + openssl_errors());
    std::unique_ptr<EVP_MAC, void (*)(EVP_MAC *)> mac(raw_mac, EVP_MAC_free);
    EVP_MAC_CTX *raw_ctx = EVP_MAC_CTX_new(mac.get());
    if (raw_ctx == nullptr) fail("EVP_MAC_CTX_new(TLS HKDF): " + openssl_errors());
    std::unique_ptr<EVP_MAC_CTX, void (*)(EVP_MAC_CTX *)> ctx(
        raw_ctx, EVP_MAC_CTX_free);
    OSSL_PARAM parameters[] = {
        OSSL_PARAM_construct_utf8_string(
            OSSL_MAC_PARAM_DIGEST, const_cast<char *>(digest_name), 0),
        OSSL_PARAM_construct_end()
    };
    size_t output_size = 0;
    if (output_capacity < expected_size ||
        EVP_MAC_init(ctx.get(), key, key_size, parameters) != 1 ||
        EVP_MAC_update(ctx.get(), input, input_size) != 1 ||
        EVP_MAC_final(ctx.get(), output, &output_size, output_capacity) != 1 ||
        output_size != expected_size) {
        fail("TLS 1.3 HMAC failed: " + openssl_errors());
    }
}

/* RFC 8446 section 7.1: HKDF-Expand-Label(Secret, Label, Context, Length). */
void tls13_hkdf_expand_label(const char *digest_name, size_t digest_size,
                             const unsigned char *secret, size_t secret_size,
                             std::string_view label,
                             unsigned char *output, size_t output_size) {
    constexpr std::string_view prefix = "tls13 ";
    if (output_size > UINT16_MAX || prefix.size() + label.size() > UINT8_MAX)
        fail("TLS 1.3 HKDF label/output is too large");

    std::array<unsigned char, 512> info{};
    size_t info_size = 0;
    info[info_size++] = static_cast<unsigned char>((output_size >> 8U) & 0xffU);
    info[info_size++] = static_cast<unsigned char>(output_size & 0xffU);
    info[info_size++] = static_cast<unsigned char>(prefix.size() + label.size());
    std::memcpy(info.data() + info_size, prefix.data(), prefix.size());
    info_size += prefix.size();
    std::memcpy(info.data() + info_size, label.data(), label.size());
    info_size += label.size();
    info[info_size++] = 0;  // empty Context

    std::array<unsigned char, EVP_MAX_MD_SIZE> previous{};
    size_t previous_size = 0;
    size_t produced = 0;
    unsigned counter = 1;
    while (produced < output_size) {
        if (counter > UINT8_MAX || previous_size + info_size + 1U > info.size())
            fail("TLS 1.3 HKDF expand exceeds bounded scratch space");
        std::array<unsigned char, 512> hmac_input{};
        std::memcpy(hmac_input.data(), previous.data(), previous_size);
        std::memcpy(hmac_input.data() + previous_size, info.data(), info_size);
        hmac_input[previous_size + info_size] =
            static_cast<unsigned char>(counter);
        tls13_hmac(digest_name, digest_size, secret, secret_size,
                   hmac_input.data(), previous_size + info_size + 1U,
                   previous.data(), previous.size());
        previous_size = digest_size;
        const size_t take = std::min(output_size - produced, previous_size);
        std::memcpy(output + produced, previous.data(), take);
        produced += take;
        ++counter;
        OPENSSL_cleanse(hmac_input.data(), hmac_input.size());
    }
    OPENSSL_cleanse(previous.data(), previous.size());
    OPENSSL_cleanse(info.data(), info.size());
}

void configure_tls_rx_cipher(SSL *ssl, TlsRxState &state) {
    const SSL_CIPHER *cipher = SSL_get_current_cipher(ssl);
    const std::string_view name =
        cipher != nullptr ? SSL_CIPHER_get_name(cipher) : "";
    if (name == "TLS_AES_128_GCM_SHA256") {
        state.cipher = TlsRxCipher::AES_128_GCM;
        state.digest_name = "SHA256";
        state.digest_size = 32;
        state.key_size = TLS_CIPHER_AES_GCM_128_KEY_SIZE;
    } else if (name == "TLS_AES_256_GCM_SHA384") {
        state.cipher = TlsRxCipher::AES_256_GCM;
        state.digest_name = "SHA384";
        state.digest_size = 48;
        state.key_size = TLS_CIPHER_AES_GCM_256_KEY_SIZE;
    } else {
        fail("RX kTLS requires TLS_AES_128_GCM_SHA256 or "
             "TLS_AES_256_GCM_SHA384; negotiated " + std::string(name));
    }
    if (state.capture_failed ||
        state.server_traffic_secret_size != state.digest_size) {
        fail("OpenSSL key-log callback did not capture the expected "
             "SERVER_TRAFFIC_SECRET_0");
    }
}

void ensure_tls_ulp(int fd) {
    std::array<char, 16> current{};
    socklen_t current_size = static_cast<socklen_t>(current.size());
    if (::getsockopt(fd, IPPROTO_TCP, TCP_ULP, current.data(), &current_size) == 0 &&
        std::string_view(current.data(),
                         ::strnlen(current.data(), current.size())) == "tls") {
        return;
    }
    constexpr char name[] = "tls";
    if (::setsockopt(fd, IPPROTO_TCP, TCP_ULP, name, sizeof(name)) != 0 &&
        errno != EEXIST && errno != EBUSY) {
        fail("setsockopt(TCP_ULP=tls): " + errno_string());
    }
}

/*
 * Install either the initial server application key or a rekey.  Linux uses
 * the TLS 1.2 AES-GCM UAPI structure for TLS 1.3 too; the version field and
 * fully implicit 12-byte IV distinguish TLS 1.3.  The IV is split into the
 * four-byte salt and eight-byte iv fields exactly as Linux kTLS expects.
 */
void install_tls13_rx_key(int fd, TlsRxState &state,
                          const unsigned char *traffic_secret,
                          size_t traffic_secret_size, bool initial) {
    if (state.digest_name == nullptr || state.digest_size == 0 ||
        state.key_size == 0) {
        fail("TLS RX cipher metadata is not initialized");
    }
    std::array<unsigned char, TLS_CIPHER_AES_GCM_256_KEY_SIZE> key{};
    std::array<unsigned char, 12> iv{};
    auto cleanse = make_scope_exit([&] {
        OPENSSL_cleanse(key.data(), key.size());
        OPENSSL_cleanse(iv.data(), iv.size());
    });
    tls13_hkdf_expand_label(state.digest_name, state.digest_size,
                            traffic_secret, traffic_secret_size, "key",
                            key.data(), state.key_size);
    tls13_hkdf_expand_label(state.digest_name, state.digest_size,
                            traffic_secret, traffic_secret_size, "iv",
                            iv.data(), iv.size());
    if (initial) ensure_tls_ulp(fd);

    int rc = -1;
    int socket_error = 0;
    if (state.cipher == TlsRxCipher::AES_128_GCM) {
        struct tls12_crypto_info_aes_gcm_128 info{};
        static_assert(sizeof(info.salt) + sizeof(info.iv) == 12,
                      "Linux AES-GCM IV layout changed");
        info.info.version = TLS_1_3_VERSION;
        info.info.cipher_type = TLS_CIPHER_AES_GCM_128;
        std::memcpy(info.key, key.data(), sizeof(info.key));
        std::memcpy(info.salt, iv.data(), sizeof(info.salt));
        std::memcpy(info.iv, iv.data() + sizeof(info.salt), sizeof(info.iv));
        // rec_seq remains zero for application-traffic generation zero and
        // is reset to zero for every TLS 1.3 KeyUpdate.
        rc = ::setsockopt(fd, SOL_TLS, TLS_RX, &info, sizeof(info));
        socket_error = errno;
        OPENSSL_cleanse(&info, sizeof(info));
    } else if (state.cipher == TlsRxCipher::AES_256_GCM) {
        struct tls12_crypto_info_aes_gcm_256 info{};
        static_assert(sizeof(info.salt) + sizeof(info.iv) == 12,
                      "Linux AES-GCM IV layout changed");
        info.info.version = TLS_1_3_VERSION;
        info.info.cipher_type = TLS_CIPHER_AES_GCM_256;
        std::memcpy(info.key, key.data(), sizeof(info.key));
        std::memcpy(info.salt, iv.data(), sizeof(info.salt));
        std::memcpy(info.iv, iv.data() + sizeof(info.salt), sizeof(info.iv));
        rc = ::setsockopt(fd, SOL_TLS, TLS_RX, &info, sizeof(info));
        socket_error = errno;
        OPENSSL_cleanse(&info, sizeof(info));
    } else {
        fail("unsupported TLS RX cipher state");
    }
    if (rc != 0) {
        fail(std::string(initial ? "initial" : "rekey") +
             " setsockopt(SOL_TLS,TLS_RX): " + errno_string(socket_error));
    }
}

void update_tls13_rx_key(int fd, TlsRxState &state, RunStats &stats) {
    std::array<unsigned char, EVP_MAX_MD_SIZE> next_secret{};
    tls13_hkdf_expand_label(
        state.digest_name, state.digest_size,
        state.server_traffic_secret.data(), state.server_traffic_secret_size,
        "traffic upd", next_secret.data(), state.digest_size);
    auto cleanse = make_scope_exit([&] {
        OPENSSL_cleanse(next_secret.data(), next_secret.size());
    });
    install_tls13_rx_key(fd, state, next_secret.data(), state.digest_size, false);
    OPENSSL_cleanse(state.server_traffic_secret.data(),
                    state.server_traffic_secret.size());
    std::memcpy(state.server_traffic_secret.data(), next_secret.data(),
                state.digest_size);
    state.server_traffic_secret_size = state.digest_size;
    ++state.traffic_secret_generation;
    stats.ktls_rx_rekeys.fetch_add(1);
}

class SslContext {
public:
    SslContext() {
        if (OPENSSL_init_ssl(OPENSSL_INIT_LOAD_SSL_STRINGS, nullptr) != 1)
            fail("OPENSSL_init_ssl: " + openssl_errors());
        if (g_tls_rx_state_ex_index < 0) {
            g_tls_rx_state_ex_index = SSL_get_ex_new_index(
                0, nullptr, nullptr, nullptr, nullptr);
            if (g_tls_rx_state_ex_index < 0)
                fail("SSL_get_ex_new_index(TLS RX state): " + openssl_errors());
        }
        ctx_ = SSL_CTX_new(TLS_client_method());
        if (ctx_ == nullptr) fail("SSL_CTX_new: " + openssl_errors());
        try {
            if (SSL_CTX_set_min_proto_version(ctx_, TLS1_3_VERSION) != 1 ||
                SSL_CTX_set_max_proto_version(ctx_, TLS1_3_VERSION) != 1)
                fail("forcing TLS 1.3 failed: " + openssl_errors());
            if (SSL_CTX_set_ciphersuites(
                    ctx_, "TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384") != 1)
                fail("setting TLS 1.3 AES-GCM ciphers failed: " + openssl_errors());
            const uint64_t enabled_options = SSL_CTX_set_options(
                ctx_, SSL_OP_ENABLE_KTLS);
            if ((enabled_options & SSL_OP_ENABLE_KTLS) == 0)
                fail("OpenSSL refused SSL_OP_ENABLE_KTLS");
            // A clean record boundary is mandatory when userspace hands RX to
            // the kernel.  Explicitly disable OpenSSL read-ahead and session
            // caching; post-handshake tickets are authenticated, validated,
            // and discarded by the bounded control-record path below.
            SSL_CTX_set_read_ahead(ctx_, 0);
            (void)SSL_CTX_set_session_cache_mode(ctx_, SSL_SESS_CACHE_OFF);
            SSL_CTX_set_keylog_callback(ctx_, tls_keylog_callback);
            SSL_CTX_set_verify(ctx_, SSL_VERIFY_PEER, nullptr);
            if (SSL_CTX_set_default_verify_paths(ctx_) != 1) {
                fail("loading default CA paths: " + openssl_errors());
            }
        } catch (...) {
            SSL_CTX_free(ctx_);
            ctx_ = nullptr;
            throw;
        }
    }
    ~SslContext() { if (ctx_ != nullptr) SSL_CTX_free(ctx_); }
    SslContext(const SslContext &) = delete;
    SslContext &operator=(const SslContext &) = delete;
    SSL_CTX *get() const { return ctx_; }
private:
    SSL_CTX *ctx_ = nullptr;
};

SSL *make_ssl(SSL_CTX *ctx, int fd, const std::string &hostname,
              TlsRxState &tls_rx) {
    SSL *ssl = SSL_new(ctx);
    if (ssl == nullptr) fail("SSL_new: " + openssl_errors());
    tls_rx.clear();
    if (SSL_set_ex_data(ssl, g_tls_rx_state_ex_index, &tls_rx) != 1) {
        const std::string detail = openssl_errors();
        SSL_free(ssl);
        fail("attaching TLS RX key-capture state: " + detail);
    }
    if (SSL_set_tlsext_host_name(ssl, hostname.c_str()) != 1 ||
        SSL_set1_host(ssl, hostname.c_str()) != 1 ||
        SSL_set_fd(ssl, fd) != 1) {
        const std::string detail = openssl_errors();
        SSL_free(ssl);
        fail("configuring TLS SNI/hostname/socket: " + detail);
    }
    SSL_set_connect_state(ssl);
    const long requested_modes = SSL_MODE_ENABLE_PARTIAL_WRITE |
                                 SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER;
    const long enabled_modes = SSL_set_mode(ssl, requested_modes);
    if ((enabled_modes & requested_modes) != requested_modes) {
        SSL_free(ssl);
        fail("OpenSSL refused required partial-write modes");
    }
    return ssl;
}

Fd make_bound_socket(const std::string &nic, const SocketAddress &address,
                     bool &connected_immediately) {
    Fd fd(::socket(address.storage.ss_family,
                   SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP));
    if (!fd) fail("socket for NIC " + nic + ": " + errno_string());
    // Binding every S3 socket makes lane/NIC attribution explicit.  Do not add
    // SO_RCVBUF here: setting it locks the receive buffer and disables Linux's
    // TCP receive autotuning, which is a severe high-BDP throughput regression.
    if (::setsockopt(fd.get(), SOL_SOCKET, SO_BINDTODEVICE, nic.c_str(),
                     static_cast<socklen_t>(nic.size() + 1)) != 0)
        fail("SO_BINDTODEVICE(" + nic + "): " + errno_string());
    int one = 1;
    if (::setsockopt(fd.get(), IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) != 0)
        fail("TCP_NODELAY: " + errno_string());
    const int rc = ::connect(fd.get(),
        reinterpret_cast<const sockaddr *>(&address.storage), address.length);
    if (rc == 0) connected_immediately = true;
    else if (errno == EINPROGRESS) connected_immediately = false;
    else fail("connect(" + address.numeric + "): " + errno_string());
    return fd;
}

void audit_tls_connection(SSL *ssl, int fd, RunStats &stats,
                          const std::string &label, TlsRxState &tls_rx) {
    const std::string version = SSL_get_version(ssl);
    const SSL_CIPHER *cipher = SSL_get_current_cipher(ssl);
    const std::string cipher_name =
        cipher != nullptr ? SSL_CIPHER_get_name(cipher) : "unknown";
    if (version != "TLSv1.3")
        fail(label + ": negotiated " + version + ", expected TLSv1.3");
    if (SSL_get_verify_result(ssl) != X509_V_OK)
        fail(label + ": certificate validation failed: " +
             std::string(X509_verify_cert_error_string(SSL_get_verify_result(ssl))));
    const uint64_t tls_sequence = stats.tls_established.fetch_add(1);

    BIO *rbio = SSL_get_rbio(ssl);
    if (rbio == nullptr || BIO_method_type(rbio) != BIO_TYPE_SOCKET ||
        static_cast<int>(BIO_get_fd(rbio, nullptr)) != fd) {
        fail(label + ": direct RX requires OpenSSL's socket BIO on the S3 fd");
    }

    configure_tls_rx_cipher(ssl, tls_rx);
    if (SSL_pending(ssl) != 0 || SSL_has_pending(ssl) != 0) {
        fail(label + ": OpenSSL retained input at the RX-kTLS handoff; "
             "refusing a sequence-number-ambiguous transition");
    }
    if (BIO_get_ktls_recv(rbio) == 1) {
        // A future OpenSSL may activate TLS 1.3 RX itself.  The direct
        // recvmsg path remains useful because SSL_read_ex still copies from
        // OpenSSL's record buffer to the caller.
        tls_rx.installed = true;
    } else {
        install_tls13_rx_key(fd, tls_rx,
                             tls_rx.server_traffic_secret.data(),
                             tls_rx.server_traffic_secret_size, true);
        tls_rx.installed = true;
        tls_rx.installed_manually = true;

        /*
         * OpenSSL's socket BIO records RX-kTLS state in this flag.  It is
         * 0x2000 throughout OpenSSL 1.1.1/3.x, but is an internal detail;
         * the security-relevant activation above is Linux TLS_RX.  Marking
         * the BIO keeps BIO_get_ktls_recv() truthful and prevents a later
         * SSL_read_ex from attempting userspace decryption.
         */
        constexpr int openssl_bio_flags_ktls_rx = 0x2000;
        BIO_set_flags(rbio, openssl_bio_flags_ktls_rx);
        if (BIO_get_ktls_recv(rbio) != 1)
            fail(label + ": TLS_RX installed but OpenSSL socket BIO could "
                 "not be marked RX-kTLS; connection cannot safely continue");
    }

    const bool ktls = tls_rx.installed && BIO_get_ktls_recv(rbio) == 1;
    if (ktls) {
        stats.ktls_rx_enabled.fetch_add(1);
        if (tls_rx.installed_manually) stats.ktls_rx_manual.fetch_add(1);
    }
    if (!ktls)
        fail(label + ": direct TLS 1.3 RX kTLS is not active");
    stats.no_pad_attempted.fetch_add(1);
    int one = 1;
    if (::setsockopt(fd, SOL_TLS, TLS_RX_EXPECT_NO_PAD, &one, sizeof(one)) != 0) {
        fail(label + ": TLS_RX_EXPECT_NO_PAD failed: " + errno_string());
    }
    tls_rx.no_pad = true;
    stats.no_pad_succeeded.fetch_add(1);
    // Every connection is validated and counted. Print one representative
    // connection; the final aggregate audits the complete pool.
    if (tls_sequence == 0) {
        print_line("TLS_SAMPLE ", label,
                   " version=", version,
                   " cipher=", cipher_name,
                   " ktls_rx=yes activation=",
                   (tls_rx.installed_manually ? "manual-keylog" : "openssl"),
                   " receive_api=recvmsg-direct expect_no_pad=yes");
    }
}

enum class TlsControlStatus : uint8_t {
    CONSUMED,
    CLOSE_NOTIFY,
    CONNECTION_ERROR
};

struct TlsControlOutcome {
    TlsControlStatus status = TlsControlStatus::CONSUMED;
    std::string detail;
};

uint16_t tls_read_u16(const unsigned char *data) {
    return static_cast<uint16_t>((static_cast<uint16_t>(data[0]) << 8U) |
                                 static_cast<uint16_t>(data[1]));
}

uint32_t tls_read_u24(const unsigned char *data) {
    return (static_cast<uint32_t>(data[0]) << 16U) |
           (static_cast<uint32_t>(data[1]) << 8U) |
           static_cast<uint32_t>(data[2]);
}

bool valid_tls13_new_session_ticket(const unsigned char *body, size_t size) {
    // ticket_lifetime(4), ticket_age_add(4), nonce<0..255>,
    // ticket<1..65535>, extensions<0..65535>.
    if (size < 4U + 4U + 1U + 2U + 1U + 2U) return false;
    size_t offset = 8;
    const size_t nonce_size = body[offset++];
    if (nonce_size > size - offset) return false;
    offset += nonce_size;
    if (size - offset < 2) return false;
    const size_t ticket_size = tls_read_u16(body + offset);
    offset += 2;
    if (ticket_size == 0 || ticket_size > size - offset) return false;
    offset += ticket_size;
    if (size - offset < 2) return false;
    const size_t extensions_size = tls_read_u16(body + offset);
    offset += 2;
    if (extensions_size != size - offset) return false;
    const size_t extensions_end = offset + extensions_size;
    while (offset < extensions_end) {
        if (extensions_end - offset < 4) return false;
        const size_t extension_size = tls_read_u16(body + offset + 2);
        offset += 4;
        if (extension_size > extensions_end - offset) return false;
        offset += extension_size;
    }
    return offset == extensions_end;
}

TlsControlOutcome process_tls13_handshake_record(
        int fd, TlsRxState &state, const unsigned char *data, size_t size,
        RunStats &stats) {
    if (size > state.control.size() - state.control_size) {
        return {TlsControlStatus::CONNECTION_ERROR,
                "TLS post-handshake message exceeds the bounded 128 KiB buffer"};
    }
    std::memcpy(state.control.data() + state.control_size, data, size);
    state.control_size += size;

    while (state.control_size >= 4) {
        const uint8_t message_type = state.control[0];
        const size_t body_size = tls_read_u24(state.control.data() + 1);
        if (body_size > state.control.size() - 4U) {
            return {TlsControlStatus::CONNECTION_ERROR,
                    "TLS post-handshake message length exceeds 128 KiB bound"};
        }
        const size_t message_size = 4U + body_size;
        if (state.control_size < message_size) return {};
        const unsigned char *body = state.control.data() + 4;

        // RFC 8446 handshake type 4: NewSessionTicket.  This benchmark does
        // not reuse TLS sessions, but still validates the authenticated
        // message framing before discarding it.
        if (message_type == 4) {
            if (!valid_tls13_new_session_ticket(body, body_size)) {
                return {TlsControlStatus::CONNECTION_ERROR,
                        "malformed TLS 1.3 NewSessionTicket"};
            }
            stats.ktls_session_tickets.fetch_add(1);
        } else if (message_type == 24) {  // RFC 8446 KeyUpdate
            if (body_size != 1 || body[0] > 1) {
                return {TlsControlStatus::CONNECTION_ERROR,
                        "malformed TLS 1.3 KeyUpdate"};
            }
            update_tls13_rx_key(fd, state, stats);
            if (body[0] == 1) {
                // Responding would require a coordinated OpenSSL/kTLS TX
                // generation transition.  S3 is not expected to request it;
                // reconnecting is safer than sending under the wrong TX key.
                return {TlsControlStatus::CONNECTION_ERROR,
                        "peer requested reciprocal TLS 1.3 KeyUpdate; "
                        "closing instead of risking an incoherent TX key"};
            }
        } else {
            return {TlsControlStatus::CONNECTION_ERROR,
                    "unexpected TLS 1.3 post-handshake message type " +
                        std::to_string(message_type)};
        }

        const size_t remaining = state.control_size - message_size;
        if (remaining != 0) {
            std::memmove(state.control.data(),
                         state.control.data() + message_size, remaining);
        }
        OPENSSL_cleanse(state.control.data() + remaining,
                        state.control_size - remaining);
        state.control_size = remaining;
    }
    return {};
}

TlsControlOutcome process_tls13_control_record(
        int fd, TlsRxState &state, uint8_t record_type,
        const unsigned char *data, size_t size, RunStats &stats) {
    stats.ktls_control_records.fetch_add(1);
    if (record_type == SSL3_RT_HANDSHAKE) {
        return process_tls13_handshake_record(fd, state, data, size, stats);
    }
    if (state.control_size != 0) {
        return {TlsControlStatus::CONNECTION_ERROR,
                "TLS control type interrupted a fragmented handshake message"};
    }
    if (record_type == SSL3_RT_ALERT) {
        stats.ktls_alerts.fetch_add(1);
        if (size != 2) {
            return {TlsControlStatus::CONNECTION_ERROR,
                    "TLS 1.3 alert payload is not exactly two bytes"};
        }
        const unsigned level = data[0];
        const unsigned description = data[1];
        if (description == SSL_AD_CLOSE_NOTIFY)
            return {TlsControlStatus::CLOSE_NOTIFY, "TLS close_notify"};
        return {TlsControlStatus::CONNECTION_ERROR,
                "TLS alert level=" + std::to_string(level) +
                    " description=" + std::to_string(description)};
    }
    return {TlsControlStatus::CONNECTION_ERROR,
            "unexpected kTLS record type " + std::to_string(record_type)};
}

enum class DirectTlsReadStatus : uint8_t {
    APPLICATION_DATA,
    CONTROL_CONSUMED,
    WANT_READ,
    PEER_CLOSED,
    CONNECTION_ERROR
};

struct DirectTlsReadResult {
    DirectTlsReadStatus status = DirectTlsReadStatus::CONNECTION_ERROR;
    size_t bytes = 0;
    std::string detail;
};

// Payload reactors own these counters exclusively.  Keeping their hot
// increments thread-local avoids bouncing one RunStats cache line among every
// reactor (and across NUMA nodes) for each recvmsg/EAGAIN.  Reactors publish
// atomic snapshots between event-loop turns for telemetry.
struct DirectTlsReadCounters {
    uint64_t calls = 0;
    uint64_t eagain = 0;
};

/*
 * This is the hot RX operation.  msg_iov points at the CUDA-registered slot;
 * Linux authenticates/decrypts directly into that exact allocation.  The
 * ancillary byte is mandatory because only application-data records may
 * proceed to HTTP/H2D accounting.
 */
DirectTlsReadResult direct_tls_recv(int fd, TlsRxState &state,
                                    unsigned char *destination,
                                    size_t capacity, RunStats &stats,
                                    DirectTlsReadCounters *local = nullptr) {
    if (!state.installed)
        return {DirectTlsReadStatus::CONNECTION_ERROR, 0,
                "direct_tls_recv called before TLS_RX activation"};
    if (capacity < kTlsPlaintextRecordMax)
        return {DirectTlsReadStatus::CONNECTION_ERROR, 0,
                "direct kTLS receive capacity is below 16 KiB"};

    struct iovec iov{};
    iov.iov_base = destination;
    iov.iov_len = capacity;
    union ControlBuffer {
        struct cmsghdr alignment;
        std::array<unsigned char, CMSG_SPACE(sizeof(unsigned char))> bytes;
    } control{};
    struct msghdr message{};
    message.msg_iov = &iov;
    message.msg_iovlen = 1;
    message.msg_control = control.bytes.data();
    message.msg_controllen = control.bytes.size();

    ssize_t received = -1;
    do {
        message.msg_controllen = control.bytes.size();
        message.msg_flags = 0;
        if (local != nullptr)
            ++local->calls;
        else
            stats.ktls_recv_calls.fetch_add(1, std::memory_order_relaxed);
        received = ::recvmsg(fd, &message, 0);
    } while (received < 0 && errno == EINTR);
    if (received < 0) {
        const int error = errno;
        if (error == EAGAIN || error == EWOULDBLOCK) {
            if (local != nullptr)
                ++local->eagain;
            else
                stats.ktls_recv_eagain.fetch_add(1,
                                                  std::memory_order_relaxed);
            return {DirectTlsReadStatus::WANT_READ, 0, {}};
        }
        return {DirectTlsReadStatus::CONNECTION_ERROR, 0,
                "kTLS recvmsg: " + errno_string(error)};
    }
    if (received == 0)
        return {DirectTlsReadStatus::PEER_CLOSED, 0, "TCP EOF"};
    if ((message.msg_flags & (MSG_CTRUNC | MSG_TRUNC)) != 0) {
        return {DirectTlsReadStatus::CONNECTION_ERROR, 0,
                "kTLS recvmsg truncated payload or ancillary record type"};
    }

    bool found_type = false;
    uint8_t record_type = 0;
    for (struct cmsghdr *header = CMSG_FIRSTHDR(&message);
         header != nullptr; header = CMSG_NXTHDR(&message, header)) {
        if (header->cmsg_level != SOL_TLS ||
            header->cmsg_type != TLS_GET_RECORD_TYPE ||
            header->cmsg_len != CMSG_LEN(sizeof(unsigned char)) || found_type) {
            return {DirectTlsReadStatus::CONNECTION_ERROR, 0,
                    "invalid or unexpected kTLS ancillary data"};
        }
        record_type = *reinterpret_cast<unsigned char *>(CMSG_DATA(header));
        found_type = true;
    }
    if (!found_type) {
        return {DirectTlsReadStatus::CONNECTION_ERROR, 0,
                "kTLS recvmsg omitted TLS_GET_RECORD_TYPE"};
    }

    const size_t bytes = static_cast<size_t>(received);
    if (record_type == SSL3_RT_APPLICATION_DATA) {
        if (state.control_size != 0) {
            return {DirectTlsReadStatus::CONNECTION_ERROR, 0,
                    "application data interrupted a fragmented TLS handshake"};
        }
        return {DirectTlsReadStatus::APPLICATION_DATA, bytes, {}};
    }

    const TlsControlOutcome control_result = process_tls13_control_record(
        fd, state, record_type, destination, bytes, stats);
    switch (control_result.status) {
        case TlsControlStatus::CONSUMED:
            return {DirectTlsReadStatus::CONTROL_CONSUMED, 0, {}};
        case TlsControlStatus::CLOSE_NOTIFY:
            return {DirectTlsReadStatus::PEER_CLOSED, 0,
                    control_result.detail};
        case TlsControlStatus::CONNECTION_ERROR:
            return {DirectTlsReadStatus::CONNECTION_ERROR, 0,
                    control_result.detail};
    }
    return {DirectTlsReadStatus::CONNECTION_ERROR, 0,
            "unreachable TLS control disposition"};
}

struct Object;
struct RangeTask;

struct DeviceAllocation {
    int gpu = -1;
    void *pointer = nullptr;
    uint64_t bytes = 0;
    ~DeviceAllocation() {
        if (pointer != nullptr) {
            cuda_log_if_error(cudaSetDevice(gpu), "cudaSetDevice(cudaFree)");
            const cudaError_t status = cudaFree(pointer);
            if (status != cudaSuccess)
                print_line("ERROR cudaFree gpu=", gpu, ": ", cuda_string(status));
        }
    }
    DeviceAllocation() = default;
    DeviceAllocation(const DeviceAllocation &) = delete;
    DeviceAllocation &operator=(const DeviceAllocation &) = delete;
};

struct Object {
    int id = -1;
    std::string bucket;
    std::string raw_key;
    std::string display_uri;
    RequestTarget target;
    std::shared_ptr<Endpoint> endpoint;
    int gpu = -1;
    uint64_t size = 0;
    bool etag_from_catalog = false;
    // A catalog ETag makes every Range GET conditional.  If the metastore
    // snapshot has only sizes, the first response learns the ETag and every
    // concurrent response must agree.  Either way, a successful iteration
    // materializes bytes from one object version without a HEAD phase.
    mutable std::mutex etag_mutex;
    std::string etag;
    DeviceAllocation device;
    std::atomic<uint64_t> completed_bytes{0};
    std::atomic<uint64_t> completed_ranges{0};
    std::vector<RangeTask *> tasks;
};

std::optional<std::string> object_etag_for_request(const Object &object) {
    std::lock_guard<std::mutex> lock(object.etag_mutex);
    if (object.etag.empty()) return std::nullopt;
    return object.etag;
}

bool validate_or_learn_object_etag(Object &object, std::string_view response_etag) {
    std::lock_guard<std::mutex> lock(object.etag_mutex);
    if (object.etag.empty()) {
        object.etag.assign(response_etag.data(), response_etag.size());
        return true;
    }
    return object.etag == response_etag;
}

std::vector<std::unique_ptr<Object>> make_objects(
    const std::vector<CatalogObjectSpec> &specs, const Options &opt,
    const std::vector<std::string> &overrides) {
    std::map<std::string, std::shared_ptr<Endpoint>> endpoint_cache;
    std::vector<std::unique_ptr<Object>> result;
    result.reserve(specs.size());
    for (size_t i = 0; i < specs.size(); ++i) {
        const CatalogObjectSpec &spec = specs[i];
        if (spec.bucket.find_first_of("\r\n/ ") != std::string::npos)
            fail("bucket is not valid for a virtual-hosted endpoint: " + spec.bucket);
        if (spec.raw_key.find_first_of("\r\n") != std::string::npos)
            fail("S3 keys containing CR/LF are rejected");
        const std::string hostname = spec.bucket + ".s3." + opt.region +
                                     ".amazonaws.com";
        auto found = endpoint_cache.find(hostname);
        if (found == endpoint_cache.end()) {
            auto endpoint = std::make_shared<Endpoint>();
            endpoint->hostname = hostname;
            endpoint->addresses = resolve_addresses(hostname, overrides);
            found = endpoint_cache.emplace(hostname, std::move(endpoint)).first;
        }
        auto object = std::make_unique<Object>();
        object->id = static_cast<int>(i);
        object->bucket = spec.bucket;
        object->raw_key = spec.raw_key;
        object->display_uri = spec.display_uri;
        object->target.hostname = hostname;
        object->target.canonical_uri = uri_encode_path(spec.raw_key);
        object->endpoint = found->second;
        object->size = spec.size;
        if (spec.etag) {
            object->etag = *spec.etag;
            object->etag_from_catalog = true;
        }
        result.push_back(std::move(object));
    }
    return result;
}

std::set<int> enabled_gpu_ids(const std::vector<Lane> &lanes) {
    std::set<int> ids;
    for (const Lane &lane : lanes) ids.insert(lane.gpu);
    return ids;
}

void assign_and_allocate_objects(std::vector<std::unique_ptr<Object>> &objects,
                                 std::vector<GpuInfo> &gpus,
                                 const std::vector<Lane> &lanes,
                                 uint64_t reserve_bytes) {
    const std::set<int> enabled = enabled_gpu_ids(lanes);
    if (enabled.empty()) fail("no GPUs are enabled by lanes");
    std::map<int, uint64_t> free_now;
    std::map<int, uint64_t> capacity;
    std::map<int, uint64_t> planned;
    for (int id : enabled) {
        (void)find_gpu(gpus, id);
        cuda_check(cudaSetDevice(id), "cudaSetDevice(capacity planning)");
        size_t free_bytes = 0;
        size_t total_bytes = 0;
        cuda_check(cudaMemGetInfo(&free_bytes, &total_bytes),
                   "cudaMemGetInfo(capacity planning)");
        free_now[id] = static_cast<uint64_t>(free_bytes);
        if (free_now[id] < reserve_bytes)
            fail("GPU " + std::to_string(id) + " free memory " +
                 std::to_string(free_now[id]) + " is below requested reserve " +
                 std::to_string(reserve_bytes));
        capacity[id] = free_now[id] - reserve_bytes;
        planned[id] = 0;
    }

    std::vector<Object *> by_descending_size;
    for (auto &holder : objects) {
        by_descending_size.push_back(holder.get());
    }
    std::sort(by_descending_size.begin(), by_descending_size.end(),
              [](const Object *a, const Object *b) {
        return a->size > b->size;
    });
    for (Object *object : by_descending_size) {
        int best = -1;
        uint64_t best_planned = std::numeric_limits<uint64_t>::max();
        for (int id : enabled) {
            if (planned[id] <= capacity[id] &&
                object->size <= capacity[id] - planned[id] &&
                planned[id] < best_planned) {
                best = id;
                best_planned = planned[id];
            }
        }
        if (best < 0)
            fail("no enabled GPU can fit " + object->display_uri +
                 " while retaining --gpu-reserve-mib");
        object->gpu = best;
        planned[best] += object->size;
    }

    for (auto &holder : objects) {
        Object &object = *holder;
        object.device.gpu = object.gpu;
        object.device.bytes = object.size;
        if (object.size == 0) continue;
        if (object.size > std::numeric_limits<size_t>::max())
            fail("object is too large for cudaMalloc size_t: " + object.display_uri);
        cuda_check(cudaSetDevice(object.gpu), "cudaSetDevice(cudaMalloc)");
        cuda_check(cudaMalloc(&object.device.pointer,
                              static_cast<size_t>(object.size)),
                   ("cudaMalloc object=" + std::to_string(object.id)).c_str());
    }
    for (int id : enabled) {
        cuda_check(cudaSetDevice(id), "cudaSetDevice(post-allocation)");
        size_t free_bytes = 0;
        size_t total_bytes = 0;
        cuda_check(cudaMemGetInfo(&free_bytes, &total_bytes),
                   "cudaMemGetInfo(post-allocation)");
        if (static_cast<uint64_t>(free_bytes) < reserve_bytes)
            fail("GPU " + std::to_string(id) +
                 " fell below the requested reserve after cudaMalloc");
    }
}

void verify_gpu_reserve(const std::set<int> &gpu_ids, uint64_t reserve_bytes,
                        const std::string &phase) {
    for (int id : gpu_ids) {
        cuda_check(cudaSetDevice(id), "cudaSetDevice(reserve verification)");
        size_t free_bytes = 0;
        size_t total_bytes = 0;
        cuda_check(cudaMemGetInfo(&free_bytes, &total_bytes),
                   "cudaMemGetInfo(reserve verification)");
        if (static_cast<uint64_t>(free_bytes) < reserve_bytes)
            fail("GPU " + std::to_string(id) + " free HBM after " + phase +
                 " is " + std::to_string(free_bytes) +
                 ", below reserve " + std::to_string(reserve_bytes));
    }
}

/* ================================================================
 * Range scheduler and exact completion accounting
 * ================================================================ */

enum class TaskState : uint8_t { PENDING, ACTIVE, WAITING_H2D, COMPLETE, FAILED };

struct RangeTask {
    uint64_t id = 0;
    Object *object = nullptr;
    uint64_t start = 0;
    uint64_t length = 0;
    int lane_id = -1;
    std::atomic<TaskState> state{TaskState::PENDING};
    unsigned retries = 0;
    uint64_t attempt = 1;

    // The following fields are owned by exactly one reactor while ACTIVE or
    // WAITING_H2D.  A failed attempt is synchronized before it is requeued.
    uint64_t received = 0;
    uint64_t h2d_completed_attempt = 0;
    uint64_t pending_h2d = 0;
};

class Scheduler {
public:
    Scheduler(std::vector<std::unique_ptr<Object>> &objects,
              const std::vector<Lane> &lanes, uint64_t range_bytes,
              RunStats &stats, FatalState &fatal)
        : queues_(lanes.size()), queue_mu_(lanes.size()), stats_(stats), fatal_(fatal) {
        std::map<int, std::vector<int>> lanes_for_gpu;
        for (const Lane &lane : lanes) lanes_for_gpu[lane.gpu].push_back(lane.id);
        uint64_t next_id = 0;
        std::map<int, size_t> round_robin;
        for (auto &holder : objects) {
            Object &object = *holder;
            const auto found = lanes_for_gpu.find(object.gpu);
            if (found == lanes_for_gpu.end() || found->second.empty())
                fail("no lane can transfer to GPU " + std::to_string(object.gpu));
            uint64_t offset = 0;
            while (offset < object.size) {
                const uint64_t length = std::min(range_bytes, object.size - offset);
                auto task = std::make_unique<RangeTask>();
                task->id = next_id++;
                task->object = &object;
                task->start = offset;
                task->length = length;
                const std::vector<int> &choices = found->second;
                task->lane_id = choices[round_robin[object.gpu]++ % choices.size()];
                object.tasks.push_back(task.get());
                queues_[static_cast<size_t>(task->lane_id)].push_back(task.get());
                tasks_.push_back(std::move(task));
                offset += length;
            }
        }
        remaining_.store(tasks_.size());
    }

    RangeTask *pop(int lane_id) {
        std::lock_guard<std::mutex> lock(queue_mu_.at(static_cast<size_t>(lane_id)));
        auto &queue = queues_.at(static_cast<size_t>(lane_id));
        if (queue.empty()) return nullptr;
        RangeTask *task = queue.front();
        queue.pop_front();
        TaskState expected = TaskState::PENDING;
        if (!task->state.compare_exchange_strong(expected, TaskState::ACTIVE)) {
            fatal_.set("scheduler popped a task not in PENDING state");
            return nullptr;
        }
        return task;
    }

    void retry(RangeTask &task, unsigned max_retries, const std::string &why) {
        if (task.pending_h2d != 0) {
            fatal_.set("range retry attempted with outstanding H2D events");
            task.state.store(TaskState::FAILED);
            return;
        }
        if (task.retries >= max_retries) {
            task.state.store(TaskState::FAILED);
            fatal_.set("range " + std::to_string(task.id) +
                       " exhausted retries: " + why);
            return;
        }
        ++task.retries;
        ++task.attempt;
        task.received = 0;
        task.h2d_completed_attempt = 0;
        task.state.store(TaskState::PENDING, std::memory_order_release);
        stats_.retries.fetch_add(1);
        {
            std::lock_guard<std::mutex> lock(
                queue_mu_.at(static_cast<size_t>(task.lane_id)));
            queues_.at(static_cast<size_t>(task.lane_id)).push_back(&task);
        }
        print_line("RETRY range=", task.id, " object=", task.object->id,
                   " attempt=", task.attempt, " reason=", why);
    }

    void received_all(RangeTask &task) {
        if (task.received != task.length)
            fatal_.set("range entered WAITING_H2D with wrong received byte count");
        else
            task.state.store(TaskState::WAITING_H2D, std::memory_order_release);
    }

    bool complete_if_ready(RangeTask &task) {
        if (task.state.load(std::memory_order_acquire) != TaskState::WAITING_H2D ||
            task.pending_h2d != 0)
            return false;
        if (task.received != task.length ||
            task.h2d_completed_attempt != task.length) {
            fatal_.set("range " + std::to_string(task.id) +
                       " has mismatched receive/H2D completion counts");
            task.state.store(TaskState::FAILED);
            return false;
        }
        task.object->completed_bytes.fetch_add(task.length);
        task.object->completed_ranges.fetch_add(1);
        task.state.store(TaskState::COMPLETE, std::memory_order_release);
        stats_.completed_ranges.fetch_add(1);
        const uint64_t before = remaining_.fetch_sub(
            1, std::memory_order_acq_rel);
        if (before == 0) {
            fatal_.set("device-complete range counter underflow");
            return false;
        }
        return true;
    }

    uint64_t remaining() const { return remaining_.load(std::memory_order_acquire); }
    size_t task_count() const { return tasks_.size(); }
    const std::vector<std::unique_ptr<RangeTask>> &tasks() const { return tasks_; }

    // Choose transport authorities without consuming or activating a Range.
    // Evenly sampling the lane's immutable plan gives multi-bucket catalogs a
    // representative initial pool.  A lane with no work still gets a valid
    // transport target so the configured pool size remains an explicit,
    // independently auditable resource rather than a dataset-size side effect.
    std::vector<Object *> preconnect_targets(int lane_id, size_t count) const {
        if (tasks_.empty()) fail("cannot preconnect without a Range plan");
        std::vector<Object *> candidates;
        for (const auto &task : tasks_)
            if (task->lane_id == lane_id) candidates.push_back(task->object);
        if (candidates.empty()) {
            for (const auto &task : tasks_) candidates.push_back(task->object);
        }
        std::vector<Object *> result;
        result.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            const size_t index = static_cast<size_t>(
                (static_cast<unsigned long long>(i) * candidates.size()) /
                count);
            result.push_back(candidates.at(index));
        }
        return result;
    }

    // Requeue the identical non-overlapping range plan after a completed
    // iteration.  Final cudaMalloc allocations and object ETags remain live;
    // the next transfer overwrites the same exact object offsets and repeats
    // all HTTP Content-Range/Content-Length/If-Match validation.
    void reset_for_iteration() {
        if (remaining() != 0)
            fail("cannot reset range scheduler before all tasks complete");
        for (size_t lane_id = 0; lane_id < queues_.size(); ++lane_id) {
            std::lock_guard<std::mutex> lock(queue_mu_[lane_id]);
            if (!queues_[lane_id].empty())
                fail("cannot reset nonempty range queue");
        }
        for (const auto &holder : tasks_) {
            const RangeTask &task = *holder;
            if (task.state.load(std::memory_order_acquire) !=
                    TaskState::COMPLETE ||
                task.pending_h2d != 0)
                fail("cannot reset an incomplete range task");
        }
        for (const auto &holder : tasks_) {
            RangeTask &task = *holder;
            ++task.attempt;
            task.retries = 0;
            task.received = 0;
            task.h2d_completed_attempt = 0;
            task.pending_h2d = 0;
            task.state.store(TaskState::PENDING, std::memory_order_release);
            std::lock_guard<std::mutex> lock(
                queue_mu_.at(static_cast<size_t>(task.lane_id)));
            queues_.at(static_cast<size_t>(task.lane_id)).push_back(&task);
        }
        std::set<Object *> reset_objects;
        for (const auto &holder : tasks_)
            reset_objects.insert(holder->object);
        for (Object *object : reset_objects) {
            object->completed_bytes.store(0, std::memory_order_relaxed);
            object->completed_ranges.store(0, std::memory_order_relaxed);
        }
        remaining_.store(tasks_.size(), std::memory_order_release);
    }

private:
    std::vector<std::unique_ptr<RangeTask>> tasks_;
    std::vector<std::deque<RangeTask *>> queues_;
    std::vector<std::mutex> queue_mu_;
    std::atomic<uint64_t> remaining_{0};
    RunStats &stats_;
    FatalState &fatal_;
};

/* ================================================================
 * Bounded NUMA-local CUDA-pinned host arena and slot state machine
 * ================================================================ */

class PinnedArena {
public:
    PinnedArena(const Lane &lane, size_t slot_size)
        : lane_(lane), slot_size_(slot_size) {
        if (lane.slots == 0 || slot_size_ == 0) fail("invalid pinned arena size");
        if (lane.slots > std::numeric_limits<size_t>::max() / slot_size_)
            fail("pinned arena size overflows size_t");
        bytes_ = lane.slots * slot_size_;
        mapping_ = ::mmap(nullptr, bytes_, PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (mapping_ == MAP_FAILED) {
            mapping_ = nullptr;
            fail("mmap pinned arena lane=" + std::to_string(lane.id) + ": " +
                 errno_string());
        }
        auto mapping_guard = make_scope_exit([&] {
            if (mapping_ != nullptr) {
                (void)::munmap(mapping_, bytes_);
                mapping_ = nullptr;
            }
        });

        constexpr size_t bits_per_word = sizeof(unsigned long) * CHAR_BIT;
        const size_t words = static_cast<size_t>(lane.numa) / bits_per_word + 1;
        std::vector<unsigned long> mask(words, 0);
        mask[static_cast<size_t>(lane.numa) / bits_per_word] |=
            1UL << (static_cast<size_t>(lane.numa) % bits_per_word);
        const unsigned long maxnode = static_cast<unsigned long>(words * bits_per_word);
        const long mbind_rc = ::syscall(SYS_mbind, mapping_, bytes_, MPOL_BIND,
                                        mask.data(), maxnode, MPOL_MF_STRICT);
        if (mbind_rc != 0)
            fail("mbind lane=" + std::to_string(lane.id) +
                 " node=" + std::to_string(lane.numa) + ": " + errno_string());

        // First-touch after mbind, while pinned to a CPU on the requested node.
        // cudaHostRegister follows placement; no pin/register operation occurs
        // after startup or anywhere on the receive hot path.
        cpu_set_t saved;
        CPU_ZERO(&saved);
        if (::sched_getaffinity(0, sizeof(saved), &saved) != 0)
            fail("sched_getaffinity before first touch: " + errno_string());
        cpu_set_t one;
        CPU_ZERO(&one);
        CPU_SET(lane.reactor_cpus.front(), &one);
        if (::sched_setaffinity(0, sizeof(one), &one) != 0)
            fail("sched_setaffinity for first touch: " + errno_string());
        auto affinity_guard = make_scope_exit([&] {
            (void)::sched_setaffinity(0, sizeof(saved), &saved);
        });
        const long page_size_long = ::sysconf(_SC_PAGESIZE);
        if (page_size_long <= 0) fail("sysconf(_SC_PAGESIZE) failed");
        const size_t page_size = static_cast<size_t>(page_size_long);
        volatile unsigned char *touch = static_cast<volatile unsigned char *>(mapping_);
        for (size_t offset = 0; offset < bytes_; offset += page_size) touch[offset] = 0;
        touch[bytes_ - 1] = 0;
        if (::sched_setaffinity(0, sizeof(saved), &saved) != 0)
            fail("restoring affinity after first touch: " + errno_string());
        affinity_guard.release();

        cuda_check(cudaSetDevice(lane.gpu), "cudaSetDevice(cudaHostRegister)");
        const cudaError_t status = cudaHostRegister(
            mapping_, bytes_, cudaHostRegisterPortable);
        if (status != cudaSuccess)
            fail("cudaHostRegister lane=" + std::to_string(lane.id) + ": " +
                 cuda_string(status));
        registered_ = true;
        mapping_guard.release();
    }

    ~PinnedArena() {
        if (registered_) {
            cuda_log_if_error(cudaSetDevice(lane_.gpu),
                              "cudaSetDevice(cudaHostUnregister)");
            const cudaError_t status = cudaHostUnregister(mapping_);
            if (status != cudaSuccess)
                print_line("ERROR cudaHostUnregister lane=", lane_.id,
                           ": ", cuda_string(status));
        }
        if (mapping_ != nullptr && ::munmap(mapping_, bytes_) != 0)
            print_line("ERROR munmap lane=", lane_.id, ": ", errno_string());
    }
    PinnedArena(const PinnedArena &) = delete;
    PinnedArena &operator=(const PinnedArena &) = delete;

    unsigned char *slot_base(size_t index) const {
        if (index >= lane_.slots) fail("pinned slot index out of bounds");
        return static_cast<unsigned char *>(mapping_) + index * slot_size_;
    }
    size_t slot_size() const { return slot_size_; }
    size_t slots() const { return lane_.slots; }
    size_t bytes() const { return bytes_; }

private:
    Lane lane_;
    size_t slot_size_ = 0;
    size_t bytes_ = 0;
    void *mapping_ = nullptr;
    bool registered_ = false;
};

enum class SlotState : uint8_t { FREE, RECEIVING, H2D_IN_FLIGHT };

struct Slot {
    unsigned char *base = nullptr;
    SlotState state = SlotState::FREE;
    cudaEvent_t event = nullptr;
    RangeTask *task = nullptr;
    uint64_t attempt = 0;
    size_t payload_offset = 0;
    size_t copy_bytes = 0;
    void *device_destination = nullptr;
};

class ReactorSlots {
public:
    static constexpr size_t kMaximumBatchSize = 64;

    ReactorSlots(PinnedArena &arena, size_t first, size_t count, int lane_id,
                 int gpu, bool receive_only, size_t h2d_batch_size,
                 RunStats &stats, FatalState &fatal)
        : slot_size_(arena.slot_size()), lane_id_(lane_id), gpu_(gpu),
          receive_only_(receive_only), stats_(stats), fatal_(fatal),
          inline_batch_limit_(std::min(h2d_batch_size, count)) {
        if (first + count > arena.slots()) fail("reactor slot partition is invalid");
        if (inline_batch_limit_ == 0) fail("inline H2D batch limit is zero");
        cuda_check(cudaSetDevice(gpu_), "cudaSetDevice(slot events)");
        slots_.resize(count);
        inline_pending_.resize(count, nullptr);
        inline_batches_.resize(count);
        try {
            for (size_t i = 0; i < count; ++i) {
                slots_[i].base = arena.slot_base(first + i);
                cuda_check(cudaEventCreateWithFlags(&slots_[i].event,
                                                    cudaEventDisableTiming),
                           "cudaEventCreateWithFlags(slot)");
            }
        } catch (...) {
            for (Slot &slot : slots_)
                if (slot.event != nullptr)
                    cuda_log_if_error(cudaEventDestroy(slot.event),
                                      "cudaEventDestroy(partial startup)");
            throw;
        }
    }
    ~ReactorSlots() {
        cuda_log_if_error(cudaSetDevice(gpu_), "cudaSetDevice(slot teardown)");
        for (Slot &slot : slots_)
            if (slot.event != nullptr)
                cuda_log_if_error(cudaEventDestroy(slot.event),
                                  "cudaEventDestroy(slot)");
    }
    ReactorSlots(const ReactorSlots &) = delete;
    ReactorSlots &operator=(const ReactorSlots &) = delete;

    Slot *acquire() {
        for (size_t i = 0; i < slots_.size(); ++i) {
            const size_t index = (next_ + i) % slots_.size();
            Slot &slot = slots_[index];
            if (slot.state == SlotState::FREE) {
                slot.state = SlotState::RECEIVING;
                next_ = (index + 1) % slots_.size();
                stats_.slot_acquired();
                return &slot;
            }
        }
        return nullptr;
    }

    bool has_free() const {
        return std::any_of(slots_.begin(), slots_.end(), [](const Slot &slot) {
            return slot.state == SlotState::FREE;
        });
    }

    bool has_in_flight() const {
        return inline_pending_count_ != 0;
    }

    void release_without_payload(Slot &slot) {
        if (slot.state != SlotState::RECEIVING) {
            fatal_.set("header-only slot release from invalid state");
            return;
        }
        slot.state = SlotState::FREE;
        stats_.slot_released();
    }

    bool submit(Slot &slot, size_t payload_offset, size_t payload_bytes,
                void *device_destination, RangeTask &task, cudaStream_t stream) {
        if (slot.state != SlotState::RECEIVING) {
            fatal_.set("H2D submission from a slot not in RECEIVING state");
            return false;
        }
        if (payload_bytes == 0 || payload_offset > slot_size_ ||
            payload_bytes > slot_size_ - payload_offset) {
            fatal_.set("H2D payload subrange escapes its pinned slot");
            return false;
        }
        unsigned char *source = slot.base + payload_offset;
        // This explicit bounds check documents and enforces the central
        // invariant: direct kTLS recvmsg() received into slot.base, and the
        // exact same registered allocation (optionally at a header offset) is
        // passed to the selected CUDA H2D API.  There is no intermediate
        // application plaintext buffer.
        if (source < slot.base || source + payload_bytes > slot.base + slot_size_) {
            fatal_.set("kTLS/H2D pinned-slot pointer identity check failed");
            return false;
        }
        if (receive_only_) {
            // Diagnostic sink: retain the exact NUMA-local, CUDA-registered
            // recvmsg arena, but issue no CUDA copy or event.  This isolates
            // RX/TCP/kTLS/HTTP throughput from H2D DMA and submission work.
            task.h2d_completed_attempt += payload_bytes;
            receive_only_bytes_ += payload_bytes;
            ++receive_only_chunks_;
            release_without_payload(slot);
            return true;
        }
        slot.state = SlotState::H2D_IN_FLIGHT;
        slot.task = &task;
        slot.attempt = task.attempt;
        slot.payload_offset = payload_offset;
        slot.copy_bytes = payload_bytes;
        slot.device_destination = device_destination;
        ++task.pending_h2d;
        stats_.h2d_submitted_bytes.fetch_add(payload_bytes);
        stats_.h2d_submitted_copies.fetch_add(1);
        stats_.gpu.at(gpu_)->outstanding_copies.fetch_add(1);
        if (!enqueue_inline(slot)) return false;
        if (inline_open_count_ >= inline_batch_limit_ &&
            !flush_inline(stream)) return false;
        return true;
    }

    bool flush_inline(cudaStream_t stream) {
        if (receive_only_ || inline_open_count_ == 0) return true;
        if (inline_batch_count_ >= inline_batches_.size() ||
            inline_pending_count_ < inline_open_count_) {
            fatal_.set("inline CUDA completion batch accounting overflow");
            return false;
        }
        const size_t marker_index =
            (inline_pending_head_ + inline_pending_count_ - 1) %
            inline_pending_.size();
        Slot *marker = inline_pending_[marker_index];
        if (marker == nullptr || marker->state != SlotState::H2D_IN_FLIGHT) {
            fatal_.set("inline CUDA completion batch marker is invalid");
            return false;
        }
        std::array<void *, kMaximumBatchSize> destinations{};
        std::array<const void *, kMaximumBatchSize> sources{};
        std::array<size_t, kMaximumBatchSize> sizes{};
        const size_t first_open =
            (inline_pending_head_ + inline_pending_count_ -
             inline_open_count_) % inline_pending_.size();
        for (size_t i = 0; i < inline_open_count_; ++i) {
            Slot *slot = inline_pending_[
                (first_open + i) % inline_pending_.size()];
            if (slot == nullptr || slot->state != SlotState::H2D_IN_FLIGHT) {
                fatal_.set("inline CUDA copy batch slot is invalid");
                return false;
            }
            destinations[i] = slot->device_destination;
            sources[i] = slot->base + slot->payload_offset;
            sizes[i] = slot->copy_bytes;
        }

        cudaError_t copy_status = cudaSuccess;
        if (inline_open_count_ == 1) {
            copy_status = cudaMemcpyAsync(
                destinations[0], sources[0], sizes[0],
                cudaMemcpyHostToDevice, stream);
        } else {
            cudaMemcpyAttributes attributes{};
            attributes.srcAccessOrder = cudaMemcpySrcAccessOrderStream;
            size_t first_attribute_index = 0;
            copy_status = cudaMemcpyBatchAsync(
                destinations.data(), sources.data(), sizes.data(),
                inline_open_count_, &attributes, &first_attribute_index, 1,
                stream);
        }
        if (copy_status != cudaSuccess) {
            stats_.cuda_errors.fetch_add(1);
            fatal_.set("same-reactor cudaMemcpyBatchAsync lane=" +
                       std::to_string(lane_id_) + ": " +
                       cuda_string(copy_status));
            fail_inline_open();
            return false;
        }
        const cudaError_t status = cudaEventRecord(marker->event, stream);
        if (status != cudaSuccess) {
            stats_.cuda_errors.fetch_add(1);
            fatal_.set("cudaEventRecord inline completion batch: " +
                       cuda_string(status));
            return false;
        }
        const size_t tail =
            (inline_batch_head_ + inline_batch_count_) % inline_batches_.size();
        inline_batches_[tail] = {marker, inline_open_count_};
        ++inline_batch_count_;
        inline_open_count_ = 0;
        stats_.h2d_inline_batches.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    bool reap() {
        bool freed = false;
        // One event marks the tail of each group of copies on this reactor's
        // stream.  Completion of that marker proves every slot in the group is
        // reusable, eliminating a cudaEventRecord/cudaEventQuery pair for each
        // individual 256 KiB copy.
        while (inline_batch_count_ != 0) {
            const InlineBatch batch = inline_batches_[inline_batch_head_];
            if (batch.marker == nullptr || batch.slot_count == 0) {
                fatal_.set("inline CUDA completion batch FIFO is inconsistent");
                break;
            }
            stats_.h2d_inline_event_queries.fetch_add(1,
                                                       std::memory_order_relaxed);
            const cudaError_t status = cudaEventQuery(batch.marker->event);
            if (status == cudaErrorNotReady) break;
            if (status != cudaSuccess) {
                stats_.cuda_errors.fetch_add(1);
                fatal_.set("cudaEventQuery: " + cuda_string(status));
                break;
            }
            pop_inline_batch();
            for (size_t i = 0; i < batch.slot_count; ++i) {
                if (inline_pending_count_ == 0) {
                    fatal_.set("inline CUDA batch exceeds pending-slot FIFO");
                    break;
                }
                Slot *slot = inline_pending_[inline_pending_head_];
                pop_inline();
                if (slot == nullptr || slot->state != SlotState::H2D_IN_FLIGHT) {
                    fatal_.set("inline CUDA pending slot is inconsistent");
                    continue;
                }
                finish_slot(*slot);
            }
            freed = true;
            if (fatal_.failed()) break;
        }
        return freed;
    }

    bool synchronize(cudaStream_t stream) {
        const cudaError_t status = cudaStreamSynchronize(stream);
        if (status != cudaSuccess) {
            stats_.cuda_errors.fetch_add(1);
            fatal_.set("cudaStreamSynchronize: " + cuda_string(status));
            return false;
        }
        reset_inline_pending();
        for (Slot &slot : slots_)
            if (slot.state == SlotState::H2D_IN_FLIGHT) finish_slot(slot);
        return true;
    }

    size_t size() const { return slots_.size(); }
    uint64_t receive_only_bytes() const { return receive_only_bytes_; }
    uint64_t receive_only_chunks() const { return receive_only_chunks_; }

    void reset_for_iteration() {
        for (const Slot &slot : slots_) {
            if (slot.state != SlotState::FREE || slot.task != nullptr ||
                slot.copy_bytes != 0 || slot.device_destination != nullptr)
                fail("cannot reset a reactor with a non-free pinned slot");
        }
        reset_inline_pending();
        next_ = 0;
        receive_only_bytes_ = 0;
        receive_only_chunks_ = 0;
    }

private:
    struct InlineBatch {
        Slot *marker = nullptr;
        size_t slot_count = 0;
    };

    bool enqueue_inline(Slot &slot) {
        if (inline_pending_count_ >= inline_pending_.size()) {
            // This cannot happen: a slot remains non-FREE while it is present
            // in the FIFO, so acquiring this RECEIVING slot proves capacity.
            // Keep the slot in-flight for synchronize() to reclaim safely.
            fatal_.set("inline CUDA completion FIFO overflow");
            return false;
        }
        const size_t tail =
            (inline_pending_head_ + inline_pending_count_) % inline_pending_.size();
        inline_pending_[tail] = &slot;
        ++inline_pending_count_;
        ++inline_open_count_;
        return true;
    }

    void pop_inline() {
        inline_pending_[inline_pending_head_] = nullptr;
        inline_pending_head_ = (inline_pending_head_ + 1) % inline_pending_.size();
        --inline_pending_count_;
        if (inline_pending_count_ == 0) inline_pending_head_ = 0;
    }

    void pop_inline_batch() {
        inline_batches_[inline_batch_head_] = {};
        inline_batch_head_ = (inline_batch_head_ + 1) % inline_batches_.size();
        --inline_batch_count_;
        if (inline_batch_count_ == 0) inline_batch_head_ = 0;
    }

    void reset_inline_pending() {
        std::fill(inline_pending_.begin(), inline_pending_.end(), nullptr);
        std::fill(inline_batches_.begin(), inline_batches_.end(), InlineBatch{});
        inline_pending_head_ = 0;
        inline_pending_count_ = 0;
        inline_batch_head_ = 0;
        inline_batch_count_ = 0;
        inline_open_count_ = 0;
    }

    void fail_inline_open() {
        const size_t count = inline_open_count_;
        const size_t first_open =
            (inline_pending_head_ + inline_pending_count_ - count) %
            inline_pending_.size();
        for (size_t i = 0; i < count; ++i) {
            const size_t index = (first_open + i) % inline_pending_.size();
            Slot *slot = inline_pending_[index];
            inline_pending_[index] = nullptr;
            if (slot != nullptr && slot->state == SlotState::H2D_IN_FLIGHT)
                fail_slot(*slot);
        }
        inline_pending_count_ -= count;
        inline_open_count_ = 0;
        if (inline_pending_count_ == 0) inline_pending_head_ = 0;
    }

    void clear_slot(Slot &slot) {
        slot.task = nullptr;
        slot.attempt = 0;
        slot.payload_offset = 0;
        slot.copy_bytes = 0;
        slot.device_destination = nullptr;
        slot.state = SlotState::FREE;
        stats_.slot_released();
    }

    void fail_slot(Slot &slot) {
        if (slot.task != nullptr && slot.task->pending_h2d != 0)
            --slot.task->pending_h2d;
        stats_.gpu.at(gpu_)->outstanding_copies.fetch_sub(1);
        clear_slot(slot);
    }

    void finish_slot(Slot &slot) {
        if (slot.task == nullptr || slot.task->pending_h2d == 0) {
            fatal_.set("completed CUDA slot lacks task accounting");
            return;
        }
        if (slot.task->attempt != slot.attempt) {
            fatal_.set("stale CUDA event completed after range retry began");
            return;
        }
        --slot.task->pending_h2d;
        slot.task->h2d_completed_attempt += slot.copy_bytes;
        stats_.h2d_completed_bytes.fetch_add(slot.copy_bytes);
        stats_.h2d_completed_copies.fetch_add(1);
        stats_.gpu.at(gpu_)->h2d_completed_bytes.fetch_add(slot.copy_bytes);
        stats_.gpu.at(gpu_)->outstanding_copies.fetch_sub(1);
        clear_slot(slot);
    }

    size_t slot_size_ = 0;
    int lane_id_ = -1;
    int gpu_ = -1;
    bool receive_only_ = false;
    RunStats &stats_;
    FatalState &fatal_;
    std::vector<Slot> slots_;
    std::vector<Slot *> inline_pending_;
    std::vector<InlineBatch> inline_batches_;
    size_t next_ = 0;
    size_t inline_pending_head_ = 0;
    size_t inline_pending_count_ = 0;
    size_t inline_batch_head_ = 0;
    size_t inline_batch_count_ = 0;
    size_t inline_open_count_ = 0;
    size_t inline_batch_limit_ = 0;
    uint64_t receive_only_bytes_ = 0;
    uint64_t receive_only_chunks_ = 0;
};

/* ================================================================
 * Per-CPU epoll reactor and persistent HTTP/1.1 connections
 * ================================================================ */

enum class ConnectionState : uint8_t {
    IDLE,
    TCP_CONNECTING,
    TLS_HANDSHAKE,
    WRITING_REQUEST,
    READING_HEADERS,
    READING_BODY,
    WAITING_H2D
};

enum class ReactorRunMode : uint8_t { PRECONNECT, PRIME, TRANSFER };

// Startup/tail diagnostics are written by exactly one pinned reactor thread.
// Give every reactor its own cache-line-aligned block so the instrumentation
// never performs a contended process-wide read/modify/write.  The observer may
// read these atomics every 100 ms; the final report performs the authoritative
// aggregation only after all reactor threads have joined.  Counters use a
// relaxed load+store (safe with one writer) rather than a locked fetch_add.
constexpr size_t kTelemetryCacheLine = 64;
struct alignas(kTelemetryCacheLine) ReactorTelemetryCounters {
    std::atomic<uint64_t> tls_ready{0};
    std::atomic<uint64_t> requests_sent{0};
    std::atomic<uint64_t> headers_validated{0};
    std::atomic<uint64_t> payload_connections{0};
    std::atomic<uint64_t> active_ranges{0};
    std::atomic<uint64_t> network_ranges_completed{0};
    std::atomic<uint64_t> device_ranges_completed{0};
    std::atomic<uint64_t> first_tls_ready_ns{0};
    std::atomic<uint64_t> first_request_ns{0};
    std::atomic<uint64_t> first_headers_ns{0};
    std::atomic<uint64_t> first_body_ns{0};
    std::atomic<uint64_t> last_body_ns{0};
    std::atomic<uint64_t> last_device_complete_ns{0};
};
static_assert(alignof(ReactorTelemetryCounters) >= kTelemetryCacheLine);
static_assert(sizeof(ReactorTelemetryCounters) % kTelemetryCacheLine == 0);

struct Connection {
    int id = -1;
    ConnectionState state = ConnectionState::IDLE;
    Fd fd;
    TlsRxState tls_rx;
    SSL *ssl = nullptr;
    bool epoll_registered = false;
    uint32_t epoll_events = 0;
    bool ever_opened = false;
    std::string peer_hostname;
    RangeTask *task = nullptr;
    std::string request;
    size_t request_offset = 0;
    HeaderAccumulator headers;
    bool close_after_response = false;
    bool ring_blocked = false;
    uint64_t ring_block_started_ns = 0;
    uint64_t address_sequence = 0;
    bool initial_tls_ready_recorded = false;
    unsigned preconnect_failures = 0;
    unsigned prime_failures = 0;
    Object *preconnect_target = nullptr;
    bool prime_complete = false;
    bool payload_active = false;
    bool range_active = false;

    // Direct RX keeps at most one partially filled pinned slot per
    // connection.  Retaining it across EAGAIN/epoll wakeups lets multiple TLS
    // records accumulate into one useful H2D copy without any payload memcpy.
    Slot *direct_fill_slot = nullptr;
    size_t direct_write_offset = 0;
    size_t direct_payload_offset = 0;
    bool direct_headers_complete = false;

    ~Connection() { if (ssl != nullptr) SSL_free(ssl); }
    Connection(const Connection &) = delete;
    Connection &operator=(const Connection &) = delete;
    Connection() = default;
};

class Reactor {
public:
    Reactor(const Lane &lane, size_t reactor_index, int cpu,
            std::vector<Object *> preconnect_targets, PinnedArena &arena,
            size_t first_slot, size_t slot_count, SSL_CTX *ssl_ctx,
            const Options &opt, const Credentials &credentials,
            Scheduler &scheduler, RunStats &stats, FatalState &fatal)
        : lane_(lane), reactor_index_(reactor_index), cpu_(cpu),
          ssl_ctx_(ssl_ctx), opt_(opt), credentials_(credentials),
          scheduler_(scheduler), stats_(stats), fatal_(fatal) {
        cuda_check(cudaSetDevice(lane_.gpu), "cudaSetDevice(reactor init)");
        cuda_check(cudaStreamCreateWithFlags(&stream_, cudaStreamNonBlocking),
                   "cudaStreamCreateWithFlags");
        auto stream_guard = make_scope_exit([&] {
            if (stream_ != nullptr) {
                cuda_log_if_error(cudaStreamDestroy(stream_),
                                  "cudaStreamDestroy(partial startup)");
                stream_ = nullptr;
            }
        });
        slots_ = std::make_unique<ReactorSlots>(
            arena, first_slot, slot_count, lane_.id, lane_.gpu,
            opt_.receive_only, opt_.h2d_batch_size, stats_, fatal_);
        epoll_.reset(::epoll_create1(EPOLL_CLOEXEC));
        if (!epoll_) fail("epoll_create1: " + errno_string());
        if (preconnect_targets.empty())
            fail("reactor must own at least one connection target");
        connections_.reserve(preconnect_targets.size());
        tls_ready_timestamps_.reserve(preconnect_targets.size());
        for (size_t i = 0; i < preconnect_targets.size(); ++i) {
            if (preconnect_targets[i] == nullptr)
                fail("reactor preconnect target is null");
            auto connection = std::make_unique<Connection>();
            connection->id = static_cast<int>(i);
            connection->preconnect_target = preconnect_targets[i];
            connection->address_sequence =
                static_cast<uint64_t>(lane_.id) * 1000003ULL +
                static_cast<uint64_t>(reactor_index_) * 1009ULL + i;
            connections_.push_back(std::move(connection));
        }
        stream_guard.release();
    }

    ~Reactor() {
        if (stream_ != nullptr) {
            cuda_log_if_error(cudaSetDevice(lane_.gpu),
                              "cudaSetDevice(reactor teardown)");
            if (slots_) (void)slots_->synchronize(stream_);
            cuda_log_if_error(cudaStreamDestroy(stream_), "cudaStreamDestroy");
        }
    }
    Reactor(const Reactor &) = delete;
    Reactor &operator=(const Reactor &) = delete;

    void start() { thread_ = std::thread([this] { run(); }); }
    void join() { if (thread_.joinable()) thread_.join(); }
    void prepare_for_preconnect() {
        if (thread_.joinable()) fail("cannot prepare a running reactor");
        for (const auto &holder : connections_) {
            const Connection &connection = *holder;
            if (connection.state != ConnectionState::IDLE || connection.fd ||
                connection.ssl != nullptr || connection.task != nullptr)
                fail("transport-only preconnect requires a fresh reactor");
        }
        run_mode_ = ReactorRunMode::PRECONNECT;
        reset_iteration_state(false);
    }
    void prepare_for_prime() {
        if (thread_.joinable()) fail("cannot prepare a running reactor");
        for (const auto &holder : connections_) {
            Connection &connection = *holder;
            if (connection.state != ConnectionState::IDLE || !connection.fd ||
                connection.ssl == nullptr || !connection.tls_rx.installed ||
                connection.task != nullptr ||
                connection.direct_fill_slot != nullptr ||
                connection.payload_active || connection.range_active ||
                connection.ring_blocked) {
                fail("HEAD prime requires a complete idle RX-kTLS pool");
            }
            connection.prime_failures = 0;
            connection.prime_complete = false;
            connection.request.clear();
            connection.request_offset = 0;
            connection.headers.reset();
            connection.close_after_response = false;
        }
        run_mode_ = ReactorRunMode::PRIME;
        reset_iteration_state(true);
    }
    void prepare_for_iteration() {
        if (thread_.joinable()) fail("cannot reset a running reactor");
        for (const auto &holder : connections_) {
            Connection &connection = *holder;
            if (connection.state != ConnectionState::IDLE ||
                connection.task != nullptr ||
                connection.direct_fill_slot != nullptr ||
                connection.payload_active || connection.range_active ||
                connection.ring_blocked)
                fail("cannot reset a reactor with active connection state");
        }
        run_mode_ = ReactorRunMode::TRANSFER;
        reset_iteration_state(true);
    }
    uint64_t preconnect_retry_count() const {
        uint64_t total = 0;
        for (const auto &connection : connections_)
            total += connection->preconnect_failures;
        return total;
    }
    uint64_t prime_retry_count() const {
        uint64_t total = 0;
        for (const auto &connection : connections_)
            total += connection->prime_failures;
        return total;
    }
    uint64_t prime_complete_count() const {
        return static_cast<uint64_t>(std::count_if(
            connections_.begin(), connections_.end(), [](const auto &connection) {
                return connection->prime_complete;
            }));
    }
    uint64_t cpu_time_ns() const { return cpu_time_ns_.load(); }
    uint64_t wall_time_ns() const { return wall_time_ns_.load(); }
    int lane_id() const { return lane_.id; }
    int cpu() const { return cpu_; }
    size_t index() const { return reactor_index_; }
    size_t connection_count() const { return connections_.size(); }

private:
    void reset_iteration_state(bool preserve_preconnect_failures) {
        // A retained transport has already passed TLS/kTLS/no-pad audit.
        // Resetting this flag makes telemetry count only handshakes performed
        // by the phase that follows this reset.
        for (const auto &holder : connections_) {
            holder->initial_tls_ready_recorded = false;
            if (!preserve_preconnect_failures) holder->preconnect_failures = 0;
        }
        slots_->reset_for_iteration();
        reset_telemetry_counters();
        tls_ready_timestamps_.clear();
        direct_recv_counters_ = {};
        ktls_recv_calls_published_.store(0, std::memory_order_relaxed);
        ktls_recv_eagain_published_.store(0, std::memory_order_relaxed);
        cpu_time_ns_.store(0, std::memory_order_relaxed);
        wall_time_ns_.store(0, std::memory_order_relaxed);
        epoll_ctl_calls_ = 0;
        epoll_ctl_skips_ = 0;
        body_seen_ = false;
    }

public:
    uint64_t epoll_ctl_calls() const { return epoll_ctl_calls_; }
    uint64_t epoll_ctl_skips() const { return epoll_ctl_skips_; }
    uint64_t ktls_recv_calls() const {
        return ktls_recv_calls_published_.load(std::memory_order_relaxed);
    }
    uint64_t ktls_recv_eagain() const {
        return ktls_recv_eagain_published_.load(std::memory_order_relaxed);
    }
    uint64_t receive_only_bytes() const {
        return slots_->receive_only_bytes();
    }
    uint64_t receive_only_chunks() const {
        return slots_->receive_only_chunks();
    }
    const ReactorTelemetryCounters &telemetry_counters() const {
        return telemetry_counters_;
    }
    const std::vector<uint64_t> &tls_ready_timestamps() const {
        return tls_ready_timestamps_;
    }
    uint64_t live_connection_count() const {
        return static_cast<uint64_t>(std::count_if(
            connections_.begin(), connections_.end(),
            [](const auto &connection) { return bool(connection->fd); }));
    }
    uint64_t live_ktls_connection_count() const {
        return static_cast<uint64_t>(std::count_if(
            connections_.begin(), connections_.end(), [](const auto &connection) {
                return bool(connection->fd) && connection->ssl != nullptr &&
                       connection->tls_rx.installed;
            }));
    }

private:
    static void increment_local(std::atomic<uint64_t> &counter) {
        counter.store(counter.load(std::memory_order_relaxed) + 1,
                      std::memory_order_relaxed);
    }

    static void set_first_local(std::atomic<uint64_t> &timestamp,
                                uint64_t value) {
        if (timestamp.load(std::memory_order_relaxed) == 0)
            timestamp.store(value, std::memory_order_relaxed);
    }

    void reset_telemetry_counters() {
        const auto zero = [](std::atomic<uint64_t> &value) {
            value.store(0, std::memory_order_relaxed);
        };
        zero(telemetry_counters_.tls_ready);
        zero(telemetry_counters_.requests_sent);
        zero(telemetry_counters_.headers_validated);
        zero(telemetry_counters_.payload_connections);
        zero(telemetry_counters_.active_ranges);
        zero(telemetry_counters_.network_ranges_completed);
        zero(telemetry_counters_.device_ranges_completed);
        zero(telemetry_counters_.first_tls_ready_ns);
        zero(telemetry_counters_.first_request_ns);
        zero(telemetry_counters_.first_headers_ns);
        zero(telemetry_counters_.first_body_ns);
        zero(telemetry_counters_.last_body_ns);
        zero(telemetry_counters_.last_device_complete_ns);
    }

    void begin_active_range(Connection &connection) {
        if (connection.range_active) {
            fatal_.set("connection attempted to activate two ranges");
            return;
        }
        connection.range_active = true;
        increment_local(telemetry_counters_.active_ranges);
    }

    void end_active_range(Connection &connection) {
        if (!connection.range_active) return;
        connection.range_active = false;
        const uint64_t current = telemetry_counters_.active_ranges.load(
            std::memory_order_relaxed);
        if (current == 0) {
            fatal_.set("per-reactor active-range counter underflow");
            return;
        }
        telemetry_counters_.active_ranges.store(current - 1,
                                                 std::memory_order_relaxed);
    }

    void publish_direct_recv_counters() {
        ktls_recv_calls_published_.store(direct_recv_counters_.calls,
                                         std::memory_order_relaxed);
        ktls_recv_eagain_published_.store(direct_recv_counters_.eagain,
                                          std::memory_order_relaxed);
    }

    void arm(Connection &connection, uint32_t io_events) {
        if (!connection.fd) return;
        epoll_event event{};
        event.events = io_events | EPOLLERR | EPOLLRDHUP;
        event.data.ptr = &connection;
        // Level-triggered interest persists.  Reaffirming EPOLLIN after a
        // recvmsg EAGAIN or a non-final slot submission does not require an
        // EPOLL_CTL_MOD syscall; at high throughput those no-op modifications
        // otherwise consume a material amount of system CPU.
        if (connection.epoll_registered &&
            connection.epoll_events == event.events) {
            ++epoll_ctl_skips_;
            return;
        }
        const int operation = connection.epoll_registered ? EPOLL_CTL_MOD
                                                          : EPOLL_CTL_ADD;
        if (::epoll_ctl(epoll_.get(), operation, connection.fd.get(), &event) != 0)
            fail("epoll_ctl lane=" + std::to_string(lane_.id) +
                 " reactor=" + std::to_string(reactor_index_) + ": " +
                 errno_string());
        ++epoll_ctl_calls_;
        connection.epoll_registered = true;
        connection.epoll_events = event.events;
    }

    void clear_direct_fill_metadata(Connection &connection) {
        connection.direct_fill_slot = nullptr;
        connection.direct_write_offset = 0;
        connection.direct_payload_offset = 0;
        connection.direct_headers_complete = false;
    }

    void discard_direct_fill(Connection &connection) {
        if (connection.direct_fill_slot != nullptr)
            slots_->release_without_payload(*connection.direct_fill_slot);
        clear_direct_fill_metadata(connection);
    }

    void begin_payload_phase(Connection &connection) {
        if (connection.payload_active) return;
        connection.payload_active = true;
        increment_local(telemetry_counters_.payload_connections);
    }

    void end_payload_phase(Connection &connection) {
        if (!connection.payload_active) return;
        connection.payload_active = false;
        const uint64_t current = telemetry_counters_.payload_connections.load(
            std::memory_order_relaxed);
        if (current == 0) {
            fatal_.set("per-reactor payload-connection counter underflow");
            return;
        }
        telemetry_counters_.payload_connections.store(
            current - 1, std::memory_order_relaxed);
    }

    void close_transport(Connection &connection) {
        // A failed/closed response may own a partially filled RECEIVING slot.
        // It contains only an uncommitted attempt and must be returned before
        // the whole idempotent range is retried.
        discard_direct_fill(connection);
        end_payload_phase(connection);
        end_active_range(connection);
        if (connection.ring_blocked) end_ring_block(connection);
        if (connection.epoll_registered && connection.fd) {
            (void)::epoll_ctl(epoll_.get(), EPOLL_CTL_DEL, connection.fd.get(), nullptr);
            ++epoll_ctl_calls_;
            connection.epoll_registered = false;
            connection.epoll_events = 0;
        }
        if (connection.ssl != nullptr) {
            SSL_free(connection.ssl);
            connection.ssl = nullptr;
        }
        connection.tls_rx.clear();
        if (connection.fd) {
            connection.fd.reset();
            stats_.active_connections.fetch_sub(1);
            stats_.lane.at(static_cast<size_t>(lane_.id))->active_connections.fetch_sub(1);
        }
        connection.peer_hostname.clear();
    }

    void begin_tls(Connection &connection) {
        if (connection.peer_hostname.empty())
            fail("TLS handshake has no peer hostname");
        connection.ssl = make_ssl(ssl_ctx_, connection.fd.get(),
                                  connection.peer_hostname,
                                  connection.tls_rx);
        connection.state = ConnectionState::TLS_HANDSHAKE;
        arm(connection, EPOLLIN | EPOLLOUT);
    }

    void open_transport(Connection &connection, const Object &object) {
        const SocketAddress &address = object.endpoint->addresses[
            connection.address_sequence++ % object.endpoint->addresses.size()];
        if (connection.ever_opened) stats_.reconnects.fetch_add(1);
        connection.ever_opened = true;
        bool connected = false;
        connection.fd = make_bound_socket(lane_.nic, address, connected);
        connection.peer_hostname = object.target.hostname;
        stats_.active_connections.fetch_add(1);
        stats_.lane.at(static_cast<size_t>(lane_.id))->active_connections.fetch_add(1);
        connection.state = connected ? ConnectionState::TLS_HANDSHAKE
                                     : ConnectionState::TCP_CONNECTING;
        if (connected) begin_tls(connection);
        else arm(connection, EPOLLOUT);
    }

    void assign_task(Connection &connection) {
        if (connection.task != nullptr || connection.state != ConnectionState::IDLE)
            return;
        RangeTask *task = scheduler_.pop(lane_.id);
        if (task == nullptr) {
            if (connection.fd) arm(connection, EPOLLIN);
            return;
        }
        connection.task = task;
        connection.request_offset = 0;
        connection.headers.reset();
        connection.close_after_response = false;
        if (task->received != 0 || task->pending_h2d != 0 ||
            task->h2d_completed_attempt != 0) {
            fatal_.set("assigned range has stale per-attempt counters");
            return;
        }
        const uint64_t end = task->start + task->length - 1;
        const std::optional<std::string> if_match =
            object_etag_for_request(*task->object);
        connection.request = make_signed_request(
            "GET", task->object->target,
            std::make_pair(task->start, end), if_match,
            opt_.region, credentials_, true);
        if (connection.fd &&
            connection.peer_hostname == task->object->target.hostname) {
            connection.state = ConnectionState::WRITING_REQUEST;
            arm(connection, EPOLLOUT);
        } else {
            if (connection.fd) close_transport(connection);
            open_transport(connection, *task->object);
        }
        begin_active_range(connection);
    }

    void assign_prime(Connection &connection) {
        if (connection.state != ConnectionState::IDLE ||
            connection.prime_complete)
            return;
        if (connection.task != nullptr || connection.preconnect_target == nullptr) {
            fatal_.set("HEAD prime encountered invalid connection ownership");
            return;
        }
        Object &object = *connection.preconnect_target;
        connection.request = make_signed_request(
            "HEAD", object.target, std::nullopt, std::nullopt,
            opt_.region, credentials_, true);
        connection.request_offset = 0;
        connection.headers.reset();
        connection.close_after_response = false;
        if (connection.fd &&
            connection.peer_hostname == object.target.hostname) {
            connection.state = ConnectionState::WRITING_REQUEST;
            arm(connection, EPOLLOUT);
        } else {
            if (connection.fd) close_transport(connection);
            open_transport(connection, object);
        }
    }

    void start_ring_block(Connection &connection) {
        if (!connection.ring_blocked) {
            connection.ring_blocked = true;
            connection.ring_block_started_ns = now_ns();
            arm(connection, 0);
        }
    }

    void end_ring_block(Connection &connection) {
        if (!connection.ring_blocked) return;
        const uint64_t end = now_ns();
        if (end >= connection.ring_block_started_ns)
            stats_.ring_stall_ns.fetch_add(end - connection.ring_block_started_ns);
        connection.ring_blocked = false;
        connection.ring_block_started_ns = 0;
    }

    void network_retry(Connection &connection, const std::string &why,
                       bool tls_error = false) {
        if (tls_error) stats_.tls_errors.fetch_add(1);
        RangeTask *task = connection.task;
        close_transport(connection);
        if (task != nullptr) {
            // A partial failed attempt may already have H2D work queued.
            // Before reissuing the whole idempotent range, synchronize this
            // reactor's stream and reap every event.  This prevents an old
            // attempt from racing a retry that overwrites the same offsets.
            if (!slots_->synchronize(stream_)) return;
            scheduler_.retry(*task, opt_.max_retries, why);
        } else if (run_mode_ == ReactorRunMode::PRECONNECT ||
                   run_mode_ == ReactorRunMode::PRIME) {
            unsigned &failures = run_mode_ == ReactorRunMode::PRECONNECT
                ? connection.preconnect_failures : connection.prime_failures;
            ++failures;
            connection.prime_complete = false;
            if (failures > opt_.max_retries) {
                fatal_.set(std::string(
                               run_mode_ == ReactorRunMode::PRECONNECT
                                   ? "preconnect" : "HEAD prime") +
                           " lane=" + std::to_string(lane_.id) +
                           " reactor=" + std::to_string(reactor_index_) +
                           " connection=" + std::to_string(connection.id) +
                           " exhausted retries: " + why);
            }
        }
        connection.task = nullptr;
        connection.request.clear();
        connection.request_offset = 0;
        connection.headers.reset();
        connection.state = ConnectionState::IDLE;
    }

    void protocol_failure(Connection &connection, const std::string &why) {
        stats_.http_errors.fetch_add(1);
        fatal_.set("lane=" + std::to_string(lane_.id) +
                   " reactor=" + std::to_string(reactor_index_) +
                   " connection=" + std::to_string(connection.id) + ": " + why);
    }

    bool handle_ssl_non_success(Connection &connection, int rc,
                                const std::string &operation,
                                uint32_t default_interest) {
        const int error = SSL_get_error(connection.ssl, rc);
        if (error == SSL_ERROR_WANT_READ) {
            stats_.want_read.fetch_add(1);
            arm(connection, EPOLLIN);
            return false;
        }
        if (error == SSL_ERROR_WANT_WRITE) {
            stats_.want_write.fetch_add(1);
            arm(connection, EPOLLOUT);
            return false;
        }
        if (error == SSL_ERROR_ZERO_RETURN || error == SSL_ERROR_SYSCALL) {
            const std::string detail = error == SSL_ERROR_ZERO_RETURN
                ? "TLS close_notify"
                : (errno != 0 ? errno_string() : "unexpected EOF");
            network_retry(connection, operation + ": " + detail,
                          connection.state == ConnectionState::TLS_HANDSHAKE);
            return false;
        }
        (void)default_interest;
        stats_.tls_errors.fetch_add(1);
        const long verify = SSL_get_verify_result(connection.ssl);
        if (verify != X509_V_OK) {
            fatal_.set(operation + ": TLS authentication failed: " +
                std::string(X509_verify_cert_error_string(verify)));
        } else {
            fatal_.set(operation + ": OpenSSL error=" + std::to_string(error) +
                       ": " + openssl_errors());
        }
        return false;
    }

    bool finish_tcp_connect(Connection &connection) {
        int error = 0;
        socklen_t length = sizeof(error);
        if (::getsockopt(connection.fd.get(), SOL_SOCKET, SO_ERROR,
                         &error, &length) != 0) {
            network_retry(connection, "getsockopt(SO_ERROR): " + errno_string());
            return false;
        }
        if (error != 0) {
            network_retry(connection, "TCP connect: " + errno_string(error));
            return false;
        }
        begin_tls(connection);
        return true;
    }

    bool drive_handshake(Connection &connection) {
        ERR_clear_error();
        const int rc = SSL_connect(connection.ssl);
        if (rc != 1)
            return handle_ssl_non_success(connection, rc, "SSL_connect",
                                          EPOLLIN | EPOLLOUT);
        audit_tls_connection(
            connection.ssl, connection.fd.get(), stats_,
            "lane=" + std::to_string(lane_.id) +
                " reactor=" + std::to_string(reactor_index_) +
                " conn=" + std::to_string(connection.id),
            connection.tls_rx);
        if (!connection.initial_tls_ready_recorded) {
            connection.initial_tls_ready_recorded = true;
            const uint64_t ready_ns = now_ns();
            tls_ready_timestamps_.push_back(ready_ns);
            increment_local(telemetry_counters_.tls_ready);
            set_first_local(telemetry_counters_.first_tls_ready_ns, ready_ns);
        }
        if (run_mode_ == ReactorRunMode::PRECONNECT) {
            if (connection.task != nullptr || !connection.request.empty()) {
                fatal_.set("transport-only preconnect acquired HTTP work");
                return false;
            }
            connection.state = ConnectionState::IDLE;
            // Do not consume post-handshake records here.  They remain in the
            // kernel TLS socket and are handled by the direct recvmsg path
            // after the first timed request.  Keeping EPOLLIN disarmed also
            // prevents an idle NewSessionTicket from being mistaken for an
            // unsolicited application response.
            arm(connection, 0);
        } else if (run_mode_ == ReactorRunMode::PRIME) {
            if (connection.task != nullptr || connection.request.empty() ||
                connection.prime_complete) {
                fatal_.set("HEAD-prime TLS handshake completed without a request");
                return false;
            }
            connection.state = ConnectionState::WRITING_REQUEST;
            arm(connection, EPOLLOUT);
        } else {
            if (connection.task == nullptr || connection.request.empty()) {
                fatal_.set("transfer TLS handshake completed without a request");
                return false;
            }
            connection.state = ConnectionState::WRITING_REQUEST;
            arm(connection, EPOLLOUT);
        }
        return true;
    }

    bool drive_write(Connection &connection) {
        if (connection.request_offset >= connection.request.size()) {
            connection.headers.reset();
            connection.state = ConnectionState::READING_HEADERS;
            arm(connection, EPOLLIN);
            return true;
        }
        size_t written = 0;
        ERR_clear_error();
        const int rc = SSL_write_ex(connection.ssl,
            connection.request.data() + connection.request_offset,
            connection.request.size() - connection.request_offset, &written);
        if (rc != 1 && connection.tls_rx.installed &&
            SSL_get_error(connection.ssl, rc) == SSL_ERROR_WANT_READ) {
            network_retry(connection,
                "SSL_write_ex requested an OpenSSL read after direct RX handoff",
                true);
            return false;
        }
        if (rc != 1)
            return handle_ssl_non_success(connection, rc, "SSL_write_ex", EPOLLOUT);
        if (written == 0) {
            fatal_.set("SSL_write_ex succeeded without progress");
            return false;
        }
        connection.request_offset += written;
        if (connection.request_offset == connection.request.size()) {
            const uint64_t request_ns = now_ns();
            increment_local(telemetry_counters_.requests_sent);
            set_first_local(telemetry_counters_.first_request_ns, request_ns);
            connection.headers.reset();
            connection.state = ConnectionState::READING_HEADERS;
            arm(connection, EPOLLIN);
        }
        return true;
    }

    bool validate_range_headers(Connection &connection) {
        const ParsedResponse &response = connection.headers.parsed();
        if (response.status != 206) {
            const std::string detail =
                "Range GET returned HTTP " + std::to_string(response.status) +
                " " + response.reason + "; S3 headers: " +
                s3_diagnostic_headers(response);
            print_line("HTTP_ERROR range=", connection.task->id, " ", detail);
            stats_.http_errors.fetch_add(1);
            if (response.status == 429 || response.status == 500 ||
                response.status == 502 || response.status == 503 ||
                response.status == 504) {
                network_retry(connection, detail);
            } else {
                fatal_.set(detail + " (redirects are not followed)");
            }
            return false;
        }
        if (response.transfer_encoding_present) {
            protocol_failure(connection,
                "Transfer-Encoding is rejected; S3 Range GET must have Content-Length");
            return false;
        }
        if (!response.content_length || response.count("content-length") != 1 ||
            *response.content_length != connection.task->length) {
            protocol_failure(connection,
                "response must have one Content-Length equal to requested range length");
            return false;
        }
        const auto content_range_value = response.get("content-range");
        if (!content_range_value || response.count("content-range") != 1) {
            protocol_failure(connection,
                "206 response must contain exactly one Content-Range");
            return false;
        }
        ContentRange range;
        try {
            range = parse_content_range(*content_range_value);
        } catch (const std::exception &e) {
            protocol_failure(connection, e.what());
            return false;
        }
        const uint64_t expected_end = connection.task->start +
                                      connection.task->length - 1;
        if (range.start != connection.task->start || range.end != expected_end ||
            range.total != connection.task->object->size) {
            protocol_failure(connection,
                "Content-Range does not exactly match requested start/end/object size");
            return false;
        }
        const auto etag = response.get("etag");
        if (!etag || response.count("etag") != 1 || etag->empty() ||
            etag->find_first_of("\r\n") != std::string::npos) {
            protocol_failure(connection,
                "Range response must contain exactly one valid ETag");
            return false;
        }
        if (!validate_or_learn_object_etag(
                *connection.task->object, *etag)) {
            protocol_failure(connection,
                "Range response ETag differs from the object-version snapshot");
            return false;
        }
        const auto encoding = response.get("content-encoding");
        if (response.count("content-encoding") > 1) {
            protocol_failure(connection, "duplicate Content-Encoding headers");
            return false;
        }
        if (encoding && lower(trim(*encoding)) != "identity") {
            protocol_failure(connection,
                "unexpected Content-Encoding despite Accept-Encoding: identity");
            return false;
        }
        connection.close_after_response = response.connection_close;
        const uint64_t headers_ns = now_ns();
        increment_local(telemetry_counters_.headers_validated);
        set_first_local(telemetry_counters_.first_headers_ns, headers_ns);
        begin_payload_phase(connection);
        return true;
    }

    bool validate_prime_headers(Connection &connection) {
        if (connection.preconnect_target == nullptr) {
            fatal_.set("HEAD prime lost its object target");
            return false;
        }
        const ParsedResponse &response = connection.headers.parsed();
        if (response.status != 200) {
            const std::string detail =
                "HEAD prime returned HTTP " + std::to_string(response.status) +
                " " + response.reason + "; S3 headers: " +
                s3_diagnostic_headers(response);
            print_line("HTTP_ERROR prime=head_object ", detail);
            stats_.http_errors.fetch_add(1);
            if (response.status == 429 || response.status == 500 ||
                response.status == 502 || response.status == 503 ||
                response.status == 504) {
                network_retry(connection, detail);
            } else {
                fatal_.set(detail + " (redirects are not followed)");
            }
            return false;
        }
        if (response.transfer_encoding_present) {
            protocol_failure(connection,
                "HEAD prime response unexpectedly used Transfer-Encoding");
            return false;
        }
        Object &object = *connection.preconnect_target;
        if (!response.content_length || response.count("content-length") != 1 ||
            *response.content_length != object.size) {
            protocol_failure(connection,
                "HEAD prime must return one Content-Length equal to catalog size");
            return false;
        }
        const auto etag = response.get("etag");
        if (!etag || response.count("etag") != 1 || etag->empty() ||
            etag->find_first_of("\r\n") != std::string::npos) {
            protocol_failure(connection,
                "HEAD prime must return exactly one valid ETag");
            return false;
        }
        const std::optional<std::string> catalog_etag =
            object_etag_for_request(object);
        if (object.etag_from_catalog &&
            (!catalog_etag || *catalog_etag != *etag)) {
            protocol_failure(connection,
                "HEAD prime ETag differs from the catalog snapshot");
            return false;
        }
        const auto encoding = response.get("content-encoding");
        if (response.count("content-encoding") > 1) {
            protocol_failure(connection,
                "HEAD prime returned duplicate Content-Encoding headers");
            return false;
        }
        if (encoding && lower(trim(*encoding)) != "identity") {
            protocol_failure(connection,
                "HEAD prime returned an unexpected Content-Encoding");
            return false;
        }
        if (response.connection_close) {
            network_retry(connection,
                "HEAD prime response requested Connection: close");
            return false;
        }
        increment_local(telemetry_counters_.headers_validated);
        set_first_local(telemetry_counters_.first_headers_ns, now_ns());
        connection.prime_complete = true;
        connection.request.clear();
        connection.request_offset = 0;
        connection.headers.reset();
        connection.close_after_response = false;
        connection.state = ConnectionState::IDLE;
        arm(connection, 0);
        return true;
    }

    bool drive_prime_read(Connection &connection) {
        if (!connection.tls_rx.installed) {
            fatal_.set("HEAD-prime read attempted before RX kTLS activation");
            return false;
        }
        for (unsigned record = 0; record < 256; ++record) {
            DirectTlsReadResult read = direct_tls_recv(
                connection.fd.get(), connection.tls_rx,
                prime_read_buffer_.data(), prime_read_buffer_.size(), stats_,
                &direct_recv_counters_);
            if (read.status == DirectTlsReadStatus::WANT_READ) {
                arm(connection, EPOLLIN);
                return false;
            }
            if (read.status == DirectTlsReadStatus::CONTROL_CONSUMED) continue;
            if (read.status == DirectTlsReadStatus::PEER_CLOSED ||
                read.status == DirectTlsReadStatus::CONNECTION_ERROR) {
                network_retry(connection,
                    "HEAD-prime direct kTLS receive: " + read.detail, true);
                return false;
            }
            size_t body_offset = 0;
            bool complete = false;
            try {
                complete = connection.headers.consume(
                    prime_read_buffer_.data(), read.bytes, body_offset);
            } catch (const std::exception &e) {
                protocol_failure(connection, e.what());
                return false;
            }
            if (!complete) continue;
            if (body_offset != read.bytes) {
                protocol_failure(connection,
                    "HEAD prime returned application payload bytes");
                return false;
            }
            (void)validate_prime_headers(connection);
            return false;
        }
        arm(connection, EPOLLIN);
        return false;
    }

    bool submit_payload(Connection &connection, Slot &slot,
                        size_t payload_offset, size_t payload_bytes) {
        RangeTask &task = *connection.task;
        if (payload_bytes > task.length - task.received) {
            slots_->release_without_payload(slot);
            protocol_failure(connection, "response body exceeds Content-Length/range");
            return false;
        }
        const uint64_t object_offset = task.start + task.received;
        if (object_offset > task.object->size ||
            payload_bytes > task.object->size - object_offset) {
            slots_->release_without_payload(slot);
            protocol_failure(connection, "device destination escapes final object allocation");
            return false;
        }
        auto *device = static_cast<unsigned char *>(task.object->device.pointer) +
                       object_offset;
        if (!slots_->submit(slot, payload_offset, payload_bytes, device, task, stream_))
            return false;
        task.received += payload_bytes;
        const bool first_body_for_reactor = !body_seen_;
        const bool range_finished = task.received == task.length;
        stats_.body_bytes.fetch_add(payload_bytes, std::memory_order_relaxed);
        uint64_t milestone_ns = 0;
        if (first_body_for_reactor || range_finished)
            milestone_ns = now_ns();
        if (first_body_for_reactor) {
            body_seen_ = true;
            telemetry_counters_.first_body_ns.store(
                milestone_ns, std::memory_order_relaxed);
        }
        LaneCounters &lane = *stats_.lane.at(static_cast<size_t>(lane_.id));
        lane.body_bytes.fetch_add(payload_bytes, std::memory_order_relaxed);
        if (range_finished) {
            if (!connection.tls_rx.installed && SSL_pending(connection.ssl) > 0) {
                fatal_.set("OpenSSL has unexpected application bytes beyond "
                           "the validated Range Content-Length");
                return false;
            }
            telemetry_counters_.last_body_ns.store(
                milestone_ns, std::memory_order_relaxed);
            increment_local(telemetry_counters_.network_ranges_completed);
            end_payload_phase(connection);
            end_active_range(connection);
            scheduler_.received_all(task);
            connection.state = ConnectionState::WAITING_H2D;
            arm(connection, 0);
        } else {
            connection.state = ConnectionState::READING_BODY;
            arm(connection, EPOLLIN);
        }
        return true;
    }

    /*
     * A typed kTLS recvmsg normally returns one plaintext TLS record.  Sending
     * every ~16 KiB record to CUDA separately would make CUDA submission, not
     * ENA/TCP/TLS, the benchmark.  Fill one registered slot with a sequence of
     * recvmsg calls and submit its contiguous application-data subrange once.
     *
     * recvmsg always writes at slot.base + write_offset.  Control records are
     * consumed in place without advancing write_offset, so the next
     * application record simply overwrites those non-payload bytes.  The HTTP
     * header may occupy a prefix of the first slot; payload after CRLFCRLF and
     * all following records remain contiguous, and no header-stripping copy is
     * needed.
     */
    bool drive_direct_read(Connection &connection) {
        constexpr unsigned max_records_per_fill = 256;
        if (connection.direct_fill_slot == nullptr) {
            fatal_.set("direct receive entered without an owned pinned slot");
            return false;
        }
        Slot &slot = *connection.direct_fill_slot;
        size_t &write_offset = connection.direct_write_offset;
        size_t &payload_offset = connection.direct_payload_offset;
        bool &headers_complete = connection.direct_headers_complete;

        for (unsigned record = 0; record < max_records_per_fill; ++record) {
            const size_t capacity = slots_slot_size() - write_offset;
            if (capacity < kTlsPlaintextRecordMax) break;

            DirectTlsReadResult read;
            try {
                read = direct_tls_recv(
                    connection.fd.get(), connection.tls_rx,
                    slot.base + write_offset, capacity, stats_,
                    &direct_recv_counters_);
            } catch (...) {
                discard_direct_fill(connection);
                throw;
            }
            if (read.status == DirectTlsReadStatus::WANT_READ) {
                /*
                 * Do not turn every brief socket drain into a small CUDA
                 * copy.  Keep one bounded RECEIVING slot attached to this
                 * connection and append more records on its next epoll wake.
                 * Header-only bytes are already copied into the bounded HTTP
                 * accumulator, so retaining a slot is useful only after body
                 * payload has begun.
                 */
                if (!headers_complete || write_offset == payload_offset)
                    discard_direct_fill(connection);
                arm(connection, EPOLLIN);
                return false;
            }
            if (read.status == DirectTlsReadStatus::CONTROL_CONSUMED) {
                // The control plaintext was written at write_offset but is not
                // object data.  Do not advance; the next recvmsg overwrites it.
                continue;
            }
            if (read.status == DirectTlsReadStatus::PEER_CLOSED ||
                read.status == DirectTlsReadStatus::CONNECTION_ERROR) {
                network_retry(connection, "direct kTLS receive: " + read.detail,
                              true);
                return false;
            }

            const size_t chunk_offset = write_offset;
            write_offset += read.bytes;
            if (!headers_complete) {
                size_t body_in_chunk = 0;
                bool complete = false;
                try {
                    complete = connection.headers.consume(
                        slot.base + chunk_offset, read.bytes, body_in_chunk);
                } catch (const std::exception &e) {
                    discard_direct_fill(connection);
                    protocol_failure(connection, e.what());
                    return false;
                }
                if (!complete) continue;
                if (!validate_range_headers(connection)) {
                    // A retrying status closes the transport and discards the
                    // fill inside validate_range_headers/network_retry.  A
                    // fatal protocol status leaves it for us to release.
                    if (connection.direct_fill_slot != nullptr)
                        discard_direct_fill(connection);
                    return false;
                }
                headers_complete = true;
                payload_offset = chunk_offset + body_in_chunk;
                connection.state = ConnectionState::READING_BODY;
            }

            if (connection.task->received > connection.task->length) {
                discard_direct_fill(connection);
                protocol_failure(connection,
                                 "range received-byte accounting overflow");
                return false;
            }
            const uint64_t remaining = connection.task->length -
                                       connection.task->received;
            const size_t payload_bytes = write_offset - payload_offset;
            if (payload_bytes > remaining) {
                discard_direct_fill(connection);
                protocol_failure(connection,
                                 "response body exceeds Content-Length/range");
                return false;
            }
            if (payload_bytes == remaining) break;
        }

        if (!headers_complete || write_offset == payload_offset) {
            // Header-only/control-only progress is already retained in the
            // bounded header/control accumulator, so this slot can be reused.
            discard_direct_fill(connection);
            arm(connection, EPOLLIN);
            return true;
        }

        // Stop this service turn after one aggregated CUDA submission.  Level
        // triggered epoll will immediately report more socket data, while
        // other ready connections still get a chance to run.
        const size_t copy_offset = payload_offset;
        const size_t copy_bytes = write_offset - payload_offset;
        clear_direct_fill_metadata(connection);
        (void)submit_payload(connection, slot, copy_offset, copy_bytes);
        return false;
    }

    bool drive_read(Connection &connection) {
        if (!connection.tls_rx.installed) {
            fatal_.set("read attempted before mandatory RX kTLS activation");
            return false;
        }
        if (connection.direct_fill_slot == nullptr) {
            connection.direct_fill_slot = slots_->acquire();
            if (connection.direct_fill_slot == nullptr) {
                start_ring_block(connection);
                return false;
            }
            connection.direct_write_offset = 0;
            connection.direct_payload_offset = 0;
            connection.direct_headers_complete =
                connection.state == ConnectionState::READING_BODY;
        }
        return drive_direct_read(connection);
    }

    size_t slots_slot_size() const {
        // Every reactor partition has the arena's uniform slot size.  The
        // receive capacity is recovered from configuration to keep Slot small.
        return static_cast<size_t>(opt_.slot_bytes);
    }

    void finish_waiting(Connection &connection) {
        if (connection.state != ConnectionState::WAITING_H2D ||
            connection.task == nullptr) return;
        if (!scheduler_.complete_if_ready(*connection.task)) return;
        increment_local(telemetry_counters_.device_ranges_completed);
        telemetry_counters_.last_device_complete_ns.store(
            now_ns(), std::memory_order_relaxed);
        connection.task = nullptr;
        connection.request.clear();
        connection.request_offset = 0;
        connection.headers.reset();
        connection.state = ConnectionState::IDLE;
        if (connection.close_after_response) close_transport(connection);
        else if (connection.fd) arm(connection, EPOLLIN);
    }

    bool drive(Connection &connection) {
        switch (connection.state) {
            case ConnectionState::IDLE:
                if (run_mode_ == ReactorRunMode::PRIME)
                    assign_prime(connection);
                else if (run_mode_ == ReactorRunMode::TRANSFER)
                    assign_task(connection);
                return connection.state != ConnectionState::IDLE;
            case ConnectionState::TCP_CONNECTING:
                return finish_tcp_connect(connection);
            case ConnectionState::TLS_HANDSHAKE:
                return drive_handshake(connection);
            case ConnectionState::WRITING_REQUEST:
                return drive_write(connection);
            case ConnectionState::READING_HEADERS:
            case ConnectionState::READING_BODY:
                return run_mode_ == ReactorRunMode::PRIME
                    ? drive_prime_read(connection) : drive_read(connection);
            case ConnectionState::WAITING_H2D:
                finish_waiting(connection);
                return false;
        }
        return false;
    }

    void service(Connection &connection, uint32_t events) {
        if (connection.state == ConnectionState::IDLE &&
            (events & EPOLLIN) != 0) {
            // No request is outstanding, so readable application/TLS closure
            // data is unsolicited.  Drop the idle persistent transport and
            // reconnect when another task arrives; this also avoids a
            // level-triggered epoll loop on an unread close_notify.
            if (run_mode_ == ReactorRunMode::PRECONNECT ||
                run_mode_ == ReactorRunMode::PRIME)
                network_retry(connection,
                    run_mode_ == ReactorRunMode::PRECONNECT
                        ? "preconnected socket became readable while idle"
                        : "HEAD-primed socket became readable while idle");
            else
                close_transport(connection);
            return;
        }
        if ((events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0) {
            if (connection.state == ConnectionState::WAITING_H2D ||
                connection.state == ConnectionState::IDLE) {
                if ((run_mode_ == ReactorRunMode::PRECONNECT ||
                     run_mode_ == ReactorRunMode::PRIME) &&
                    connection.state == ConnectionState::IDLE)
                    network_retry(connection,
                        run_mode_ == ReactorRunMode::PRECONNECT
                            ? "preconnected socket closed while idle"
                            : "HEAD-primed socket closed while idle");
                else
                    close_transport(connection);
                return;
            }
        }
        for (unsigned iteration = 0; iteration < 64 && !fatal_.failed(); ++iteration) {
            const ConnectionState before = connection.state;
            if (!drive(connection)) break;
            if (connection.ring_blocked || connection.state == ConnectionState::WAITING_H2D ||
                connection.state == ConnectionState::IDLE) break;
            // A state transition can continue immediately.  Repeating a read
            // in the same state is also safe on a nonblocking SSL object; WANT
            // will arm epoll and stop this loop.
            (void)before;
        }
        if ((events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0 &&
            connection.fd &&
            connection.state == ConnectionState::WAITING_H2D) {
            // The complete response is already validated; preserve its task
            // while discarding a transport that closed after the body.
            close_transport(connection);
        } else if ((events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0 &&
            connection.fd &&
            (connection.task != nullptr ||
             run_mode_ == ReactorRunMode::PRECONNECT ||
             run_mode_ == ReactorRunMode::PRIME) &&
            connection.state != ConnectionState::WAITING_H2D && !fatal_.failed()) {
            network_retry(connection, "epoll reported socket close/error");
        }
    }

    void run() noexcept {
        timespec cpu_start{};
        timespec cpu_end{};
        const uint64_t wall_start = now_ns();
        try {
            pin_this_thread(cpu_, "reactor affinity");
            cuda_check(cudaSetDevice(lane_.gpu), "cudaSetDevice(reactor thread)");
            if (::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu_start) != 0)
                fail("clock_gettime(CLOCK_THREAD_CPUTIME_ID): " + errno_string());
            std::array<epoll_event, 128> events{};
            if (run_mode_ == ReactorRunMode::PRECONNECT) {
                while (!fatal_.failed() &&
                       live_ktls_connection_count() != connections_.size()) {
                    for (auto &holder : connections_) {
                        Connection &connection = *holder;
                        if (connection.state == ConnectionState::IDLE &&
                            !connection.fd) {
                            if (connection.preconnect_target == nullptr)
                                fail("connection lost its preconnect target");
                            open_transport(connection,
                                           *connection.preconnect_target);
                        }
                    }
                    const int count = ::epoll_wait(
                        epoll_.get(), events.data(),
                        static_cast<int>(events.size()), 25);
                    if (count < 0) {
                        if (errno == EINTR) continue;
                        fail("epoll_wait(preconnect): " + errno_string());
                    }
                    for (int i = 0; i < count && !fatal_.failed(); ++i) {
                        auto *connection = static_cast<Connection *>(
                            events[i].data.ptr);
                        if (connection != nullptr)
                            service(*connection, events[i].events);
                    }
                }
            } else if (run_mode_ == ReactorRunMode::PRIME) {
                while (!fatal_.failed() &&
                       prime_complete_count() != connections_.size()) {
                    for (auto &holder : connections_)
                        if (holder->state == ConnectionState::IDLE &&
                            !holder->prime_complete)
                            assign_prime(*holder);
                    const int count = ::epoll_wait(
                        epoll_.get(), events.data(),
                        static_cast<int>(events.size()), 25);
                    if (count < 0) {
                        if (errno == EINTR) continue;
                        fail("epoll_wait(HEAD prime): " + errno_string());
                    }
                    for (int i = 0; i < count && !fatal_.failed(); ++i) {
                        auto *connection = static_cast<Connection *>(
                            events[i].data.ptr);
                        if (connection != nullptr)
                            service(*connection, events[i].events);
                    }
                    publish_direct_recv_counters();
                }
            } else {
                while (!fatal_.failed() && scheduler_.remaining() != 0) {
                    (void)slots_->reap();
                    for (auto &holder : connections_) {
                        Connection &connection = *holder;
                        if (connection.ring_blocked && slots_->has_free()) {
                            end_ring_block(connection);
                            arm(connection, EPOLLIN);
                        }
                        finish_waiting(connection);
                    }
                    for (auto &holder : connections_)
                        if (holder->state == ConnectionState::IDLE)
                            assign_task(*holder);

                    const bool fast_poll = slots_->has_in_flight() ||
                        std::any_of(connections_.begin(), connections_.end(),
                            [](const auto &c) { return c->ring_blocked; });
                    const int timeout_ms = fast_poll ? 1 : 25;
                    const int count = ::epoll_wait(
                        epoll_.get(), events.data(),
                        static_cast<int>(events.size()), timeout_ms);
                    if (count < 0) {
                        if (errno == EINTR) continue;
                        fail("epoll_wait: " + errno_string());
                    }
                    for (int i = 0; i < count && !fatal_.failed(); ++i) {
                        auto *connection = static_cast<Connection *>(
                            events[i].data.ptr);
                        if (connection != nullptr)
                            service(*connection, events[i].events);
                    }
                    if (!fatal_.failed() && !slots_->flush_inline(stream_))
                        break;
                    publish_direct_recv_counters();
                }
            }
            if (run_mode_ == ReactorRunMode::TRANSFER)
                (void)slots_->synchronize(stream_);
            for (auto &holder : connections_) {
                if (run_mode_ == ReactorRunMode::TRANSFER)
                    finish_waiting(*holder);
                if (!fatal_.failed()) {
                    if (holder->state != ConnectionState::IDLE ||
                        holder->task != nullptr) {
                        fatal_.set("completed reactor retained non-idle task state");
                    } else if (run_mode_ == ReactorRunMode::PRECONNECT &&
                               (!holder->fd || holder->ssl == nullptr ||
                                !holder->tls_rx.installed)) {
                        fatal_.set("preconnect reactor retained an incomplete transport");
                    } else if (run_mode_ == ReactorRunMode::PRIME &&
                               (!holder->prime_complete || !holder->fd ||
                                holder->ssl == nullptr ||
                                !holder->tls_rx.installed)) {
                        fatal_.set("HEAD-prime reactor retained an incomplete pool");
                    }
                }
            }
            // Successful iterations deliberately retain idle HTTP/1.1
            // transports.  A following in-process iteration can therefore
            // measure a database-style persistent pool without repeating TCP/TLS.
            // On failure, close everything so no partially valid pool can be
            // mistaken for reusable state.
            if (fatal_.failed())
                for (auto &holder : connections_) close_transport(*holder);
            if (::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu_end) != 0)
                fail("clock_gettime(CLOCK_THREAD_CPUTIME_ID): " + errno_string());
        } catch (const std::exception &e) {
            fatal_.set("reactor lane=" + std::to_string(lane_.id) +
                       " index=" + std::to_string(reactor_index_) + ": " + e.what());
            if (run_mode_ == ReactorRunMode::TRANSFER)
                (void)slots_->synchronize(stream_);
            for (auto &holder : connections_) close_transport(*holder);
            (void)::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu_end);
        }
        for (auto &holder : connections_)
            if (holder->ring_blocked) end_ring_block(*holder);
        publish_direct_recv_counters();
        const uint64_t cpu_start_ns = static_cast<uint64_t>(cpu_start.tv_sec) *
                                      1000000000ULL + cpu_start.tv_nsec;
        const uint64_t cpu_end_ns = static_cast<uint64_t>(cpu_end.tv_sec) *
                                    1000000000ULL + cpu_end.tv_nsec;
        cpu_time_ns_.store(cpu_end_ns >= cpu_start_ns ? cpu_end_ns - cpu_start_ns : 0);
        wall_time_ns_.store(now_ns() - wall_start);
    }

    Lane lane_;
    size_t reactor_index_ = 0;
    int cpu_ = -1;
    SSL_CTX *ssl_ctx_ = nullptr;
    const Options &opt_;
    const Credentials &credentials_;
    Scheduler &scheduler_;
    RunStats &stats_;
    FatalState &fatal_;
    Fd epoll_;
    cudaStream_t stream_ = nullptr;
    std::unique_ptr<ReactorSlots> slots_;
    std::vector<std::unique_ptr<Connection>> connections_;
    std::thread thread_;
    std::atomic<uint64_t> cpu_time_ns_{0};
    std::atomic<uint64_t> wall_time_ns_{0};
    uint64_t epoll_ctl_calls_ = 0;
    uint64_t epoll_ctl_skips_ = 0;
    DirectTlsReadCounters direct_recv_counters_;
    std::array<unsigned char, kTlsPlaintextRecordMax> prime_read_buffer_{};
    std::atomic<uint64_t> ktls_recv_calls_published_{0};
    std::atomic<uint64_t> ktls_recv_eagain_published_{0};
    ReactorTelemetryCounters telemetry_counters_;
    std::vector<uint64_t> tls_ready_timestamps_;
    ReactorRunMode run_mode_ = ReactorRunMode::TRANSFER;
    bool body_seen_ = false;
};

struct LaneTelemetryAggregate {
    uint64_t first_body_ns = 0;
    uint64_t last_body_ns = 0;
    uint64_t last_device_complete_ns = 0;
    uint64_t network_ranges_completed = 0;
    uint64_t device_ranges_completed = 0;
};

struct ReactorTelemetryAggregate {
    uint64_t tls_ready = 0;
    uint64_t requests_sent = 0;
    uint64_t headers_validated = 0;
    uint64_t payload_connections = 0;
    uint64_t active_ranges = 0;
    uint64_t network_ranges_completed = 0;
    uint64_t device_ranges_completed = 0;
    uint64_t first_tls_ready_ns = 0;
    uint64_t first_request_ns = 0;
    uint64_t first_headers_ns = 0;
    uint64_t first_body_ns = 0;
    uint64_t last_body_ns = 0;
    uint64_t last_device_complete_ns = 0;
    std::vector<LaneTelemetryAggregate> lane;
    std::vector<uint64_t> tls_ready_timestamps;
};

void aggregate_first_timestamp(uint64_t &aggregate, uint64_t candidate) {
    if (candidate != 0 && (aggregate == 0 || candidate < aggregate))
        aggregate = candidate;
}

ReactorTelemetryAggregate aggregate_reactor_telemetry(
        const std::vector<std::unique_ptr<Reactor>> &reactors,
        size_t lane_count, bool collect_tls_timestamps) {
    ReactorTelemetryAggregate result;
    result.lane.resize(lane_count);
    if (collect_tls_timestamps) {
        size_t connection_count = 0;
        for (const auto &reactor : reactors)
            connection_count += reactor->connection_count();
        result.tls_ready_timestamps.reserve(connection_count);
    }
    for (const auto &reactor : reactors) {
        const ReactorTelemetryCounters &counters =
            reactor->telemetry_counters();
        const auto load = [](const std::atomic<uint64_t> &value) {
            return value.load(std::memory_order_relaxed);
        };
        const uint64_t reactor_tls_ready = load(counters.tls_ready);
        const uint64_t reactor_requests = load(counters.requests_sent);
        const uint64_t reactor_headers = load(counters.headers_validated);
        const uint64_t reactor_payload = load(counters.payload_connections);
        const uint64_t reactor_active = load(counters.active_ranges);
        const uint64_t reactor_network = load(counters.network_ranges_completed);
        const uint64_t reactor_device = load(counters.device_ranges_completed);
        const uint64_t reactor_first_tls = load(counters.first_tls_ready_ns);
        const uint64_t reactor_first_request = load(counters.first_request_ns);
        const uint64_t reactor_first_headers = load(counters.first_headers_ns);
        const uint64_t reactor_first_body = load(counters.first_body_ns);
        const uint64_t reactor_last_body = load(counters.last_body_ns);
        const uint64_t reactor_last_device =
            load(counters.last_device_complete_ns);

        result.tls_ready += reactor_tls_ready;
        result.requests_sent += reactor_requests;
        result.headers_validated += reactor_headers;
        result.payload_connections += reactor_payload;
        result.active_ranges += reactor_active;
        result.network_ranges_completed += reactor_network;
        result.device_ranges_completed += reactor_device;
        aggregate_first_timestamp(result.first_tls_ready_ns, reactor_first_tls);
        aggregate_first_timestamp(result.first_request_ns, reactor_first_request);
        aggregate_first_timestamp(result.first_headers_ns, reactor_first_headers);
        aggregate_first_timestamp(result.first_body_ns, reactor_first_body);
        result.last_body_ns = std::max(result.last_body_ns, reactor_last_body);
        result.last_device_complete_ns = std::max(
            result.last_device_complete_ns, reactor_last_device);

        LaneTelemetryAggregate &lane = result.lane.at(
            static_cast<size_t>(reactor->lane_id()));
        aggregate_first_timestamp(lane.first_body_ns, reactor_first_body);
        lane.last_body_ns = std::max(lane.last_body_ns, reactor_last_body);
        lane.last_device_complete_ns = std::max(
            lane.last_device_complete_ns, reactor_last_device);
        lane.network_ranges_completed += reactor_network;
        lane.device_ranges_completed += reactor_device;

        if (collect_tls_timestamps) {
            const std::vector<uint64_t> &timestamps =
                reactor->tls_ready_timestamps();
            result.tls_ready_timestamps.insert(
                result.tls_ready_timestamps.end(), timestamps.begin(),
                timestamps.end());
        }
    }
    if (collect_tls_timestamps)
        std::sort(result.tls_ready_timestamps.begin(),
                  result.tls_ready_timestamps.end());
    return result;
}

uint64_t live_reactor_connections(
        const std::vector<std::unique_ptr<Reactor>> &reactors) {
    uint64_t total = 0;
    for (const auto &reactor : reactors)
        total += reactor->live_connection_count();
    return total;
}

uint64_t live_reactor_ktls_connections(
        const std::vector<std::unique_ptr<Reactor>> &reactors) {
    uint64_t total = 0;
    for (const auto &reactor : reactors)
        total += reactor->live_ktls_connection_count();
    return total;
}

uint64_t total_preconnect_retries(
        const std::vector<std::unique_ptr<Reactor>> &reactors) {
    uint64_t total = 0;
    for (const auto &reactor : reactors)
        total += reactor->preconnect_retry_count();
    return total;
}

uint64_t total_prime_retries(
        const std::vector<std::unique_ptr<Reactor>> &reactors) {
    uint64_t total = 0;
    for (const auto &reactor : reactors)
        total += reactor->prime_retry_count();
    return total;
}

uint64_t total_prime_completions(
        const std::vector<std::unique_ptr<Reactor>> &reactors) {
    uint64_t total = 0;
    for (const auto &reactor : reactors)
        total += reactor->prime_complete_count();
    return total;
}

/* ================================================================
 * Telemetry, CONFIG, final validation, and RESULT_JSON
 * ================================================================ */

uint64_t total_ktls_recv_calls(
        const RunStats &stats,
        const std::vector<std::unique_ptr<Reactor>> &reactors) {
    uint64_t total = stats.ktls_recv_calls.load(std::memory_order_relaxed);
    for (const auto &reactor : reactors) total += reactor->ktls_recv_calls();
    return total;
}

uint64_t total_ktls_recv_eagain(
        const RunStats &stats,
        const std::vector<std::unique_ptr<Reactor>> &reactors) {
    uint64_t total = stats.ktls_recv_eagain.load(std::memory_order_relaxed);
    for (const auto &reactor : reactors) total += reactor->ktls_recv_eagain();
    return total;
}

uint64_t nic_rx_bytes(const std::string &nic) {
    return read_u64_file("/sys/class/net/" + nic + "/statistics/rx_bytes");
}

std::map<std::string, uint64_t> sample_nic_rx(const std::vector<Lane> &lanes) {
    std::map<std::string, uint64_t> result;
    for (const Lane &lane : lanes)
        if (result.count(lane.nic) == 0) result[lane.nic] = nic_rx_bytes(lane.nic);
    return result;
}

int choose_telemetry_cpu(const std::vector<Lane> &lanes) {
    std::set<int> busy;
    for (const Lane &lane : lanes)
        busy.insert(lane.reactor_cpus.begin(), lane.reactor_cpus.end());
    cpu_set_t allowed;
    CPU_ZERO(&allowed);
    if (::sched_getaffinity(0, sizeof(allowed), &allowed) != 0)
        fail("sched_getaffinity(telemetry CPU): " + errno_string());
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu)
        if (CPU_ISSET(cpu, &allowed) && busy.count(cpu) == 0) return cpu;
    return -1;
}

std::string hostname_string() {
    std::array<char, 256> name{};
    if (::gethostname(name.data(), name.size()) != 0)
        fail("gethostname: " + errno_string());
    name.back() = '\0';
    return name.data();
}

std::string kernel_string() {
    utsname info{};
    if (::uname(&info) != 0) fail("uname: " + errno_string());
    return std::string(info.sysname) + " " + info.release + " " + info.machine;
}

std::string cuda_version_string(int value) {
    const int major = value / 1000;
    const int minor = (value % 1000) / 10;
    return std::to_string(major) + "." + std::to_string(minor);
}

void print_config(const Options &opt, const std::vector<NicInfo> &nics,
                  const std::vector<GpuInfo> &gpus, const std::vector<Lane> &lanes,
                  const std::vector<std::unique_ptr<Object>> &objects,
                  const Credentials &credentials,
                  const std::vector<std::string> &endpoint_overrides,
                  int telemetry_cpu,
                  int cuda_driver, int cuda_runtime, bool tls_stats_available,
                  uint64_t mapped_bytes, uint64_t registered_bytes) {
    uint64_t total_bytes = 0;
    uint64_t total_slots = 0;
    uint64_t total_connections = 0;
    uint64_t total_reactors = 0;
    std::map<int, uint64_t> planned;
    std::map<int, uint64_t> planned_objects;
    std::map<int, uint64_t> planned_ranges;
    std::set<std::string> endpoints;
    for (const auto &object : objects) {
        total_bytes += object->size;
        planned[object->gpu] += object->size;
        planned_objects[object->gpu] += 1;
        planned_ranges[object->gpu] += object->tasks.size();
        endpoints.insert(object->target.hostname);
    }
    for (const Lane &lane : lanes) {
        total_slots += lane.slots;
        total_connections += lane.connections;
        total_reactors += lane.reactor_cpus.size();
    }

    print_line("===== CONFIG =====");
    print_line("host=", hostname_string(), " kernel=", kernel_string());
    print_line("openssl=", OpenSSL_version(OPENSSL_VERSION),
               " cuda_runtime=", cuda_version_string(cuda_runtime),
               " cuda_driver=", cuda_version_string(cuda_driver));
#ifdef OPENSSL_NO_KTLS
    print_line("openssl_ktls_compile_support=NO");
#else
    print_line("openssl_ktls_compile_support=yes");
#endif
    print_line("credentials_source=", credentials.source,
               " session_token=", credentials.session_token.empty() ? "no" : "yes",
               credentials.expiration.empty() ? "" : " expiration=",
               credentials.expiration.empty() ? "" : credentials.expiration);
    print_line("discovered_nics=", nics.size());
    for (const NicInfo &nic : nics)
        print_line("NIC name=", nic.name, " driver=", nic.driver,
                   " driver_version=", nic.driver_version,
                   " large_rx_page=", nic.large_rx_page,
                   " numa=", nic.numa,
                   " mtu=", nic.mtu, " rx_queues=", nic.rx_queues,
                   " tx_queues=", nic.tx_queues,
                   " pci_device=", nic.has_pci_device ? "yes" : "no");
    print_line("discovered_gpus=", gpus.size());
    for (const GpuInfo &gpu : gpus)
        print_line("GPU id=", gpu.id, " name=", gpu.name,
                   " pci=", gpu.pci_bus_id, " numa=", gpu.numa,
                   " total_GiB=", std::fixed, std::setprecision(2),
                   static_cast<double>(gpu.total_bytes) / GiB,
                   " free_at_discovery_GiB=", static_cast<double>(gpu.free_bytes) / GiB);
    for (const Lane &lane : lanes)
        print_line("LANE id=", lane.id, " nic=", lane.nic,
                   " nic_numa=", lane.nic_numa, " gpu=", lane.gpu,
                   " gpu_numa=", lane.gpu_numa, " arena_numa=", lane.numa,
                   " reactor_cpus=", join_ints(lane.reactor_cpus),
                   " reactors=", lane.reactor_cpus.size(),
                   " connections=", lane.connections, " slots=", lane.slots,
                   " slots_per_reactor=", lane.slots / lane.reactor_cpus.size(),
                   " cross_numa=",
                   (lane.nic_numa >= 0 && lane.gpu_numa >= 0 &&
                    lane.nic_numa != lane.gpu_numa) ? "WARNING" : "no");
    print_line("slot_bytes=", opt.slot_bytes,
               " slot_KiB=", static_cast<double>(opt.slot_bytes) / KiB,
               " slot_MiB=", static_cast<double>(opt.slot_bytes) / MiB,
               " pinned_hwm_requested_bytes=", opt.pinned_hwm_bytes,
               " pinned_mmap_bytes=", mapped_bytes,
               " pinned_registered_bytes=", registered_bytes,
               " slots_total=", total_slots,
               " slots_in_use=0 pinned_peak_slots=0");
    print_line("range_bytes=", opt.range_bytes,
               " range_MiB=", static_cast<double>(opt.range_bytes) / MiB,
               " connections_total=", total_connections,
               " reactors_total=", total_reactors,
               " measured_iterations=", opt.iterations,
               " scan_warmup_iterations=0",
               " connection_pool_preconnect=transport_only",
               " preconnect_http_requests=0 preconnect_payload_bytes=0",
               " connection_pool_prime=head_object",
               " prime_requests_per_connection=1 prime_payload_bytes=0",
               " persistent_pool_across_iterations=yes",
               " gpu_reserve_bytes=", opt.gpu_reserve_bytes,
               " max_retries=", opt.max_retries);
    print_line("payload_sink=", opt.receive_only ? "receive_only" : "h2d",
               " h2d_mode=",
               opt.receive_only ? "disabled" : "same_reactor_batch",
               " h2d_batch_size=", opt.h2d_batch_size);
    print_line("telemetry_observer_cpu=", telemetry_cpu,
               " reactor_cpu_shared=", telemetry_cpu >= 0 ? "no" : "WARNING");
    print_line("catalog_snapshot=", opt.catalog_snapshot,
               " metadata_model=database_metastore_snapshot",
               " size_metadata=all catalog_head_requests=0",
               " query_head_requests=0");
    print_line("objects=", objects.size(), " expected_bytes=", total_bytes);
    for (const auto &[gpu, bytes] : planned)
        print_line("GPU_PLAN gpu=", gpu, " objects=", planned_objects[gpu],
                   " ranges=", planned_ranges[gpu], " bytes=", bytes);
    std::ostringstream endpoint_text;
    bool first = true;
    for (const std::string &endpoint : endpoints) {
        if (!first) endpoint_text << ',';
        first = false;
        endpoint_text << endpoint;
    }
    print_line("endpoint_hostnames=", endpoint_text.str(),
               " endpoint_ip_override=", endpoint_overrides.empty() ? "no" : "yes",
               " override_ip_count=", endpoint_overrides.size());
    print_line("tls_required=TLSv1.3 ciphers=TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384",
               " ktls_required=yes",
               " rx_activation=openssl_keylog_plus_linux_TLS_RX",
               " rx_api=recvmsg_TLS_GET_RECORD_TYPE",
               " TLS_RX_EXPECT_NO_PAD_requested=yes",
               " no_pad_required=yes",
               " proc_tls_stat=", tls_stats_available ? "available" : "UNAVAILABLE");
    if (opt.receive_only) {
        print_line("tcp_receive_autotuning_preserved=yes SO_RCVBUF_set=no",
                   " payload_path=recvmsg(CUDA_registered_pinned_slot)->release",
                   " cuda_copy_submissions=0",
                   " gpu_object_allocations=retained_but_unwritten",
                   " openssl_payload_record_buffer_bypassed=yes",
                   " diagnostic=receive_only_NONPRODUCTION");
    } else {
        print_line("tcp_receive_autotuning_preserved=yes SO_RCVBUF_set=no",
                   " payload_path=recvmsg(pinned_slot)->",
                   "cudaMemcpyBatchAsync(same_reactor)",
                   "(same_slot)->cudaMalloc_offset",
                   " openssl_payload_record_buffer_bypassed=yes",
                   " slot_reuse=cuda_event_gated");
    }
    print_line("==================");
}

struct RampSample {
    uint64_t elapsed_ms = 0;
    uint64_t tls_ready = 0;
    uint64_t requests_sent = 0;
    uint64_t headers_validated = 0;
    uint64_t payload_connections = 0;
    uint64_t active_ranges = 0;
    uint64_t body_bytes = 0;
};

class Telemetry {
public:
    Telemetry(const std::vector<Lane> &lanes,
              const std::vector<std::unique_ptr<Reactor>> &reactors,
              RunStats &stats,
              Clock::time_point start, uint64_t configured_connections,
              int cpu)
        : lanes_(lanes), reactors_(reactors), stats_(stats), start_(start),
          configured_connections_(configured_connections), cpu_(cpu) {
        for (const Lane &lane : lanes_) total_slots_ += lane.slots;
    }
    ~Telemetry() { stop(); }
    void start() { thread_ = std::thread([this] { run(); }); }
    void stop() {
        stop_.store(true);
        if (thread_.joinable()) thread_.join();
    }
    const std::vector<RampSample> &ramp_samples() const {
        return ramp_samples_;
    }

private:
    void run() noexcept {
        try {
            if (cpu_ >= 0)
                pin_this_thread(cpu_, "telemetry observer affinity");
            auto previous_time = start_;
            uint64_t previous_body = 0;
            uint64_t previous_h2d = 0;
            uint64_t previous_h2d_copies = 0;
            uint64_t previous_want_read = stats_.want_read.load();
            uint64_t previous_want_write = stats_.want_write.load();
            uint64_t previous_ktls_eagain =
                total_ktls_recv_eagain(stats_, reactors_);
            std::vector<uint64_t> previous_lane_body(lanes_.size(), 0);
            std::map<int, uint64_t> previous_gpu_h2d;
            std::map<std::string, uint64_t> previous_nic = sample_nic_rx(lanes_);
            // Wake cheaply at 100 ms during the run.  Only atomic counters are
            // sampled at that cadence; NIC sysfs reads and console output stay
            // at one second.  Retain the first three seconds, which covers the
            // observed TLS/range ramp without producing live 10 Hz logging.
            uint64_t tick = 0;
            auto next = start_ + std::chrono::milliseconds(100);
            while (!stop_.load()) {
                std::this_thread::sleep_until(next);
                if (stop_.load()) break;
                const auto now = Clock::now();
                ++tick;
                if (tick <= 30) {
                    // This observer-side reduction only reads each reactor's
                    // isolated block; reactor CPUs never update a shared ramp
                    // counter and never format telemetry on the data path.
                    const ReactorTelemetryAggregate aggregate =
                        aggregate_reactor_telemetry(
                            reactors_, lanes_.size(), false);
                    RampSample sample;
                    sample.elapsed_ms = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            now - start_).count());
                    sample.tls_ready = aggregate.tls_ready;
                    sample.requests_sent = aggregate.requests_sent;
                    sample.headers_validated = aggregate.headers_validated;
                    sample.payload_connections = aggregate.payload_connections;
                    sample.active_ranges = aggregate.active_ranges;
                    sample.body_bytes = stats_.body_bytes.load(
                        std::memory_order_relaxed);
                    ramp_samples_.push_back(sample);
                }
                if (tick % 10 != 0) {
                    next += std::chrono::milliseconds(100);
                    if (next < now) next = now + std::chrono::milliseconds(100);
                    continue;
                }
                const double interval = std::chrono::duration<double>(now - previous_time).count();
                const double elapsed = std::chrono::duration<double>(now - start_).count();
                const uint64_t body = stats_.body_bytes.load();
                const uint64_t h2d = stats_.h2d_completed_bytes.load();
                const uint64_t h2d_copies = stats_.h2d_completed_copies.load();
                const uint64_t want_read = stats_.want_read.load();
                const uint64_t want_write = stats_.want_write.load();
                const uint64_t ktls_eagain =
                    total_ktls_recv_eagain(stats_, reactors_);
                const std::map<std::string, uint64_t> current_nic = sample_nic_rx(lanes_);
                const double ring_percent = elapsed > 0.0 && configured_connections_ != 0
                    ? static_cast<double>(stats_.ring_stall_ns.load()) /
                          (elapsed * 1.0e9 * static_cast<double>(configured_connections_)) * 100.0
                    : 0.0;
                std::ostringstream line;
                line << std::fixed << std::setprecision(2)
                     << "T elapsed_s=" << elapsed
                     // These are aggregate deltas across every lane/GPU for
                     // this observer interval.  Keep that scope in the field
                     // names so they cannot be mistaken for the per-entity,
                     // whole-run rates printed in the final summary.
                     << " s3_interval_agg_gbps="
                     << gbps(body - previous_body, interval)
                     << " h2d_interval_agg_gbps="
                     << gbps(h2d - previous_h2d, interval)
                     << " h2d_copy_avg_KiB="
                     << (h2d_copies > previous_h2d_copies
                             ? static_cast<double>(h2d - previous_h2d) /
                                   static_cast<double>(h2d_copies -
                                                       previous_h2d_copies) / KiB
                             : 0.0)
                     << " total_body_GB=" << decimal_gb(body)
                     << " total_h2d_GB=" << decimal_gb(h2d)
                     << " active_connections=" << stats_.active_connections.load()
                     << " completed_ranges=" << stats_.completed_ranges.load()
                     << " retry_count=" << stats_.retries.load()
                     << " pinned_slots_used=" << stats_.pinned_used.load()
                     << " pinned_slots_total=" << total_slots_
                     << " pinned_peak=" << stats_.pinned_peak.load()
                     << " ring_full_percent=" << ring_percent
                     << " ssl_want_read=" << want_read
                     << " ssl_want_read_s=" << static_cast<double>(want_read - previous_want_read) / interval
                     << " ssl_want_write=" << want_write
                     << " ssl_want_write_s=" << static_cast<double>(want_write - previous_want_write) / interval
                     << " ktls_eagain=" << ktls_eagain
                     << " ktls_eagain_s=" << static_cast<double>(
                            ktls_eagain - previous_ktls_eagain) / interval;
                for (const Lane &lane : lanes_) {
                    const uint64_t lane_body = stats_.lane.at(
                        static_cast<size_t>(lane.id))->body_bytes.load();
                    const uint64_t nic_now = current_nic.at(lane.nic);
                    const uint64_t nic_before = previous_nic.at(lane.nic);
                    line << " L" << lane.id << "[nic=" << lane.nic
                         << ",rx=" << gbps(nic_now - nic_before, interval)
                         << ",body=" << gbps(
                                lane_body - previous_lane_body.at(static_cast<size_t>(lane.id)),
                                interval)
                         << ",active=" << stats_.lane.at(
                                static_cast<size_t>(lane.id))->active_connections.load()
                         << ']';
                    previous_lane_body.at(static_cast<size_t>(lane.id)) = lane_body;
                }
                for (const auto &[gpu, counters] : stats_.gpu) {
                    const uint64_t gpu_h2d = counters->h2d_completed_bytes.load();
                    line << " G" << gpu << "[h2d="
                         << gbps(gpu_h2d - previous_gpu_h2d[gpu], interval)
                         << ",out=" << counters->outstanding_copies.load() << ']';
                    previous_gpu_h2d[gpu] = gpu_h2d;
                }
                print_line(line.str());
                previous_time = now;
                previous_body = body;
                previous_h2d = h2d;
                previous_h2d_copies = h2d_copies;
                previous_want_read = want_read;
                previous_want_write = want_write;
                previous_ktls_eagain = ktls_eagain;
                previous_nic = current_nic;
                next += std::chrono::milliseconds(100);
                if (next < now) next = now + std::chrono::milliseconds(100);
            }
        } catch (const std::exception &e) {
            print_line("ERROR telemetry thread: ", e.what());
        }
    }

    const std::vector<Lane> &lanes_;
    const std::vector<std::unique_ptr<Reactor>> &reactors_;
    RunStats &stats_;
    Clock::time_point start_;
    uint64_t configured_connections_ = 0;
    int cpu_ = -1;
    uint64_t total_slots_ = 0;
    std::atomic<bool> stop_{false};
    std::thread thread_;
    std::vector<RampSample> ramp_samples_;
};

std::string json_escape(std::string_view value) {
    std::ostringstream out;
    for (unsigned char c : value) {
        switch (c) {
            case '"': out << "\\\""; break;
            case '\\': out << "\\\\"; break;
            case '\b': out << "\\b"; break;
            case '\f': out << "\\f"; break;
            case '\n': out << "\\n"; break;
            case '\r': out << "\\r"; break;
            case '\t': out << "\\t"; break;
            default:
                if (c < 0x20) {
                    out << "\\u" << std::hex << std::setw(4) << std::setfill('0')
                        << static_cast<unsigned>(c) << std::dec;
                } else out << static_cast<char>(c);
        }
    }
    return out.str();
}

bool validate_completed_objects(const std::vector<std::unique_ptr<Object>> &objects,
                                FatalState &fatal) {
    for (const auto &holder : objects) {
        const Object &object = *holder;
        std::vector<const RangeTask *> ranges(object.tasks.begin(), object.tasks.end());
        std::sort(ranges.begin(), ranges.end(), [](const RangeTask *a, const RangeTask *b) {
            return a->start < b->start;
        });
        uint64_t cursor = 0;
        for (const RangeTask *range : ranges) {
            if (range->state.load() != TaskState::COMPLETE) {
                fatal.set("object " + std::to_string(object.id) +
                          " has an incomplete range " + std::to_string(range->id));
                return false;
            }
            if (range->start != cursor) {
                fatal.set("object " + std::to_string(object.id) +
                          " has a range gap or overlap at " + std::to_string(cursor));
                return false;
            }
            if (range->length > object.size - cursor) {
                fatal.set("object range accounting exceeds object size");
                return false;
            }
            cursor += range->length;
        }
        if (cursor != object.size || object.completed_bytes.load() != object.size ||
            object.completed_ranges.load() != object.tasks.size()) {
            fatal.set("object " + std::to_string(object.id) +
                      " final byte/range accounting mismatch");
            return false;
        }
        if (object.size != 0 && object.device.pointer == nullptr) {
            fatal.set("object " + std::to_string(object.id) +
                      " lost its final cudaMalloc allocation");
            return false;
        }
        if (object.size != 0 && !object_etag_for_request(object)) {
            fatal.set("object " + std::to_string(object.id) +
                      " completed without an ETag snapshot");
            return false;
        }
    }
    return true;
}

struct StartupTimings {
    uint64_t program_start_ns = 0;
    uint64_t credentials_begin_ns = 0;
    uint64_t credentials_end_ns = 0;
    uint64_t catalog_begin_ns = 0;
    uint64_t catalog_end_ns = 0;
    uint64_t catalog_objects = 0;
    uint64_t catalog_etags = 0;
    uint64_t allocation_begin_ns = 0;
    uint64_t allocation_end_ns = 0;
    uint64_t reactor_prepare_begin_ns = 0;
    uint64_t reactor_prepare_end_ns = 0;
    uint64_t preconnect_begin_ns = 0;
    uint64_t preconnect_end_ns = 0;
    uint64_t preconnect_connections = 0;
    uint64_t preconnect_ktls_connections = 0;
    uint64_t preconnect_tls_ready = 0;
    uint64_t preconnect_first_tls_ready_ns = 0;
    uint64_t preconnect_tls_50_ns = 0;
    uint64_t preconnect_tls_90_ns = 0;
    uint64_t preconnect_tls_100_ns = 0;
    uint64_t preconnect_retries = 0;
    uint64_t preconnect_reconnects = 0;
    uint64_t preconnect_tls_rx_sw_delta = 0;
    bool preconnect_tls_stats_available = false;
    uint64_t prime_begin_ns = 0;
    uint64_t prime_end_ns = 0;
    uint64_t prime_connections = 0;
    uint64_t prime_ktls_connections = 0;
    uint64_t prime_http_requests = 0;
    uint64_t prime_responses = 0;
    uint64_t prime_ktls_recv_calls = 0;
    uint64_t prime_retries = 0;
    uint64_t prime_reconnects = 0;
};

bool validate_and_report_preconnect(
        const std::vector<std::unique_ptr<Object>> &objects,
        const std::vector<std::unique_ptr<Reactor>> &reactors,
        const Scheduler &scheduler, RunStats &stats, FatalState &fatal,
        uint64_t configured_connections, Clock::time_point start,
        Clock::time_point end, const TlsStatMap &tls_before,
        const TlsStatMap &tls_after, bool tls_before_available,
        bool tls_after_available, StartupTimings &startup) {
    const ReactorTelemetryAggregate telemetry =
        aggregate_reactor_telemetry(reactors, stats.lane.size(), true);
    const uint64_t live = live_reactor_connections(reactors);
    const uint64_t live_ktls = live_reactor_ktls_connections(reactors);
    const uint64_t preconnect_retries = total_preconnect_retries(reactors);
    uint64_t receive_only_bytes = 0;
    uint64_t receive_only_chunks = 0;
    for (const auto &reactor : reactors) {
        receive_only_bytes += reactor->receive_only_bytes();
        receive_only_chunks += reactor->receive_only_chunks();
    }

    if (scheduler.remaining() != scheduler.task_count())
        fatal.set("transport-only preconnect consumed scheduler tasks");
    for (const auto &task : scheduler.tasks()) {
        if (task->state.load(std::memory_order_acquire) != TaskState::PENDING ||
            task->received != 0 || task->h2d_completed_attempt != 0 ||
            task->pending_h2d != 0 || task->retries != 0) {
            fatal.set("transport-only preconnect mutated a Range task");
            break;
        }
    }
    for (const auto &object : objects) {
        if (object->completed_bytes.load(std::memory_order_relaxed) != 0 ||
            object->completed_ranges.load(std::memory_order_relaxed) != 0) {
            fatal.set("transport-only preconnect mutated object completion state");
            break;
        }
    }
    if (telemetry.requests_sent != 0 || telemetry.headers_validated != 0 ||
        telemetry.payload_connections != 0 || telemetry.active_ranges != 0 ||
        telemetry.network_ranges_completed != 0 ||
        telemetry.device_ranges_completed != 0)
        fatal.set("transport-only preconnect produced HTTP/range telemetry");
    if (stats.body_bytes.load() != 0 ||
        stats.h2d_submitted_bytes.load() != 0 ||
        stats.h2d_submitted_copies.load() != 0 ||
        stats.h2d_completed_bytes.load() != 0 ||
        stats.h2d_completed_copies.load() != 0 ||
        receive_only_bytes != 0 || receive_only_chunks != 0)
        fatal.set("transport-only preconnect moved payload bytes");
    if (stats.completed_ranges.load() != 0 || stats.retries.load() != 0 ||
        stats.http_errors.load() != 0 || stats.cuda_errors.load() != 0)
        fatal.set("transport-only preconnect changed transfer counters");
    if (stats.pinned_used.load() != 0 || stats.pinned_peak.load() != 0)
        fatal.set("transport-only preconnect acquired a pinned payload slot");
    if (total_ktls_recv_calls(stats, reactors) != 0)
        fatal.set("transport-only preconnect called the payload recvmsg path");
    if (live != configured_connections || live_ktls != configured_connections)
        fatal.set("transport-only preconnect did not establish the full RX-kTLS pool");
    if (stats.active_connections.load() != live)
        fatal.set("preconnect live connection accounting mismatch");
    if (telemetry.tls_ready != configured_connections)
        fatal.set("preconnect TLS-ready total differs from configured pool size");
    if (telemetry.tls_ready_timestamps.size() != telemetry.tls_ready)
        fatal.set("preconnect TLS timestamp/counter totals differ");
    if (stats.ktls_rx_enabled.load() != stats.tls_established.load())
        fatal.set("a preconnect TLS transport was admitted without RX kTLS");
    if (stats.no_pad_attempted.load() != stats.no_pad_succeeded.load())
        fatal.set("TLS_RX_EXPECT_NO_PAD failed during preconnect");

    startup.preconnect_connections = live;
    startup.preconnect_ktls_connections = live_ktls;
    startup.preconnect_tls_ready = telemetry.tls_ready;
    startup.preconnect_first_tls_ready_ns = telemetry.first_tls_ready_ns;
    const auto percentile_timestamp = [&](uint64_t numerator) {
        if (telemetry.tls_ready_timestamps.empty()) return uint64_t{0};
        const uint64_t population = telemetry.tls_ready_timestamps.size();
        const uint64_t target = (population * numerator + 99) / 100;
        return telemetry.tls_ready_timestamps.at(
            static_cast<size_t>(target - 1));
    };
    startup.preconnect_tls_50_ns = percentile_timestamp(50);
    startup.preconnect_tls_90_ns = percentile_timestamp(90);
    startup.preconnect_tls_100_ns = percentile_timestamp(100);
    startup.preconnect_retries = preconnect_retries;
    startup.preconnect_reconnects = stats.reconnects.load();
    startup.preconnect_tls_stats_available =
        tls_before_available && tls_after_available;
    if (startup.preconnect_tls_stats_available) {
        const int64_t delta = tls_stat_value(tls_after, "TlsRxSw") -
                              tls_stat_value(tls_before, "TlsRxSw");
        if (delta < 0) {
            fatal.set("TlsRxSw decreased during preconnect");
        } else {
            startup.preconnect_tls_rx_sw_delta =
                static_cast<uint64_t>(delta);
            if (startup.preconnect_tls_rx_sw_delta < configured_connections)
                fatal.set("TlsRxSw did not cover every preconnected transport");
        }
    }

    const double wall = std::chrono::duration<double>(end - start).count();
    const uint64_t start_ns = clock_time_ns(start);
    const auto elapsed = [start_ns](uint64_t timestamp_ns) {
        return timestamp_ns >= start_ns
            ? static_cast<double>(timestamp_ns - start_ns) / 1.0e9 : 0.0;
    };
    const uint64_t tasks_consumed = scheduler.task_count() >= scheduler.remaining()
        ? scheduler.task_count() - scheduler.remaining() : scheduler.task_count();
    print_line("PRECONNECT_RESULT mode=transport_only",
               " connections=", live,
               " ktls_connections=", live_ktls,
               " tls_ready=", telemetry.tls_ready,
               std::fixed, std::setprecision(6), " wall_s=", wall,
               " first_tls_ready_s=", elapsed(
                   startup.preconnect_first_tls_ready_ns),
               " tls_50_s=", elapsed(startup.preconnect_tls_50_ns),
               " tls_90_s=", elapsed(startup.preconnect_tls_90_ns),
               " tls_100_s=", elapsed(startup.preconnect_tls_100_ns),
               " http_requests=", telemetry.requests_sent,
               " scheduler_tasks_consumed=", tasks_consumed,
               " s3_body_bytes=", stats.body_bytes.load(),
               " h2d_bytes=", stats.h2d_completed_bytes.load(),
               " h2d_copies=", stats.h2d_completed_copies.load(),
               " ktls_recv_calls=", total_ktls_recv_calls(stats, reactors),
               " pinned_peak_slots=", stats.pinned_peak.load(),
               " retries=", preconnect_retries,
               " reconnects=", stats.reconnects.load(),
               " tls_errors=", stats.tls_errors.load(),
               " tls_rx_sw_delta=",
               startup.preconnect_tls_stats_available
                   ? std::to_string(startup.preconnect_tls_rx_sw_delta)
                   : "unavailable",
               " pass=", fatal.failed() ? "false" : "true");
    return !fatal.failed();
}

bool validate_and_report_prime(
        const std::vector<std::unique_ptr<Object>> &objects,
        const std::vector<std::unique_ptr<Reactor>> &reactors,
        const Scheduler &scheduler, RunStats &stats, FatalState &fatal,
        uint64_t configured_connections, Clock::time_point start,
        Clock::time_point end, StartupTimings &startup) {
    const ReactorTelemetryAggregate telemetry =
        aggregate_reactor_telemetry(reactors, stats.lane.size(), false);
    const uint64_t live = live_reactor_connections(reactors);
    const uint64_t live_ktls = live_reactor_ktls_connections(reactors);
    const uint64_t completions = total_prime_completions(reactors);
    const uint64_t retries = total_prime_retries(reactors);
    const uint64_t recv_calls = total_ktls_recv_calls(stats, reactors);
    uint64_t receive_only_bytes = 0;
    uint64_t receive_only_chunks = 0;
    for (const auto &reactor : reactors) {
        receive_only_bytes += reactor->receive_only_bytes();
        receive_only_chunks += reactor->receive_only_chunks();
    }

    if (scheduler.remaining() != scheduler.task_count())
        fatal.set("HEAD prime consumed scheduler tasks");
    for (const auto &task : scheduler.tasks()) {
        if (task->state.load(std::memory_order_acquire) != TaskState::PENDING ||
            task->received != 0 || task->h2d_completed_attempt != 0 ||
            task->pending_h2d != 0 || task->retries != 0) {
            fatal.set("HEAD prime mutated a Range task");
            break;
        }
    }
    for (const auto &object : objects) {
        if (object->completed_bytes.load(std::memory_order_relaxed) != 0 ||
            object->completed_ranges.load(std::memory_order_relaxed) != 0) {
            fatal.set("HEAD prime mutated object completion state");
            break;
        }
    }
    if (telemetry.requests_sent < configured_connections ||
        telemetry.headers_validated < configured_connections ||
        completions != configured_connections)
        fatal.set("HEAD prime did not leave every connection with a validated response");
    if (telemetry.payload_connections != 0 || telemetry.active_ranges != 0 ||
        telemetry.network_ranges_completed != 0 ||
        telemetry.device_ranges_completed != 0 ||
        telemetry.first_body_ns != 0 || telemetry.last_body_ns != 0)
        fatal.set("HEAD prime produced payload/range telemetry");
    if (stats.body_bytes.load() != 0 ||
        stats.h2d_submitted_bytes.load() != 0 ||
        stats.h2d_submitted_copies.load() != 0 ||
        stats.h2d_completed_bytes.load() != 0 ||
        stats.h2d_completed_copies.load() != 0 ||
        receive_only_bytes != 0 || receive_only_chunks != 0)
        fatal.set("HEAD prime moved payload bytes");
    if (stats.completed_ranges.load() != 0 || stats.retries.load() != 0 ||
        stats.cuda_errors.load() != 0)
        fatal.set("HEAD prime changed transfer counters");
    if (stats.pinned_used.load() != 0 || stats.pinned_peak.load() != 0)
        fatal.set("HEAD prime acquired a pinned payload slot");
    if (recv_calls < configured_connections)
        fatal.set("HEAD prime did not receive one response through kTLS per connection");
    if (live != configured_connections || live_ktls != configured_connections)
        fatal.set("HEAD prime did not retain the full RX-kTLS pool");
    if (stats.active_connections.load() != live)
        fatal.set("HEAD-prime live connection accounting mismatch");
    if (stats.ktls_rx_enabled.load() != stats.tls_established.load())
        fatal.set("a HEAD-prime replacement transport lacked RX kTLS");
    if (stats.no_pad_attempted.load() != stats.no_pad_succeeded.load())
        fatal.set("TLS_RX_EXPECT_NO_PAD failed during HEAD prime");

    startup.prime_connections = live;
    startup.prime_ktls_connections = live_ktls;
    startup.prime_http_requests = telemetry.requests_sent;
    startup.prime_responses = telemetry.headers_validated;
    startup.prime_ktls_recv_calls = recv_calls;
    startup.prime_retries = retries;
    startup.prime_reconnects = stats.reconnects.load();

    const size_t tasks_consumed = scheduler.task_count() >= scheduler.remaining()
        ? scheduler.task_count() - scheduler.remaining() : scheduler.task_count();
    print_line("PRIME_RESULT mode=head_object",
               " connections=", live,
               " ktls_connections=", live_ktls,
               " http_requests=", telemetry.requests_sent,
               " responses_validated=", telemetry.headers_validated,
               std::fixed, std::setprecision(6),
               " wall_s=", std::chrono::duration<double>(end - start).count(),
               " scheduler_tasks_consumed=", tasks_consumed,
               " s3_body_bytes=", stats.body_bytes.load(),
               " h2d_bytes=", stats.h2d_completed_bytes.load(),
               " pinned_peak_slots=", stats.pinned_peak.load(),
               " ktls_recv_calls=", recv_calls,
               " retries=", retries,
               " reconnects=", stats.reconnects.load(),
               " http_errors=", stats.http_errors.load(),
               " tls_errors=", stats.tls_errors.load(),
               " pass=", fatal.failed() ? "false" : "true");
    return !fatal.failed();
}

struct FinalInputs {
    Clock::time_point start;
    Clock::time_point end;
    std::map<std::string, uint64_t> nic_before;
    std::map<std::string, uint64_t> nic_after;
    TlsStatMap tls_before;
    TlsStatMap tls_after;
    bool tls_before_available = false;
    bool tls_after_available = false;
    rusage usage{};
    rusage usage_before{};
    uint64_t pinned_hwm = 0;
    uint64_t pinned_bytes = 0;
    uint64_t total_slots = 0;
    uint64_t configured_connections = 0;
    uint64_t pool_connections_at_start = 0;
    uint64_t tls_connections_before = 0;
    unsigned sequence = 1;
    unsigned measured_index = 1;
    StartupTimings startup;
    std::vector<RampSample> ramp_samples;
};

bool transfer_byte_accounting_matches(bool receive_only, uint64_t expected,
                                      uint64_t body, uint64_t h2d_submitted,
                                      uint64_t h2d_completed,
                                      uint64_t receive_only_bytes) {
    // body/H2D are physical work counters.  A failed partial Range attempt is
    // real network and DMA work, so retries may make them exceed the useful
    // catalog bytes.  Exact useful completion is enforced independently by
    // every RangeTask and Object; never "correct" physical telemetry by
    // subtracting abandoned attempts.
    if (body < expected) return false;
    if (receive_only)
        return h2d_submitted == 0 && h2d_completed == 0 &&
               receive_only_bytes == body;
    return receive_only_bytes == 0 && h2d_submitted == body &&
           h2d_completed == body;
}

bool print_iteration_checkpoint(
        const Options &opt,
        const std::vector<std::unique_ptr<Object>> &objects,
        const std::vector<std::unique_ptr<Reactor>> &reactors,
        const Scheduler &scheduler, RunStats &stats, FatalState &fatal,
        Clock::time_point start, Clock::time_point end,
        unsigned sequence, unsigned measured_index,
        uint64_t pool_connections_at_start,
        uint64_t tls_connections_before) {
    (void)validate_completed_objects(objects, fatal);
    uint64_t expected = 0;
    for (const auto &object : objects) expected += object->size;
    const ReactorTelemetryAggregate telemetry =
        aggregate_reactor_telemetry(reactors, stats.lane.size(), false);
    const uint64_t body = stats.body_bytes.load(std::memory_order_relaxed);
    const uint64_t h2d = stats.h2d_completed_bytes.load(
        std::memory_order_relaxed);
    uint64_t receive_only = 0;
    for (const auto &reactor : reactors)
        receive_only += reactor->receive_only_bytes();
    const uint64_t live = live_reactor_connections(reactors);
    const uint64_t live_ktls = live_reactor_ktls_connections(reactors);
    if (!transfer_byte_accounting_matches(
            opt.receive_only, expected, body,
            stats.h2d_submitted_bytes.load(), h2d, receive_only))
        fatal.set("iteration physical/useful byte accounting mismatch");
    if (scheduler.remaining() != 0 ||
        stats.completed_ranges.load() != scheduler.task_count() ||
        telemetry.network_ranges_completed != scheduler.task_count() ||
        telemetry.device_ranges_completed != scheduler.task_count())
        fatal.set("iteration range completion accounting mismatch");
    if (telemetry.payload_connections != 0 || telemetry.active_ranges != 0)
        fatal.set("iteration ended with active payload/range state");
    if (stats.pinned_used.load() != 0)
        fatal.set("iteration ended with pinned slots in use");
    if (stats.cuda_errors.load() != 0)
        fatal.set("iteration encountered a CUDA error");
    if (stats.active_connections.load() != live)
        fatal.set("iteration live connection accounting mismatch");
    if (live != live_ktls)
        fatal.set("iteration retained a non-kTLS connection");

    const uint64_t start_ns = clock_time_ns(start);
    const auto elapsed = [start_ns](uint64_t timestamp) {
        return timestamp >= start_ns
            ? static_cast<double>(timestamp - start_ns) / 1.0e9 : 0.0;
    };
    const double wall = std::chrono::duration<double>(end - start).count();
    const double active = telemetry.first_body_ns != 0 &&
                          telemetry.last_body_ns >= telemetry.first_body_ns
        ? static_cast<double>(telemetry.last_body_ns -
                              telemetry.first_body_ns) / 1.0e9
        : 0.0;
    const uint64_t tls_now = stats.tls_established.load();
    const uint64_t new_tls = tls_now >= tls_connections_before
        ? tls_now - tls_connections_before : 0;
    if (telemetry.tls_ready > new_tls)
        fatal.set("iteration TLS-ready count exceeds new TLS connections");
    print_line("ITERATION_RESULT role=measured",
               " sequence=", sequence,
               " measured_index=", measured_index,
               " pool_mode=head_primed",
               " pool_connections_at_start=", pool_connections_at_start,
               " pool_connections_live_at_end=", live,
               " new_tls_connections=", new_tls,
               std::fixed, std::setprecision(6),
               " first_body_s=", elapsed(telemetry.first_body_ns),
               " device_complete_s=", elapsed(
                   telemetry.last_device_complete_ns),
               " wall_s=", wall,
               " first_to_last_body_gbps=", gbps(expected, active),
               " end_to_end_gbps=", gbps(expected, wall),
               " useful_object_bytes=", expected,
               " physical_s3_body_bytes=", body,
               " retry_s3_body_bytes=", body >= expected ? body - expected : 0,
               " physical_h2d_bytes=", h2d,
               " retry_h2d_bytes=", !opt.receive_only && h2d >= expected
                   ? h2d - expected : 0,
               " retries=", stats.retries.load(),
               " reconnects=", stats.reconnects.load(),
               " http_errors=", stats.http_errors.load(),
               " tls_errors=", stats.tls_errors.load(),
               " cuda_errors=", stats.cuda_errors.load(),
               " pass=", fatal.failed() ? "false" : "true");
    return !fatal.failed();
}

bool print_final_summary(const Options &opt, const std::vector<Lane> &lanes,
                         const std::vector<std::unique_ptr<Object>> &objects,
                         const std::vector<std::unique_ptr<Reactor>> &reactors,
                         const Scheduler &scheduler, RunStats &stats,
                         FatalState &fatal, const FinalInputs &input) {
    (void)validate_completed_objects(objects, fatal);
    uint64_t expected = 0;
    uint64_t object_completed_bytes = 0;
    uint64_t objects_complete = 0;
    uint64_t allocations_live = 0;
    std::map<int, uint64_t> gpu_object_count;
    std::map<int, uint64_t> gpu_objects_complete;
    std::map<int, uint64_t> gpu_allocations_live;
    for (const auto &object : objects) {
        expected += object->size;
        object_completed_bytes += object->completed_bytes.load();
        gpu_object_count[object->gpu] += 1;
        const bool object_complete =
            object->completed_bytes.load() == object->size &&
            object->completed_ranges.load() == object->tasks.size();
        const bool allocation_live =
            object->size == 0 || object->device.pointer != nullptr;
        if (object_complete) {
            objects_complete += 1;
            gpu_objects_complete[object->gpu] += 1;
        }
        if (allocation_live) {
            allocations_live += 1;
            gpu_allocations_live[object->gpu] += 1;
        }
    }
    // Reactor threads are joined before this function is called.  This is the
    // authoritative reduction of the per-CPU telemetry blocks; no benchmark
    // thread ever increments a shared startup/tail diagnostic counter.
    ReactorTelemetryAggregate reactor_telemetry =
        aggregate_reactor_telemetry(reactors, lanes.size(), true);
    std::vector<uint64_t> lane_ranges_total(lanes.size(), 0);
    for (const auto &task : scheduler.tasks())
        lane_ranges_total.at(static_cast<size_t>(task->lane_id)) += 1;
    uint64_t expected_get_connections = 0;
    for (const Lane &lane : lanes) {
        expected_get_connections += std::min<uint64_t>(
            lane.connections,
            lane_ranges_total.at(static_cast<size_t>(lane.id)));
    }
    const bool prior_measured_iteration = input.sequence > 1;
    const uint64_t tls_total = stats.tls_established.load();
    const uint64_t new_tls_connections =
        tls_total >= input.tls_connections_before
            ? tls_total - input.tls_connections_before : 0;
    const uint64_t tls_percentile_population = reactor_telemetry.tls_ready;
    const auto tls_percentile_timestamp = [&](uint64_t numerator,
                                               uint64_t denominator) {
        if (tls_percentile_population == 0) return uint64_t{0};
        const uint64_t target =
            (tls_percentile_population * numerator + denominator - 1) /
            denominator;
        return target != 0 && reactor_telemetry.tls_ready_timestamps.size() >= target
            ? reactor_telemetry.tls_ready_timestamps[
                  static_cast<size_t>(target - 1)]
            : uint64_t{0};
    };
    const uint64_t get_tls_50_ns = tls_percentile_timestamp(50, 100);
    const uint64_t get_tls_90_ns = tls_percentile_timestamp(90, 100);
    const uint64_t get_tls_100_ns = tls_percentile_timestamp(100, 100);
    uint64_t payload_connections_peak_sampled = 0;
    for (const RampSample &sample : input.ramp_samples)
        payload_connections_peak_sampled = std::max(
            payload_connections_peak_sampled, sample.payload_connections);
    const double wall = std::chrono::duration<double>(input.end - input.start).count();
    const uint64_t transfer_start_ns = clock_time_ns(input.start);
    const uint64_t first_body_ns = reactor_telemetry.first_body_ns;
    const uint64_t network_complete_ns = reactor_telemetry.last_body_ns;
    const uint64_t device_complete_ns =
        reactor_telemetry.last_device_complete_ns;
    const auto elapsed_seconds = [transfer_start_ns](uint64_t timestamp_ns) {
        return transfer_start_ns != 0 && timestamp_ns >= transfer_start_ns
            ? static_cast<double>(timestamp_ns - transfer_start_ns) / 1.0e9
            : 0.0;
    };
    const auto interval_seconds = [](uint64_t begin_ns, uint64_t end_ns) {
        return begin_ns != 0 && end_ns >= begin_ns
            ? static_cast<double>(end_ns - begin_ns) / 1.0e9
            : 0.0;
    };
    const double first_body_seconds = elapsed_seconds(first_body_ns);
    const double network_complete_seconds = elapsed_seconds(network_complete_ns);
    const double sink_complete_seconds = elapsed_seconds(device_complete_ns);
    const double network_active_seconds =
        first_body_ns != 0 && network_complete_ns >= first_body_ns
            ? static_cast<double>(network_complete_ns - first_body_ns) / 1.0e9
            : 0.0;
    const double network_to_sink_seconds =
        network_complete_ns != 0 && device_complete_ns >= network_complete_ns
            ? static_cast<double>(device_complete_ns - network_complete_ns) / 1.0e9
            : 0.0;
    const uint64_t body = stats.body_bytes.load();
    const uint64_t h2d = stats.h2d_completed_bytes.load();
    uint64_t receive_only = 0;
    uint64_t receive_only_chunks = 0;
    for (const auto &reactor : reactors) {
        receive_only += reactor->receive_only_bytes();
        receive_only_chunks += reactor->receive_only_chunks();
    }
    const bool all_ktls = tls_total != 0 &&
                          stats.ktls_rx_enabled.load() == tls_total;
    const bool all_no_pad = stats.no_pad_succeeded.load() ==
                            stats.no_pad_attempted.load();
    const bool tls_rx_sw_required = new_tls_connections != 0;
    const bool tls_rx_sw_observed =
        input.tls_before_available && input.tls_after_available &&
        tls_stat_value(input.tls_after, "TlsRxSw") >
            tls_stat_value(input.tls_before, "TlsRxSw");
    const bool tls_rx_sw_validation_ok =
        !tls_rx_sw_required || !input.tls_before_available ||
        !input.tls_after_available || tls_rx_sw_observed;
    if (!all_ktls)
        fatal.set("not every established TLS connection used RX kTLS");
    if (!all_no_pad)
        fatal.set("TLS_RX_EXPECT_NO_PAD did not succeed on every RX-kTLS connection");
    if (tls_rx_sw_required &&
        input.tls_before_available && input.tls_after_available &&
        !tls_rx_sw_observed)
        fatal.set("/proc/net/tls_stat TlsRxSw did not increase");
    if (stats.cuda_errors.load() != 0) fatal.set("one or more CUDA errors occurred");
    if (reactor_telemetry.payload_connections != 0)
        fatal.set("payload connections remain active at final report");
    if (reactor_telemetry.active_ranges != 0)
        fatal.set("network-active ranges remain at final report");
    if (reactor_telemetry.tls_ready_timestamps.size() !=
        reactor_telemetry.tls_ready)
        fatal.set("per-reactor TLS timestamp/counter totals differ");
    if (reactor_telemetry.tls_ready > new_tls_connections)
        fatal.set("measured-iteration TLS-ready count exceeds new TLS connections");
    if (reactor_telemetry.network_ranges_completed != scheduler.task_count())
        fatal.set("per-reactor network range totals differ from scheduler");
    if (reactor_telemetry.device_ranges_completed != scheduler.task_count())
        fatal.set("per-reactor sink range totals differ from scheduler");
    if (!transfer_byte_accounting_matches(
            opt.receive_only, expected, body,
            stats.h2d_submitted_bytes.load(), h2d, receive_only))
        fatal.set("global physical/useful byte accounting mismatch");
    if (stats.pinned_used.load() != 0) fatal.set("pinned slots remain in use at final report");
    const uint64_t live_connections = live_reactor_connections(reactors);
    const uint64_t live_ktls_connections =
        live_reactor_ktls_connections(reactors);
    if (stats.active_connections.load() != live_connections)
        fatal.set("live reactor connections differ from global accounting");
    if (live_ktls_connections != live_connections)
        fatal.set("a retained persistent connection lacks RX kTLS");
    if (scheduler.remaining() != 0) fatal.set("one or more ranges remain incomplete");
    if (expected != 0 && first_body_ns == 0)
        fatal.set("first S3 body byte milestone was not recorded");
    if (expected != 0 && network_complete_ns == 0)
        fatal.set("network completion milestone was not recorded");
    if (expected != 0 && device_complete_ns == 0)
        fatal.set("payload-sink completion milestone was not recorded");
    const bool pass = !fatal.failed();
    const uint64_t retry_body_bytes = body >= expected ? body - expected : 0;
    const uint64_t retry_h2d_bytes = !opt.receive_only && h2d >= expected
        ? h2d - expected : 0;
    const double ring_seconds = static_cast<double>(stats.ring_stall_ns.load()) / 1.0e9;
    const double ring_percent = wall > 0.0 && input.configured_connections != 0
        ? ring_seconds / (wall * static_cast<double>(input.configured_connections)) * 100.0
        : 0.0;
    const double user_total = static_cast<double>(input.usage.ru_utime.tv_sec) +
        static_cast<double>(input.usage.ru_utime.tv_usec) / 1.0e6;
    const double system_total = static_cast<double>(input.usage.ru_stime.tv_sec) +
        static_cast<double>(input.usage.ru_stime.tv_usec) / 1.0e6;
    const double user_before = static_cast<double>(input.usage_before.ru_utime.tv_sec) +
        static_cast<double>(input.usage_before.ru_utime.tv_usec) / 1.0e6;
    const double system_before = static_cast<double>(input.usage_before.ru_stime.tv_sec) +
        static_cast<double>(input.usage_before.ru_stime.tv_usec) / 1.0e6;
    const double user_seconds = user_total - user_before;
    const double system_seconds = system_total - system_before;
    std::map<int, double> lane_reactor_cpu_seconds;
    std::map<int, double> lane_reactor_wall_seconds;
    double reactor_cpu_seconds = 0.0;
    double reactor_wall_seconds = 0.0;
    double reactor_max_utilization = 0.0;
    uint64_t epoll_ctl_calls = 0;
    uint64_t epoll_ctl_skips = 0;
    for (const auto &reactor : reactors) {
        const double reactor_wall =
            static_cast<double>(reactor->wall_time_ns()) / 1.0e9;
        const double cpu_seconds =
            static_cast<double>(reactor->cpu_time_ns()) / 1.0e9;
        lane_reactor_cpu_seconds[reactor->lane_id()] += cpu_seconds;
        lane_reactor_wall_seconds[reactor->lane_id()] += reactor_wall;
        reactor_cpu_seconds += cpu_seconds;
        reactor_wall_seconds += reactor_wall;
        epoll_ctl_calls += reactor->epoll_ctl_calls();
        epoll_ctl_skips += reactor->epoll_ctl_skips();
        if (reactor_wall > 0.0) {
            reactor_max_utilization = std::max(
                reactor_max_utilization, cpu_seconds / reactor_wall * 100.0);
        }
    }
    const uint64_t ktls_recv_calls = total_ktls_recv_calls(stats, reactors);
    const uint64_t ktls_recv_eagain = total_ktls_recv_eagain(stats, reactors);

    print_line("===== FINAL SUMMARY =====");
    print_line("ITERATION role=measured",
               " sequence=", input.sequence,
               " measured_index=", input.measured_index,
               " pool_mode=head_primed",
               " pool_connections_at_start=", input.pool_connections_at_start,
               " pool_connections_live_at_end=", live_connections,
               " pool_ktls_connections_live_at_end=", live_ktls_connections,
               " new_tls_connections=", new_tls_connections);
    print_line(
        "RATE_SCOPE live_interval_agg_gbps=all_entities_delta/sample_interval",
        " final_entity_contribution_gbps=entity_bytes/end_to_end_s",
        " lane_first_to_last_gbps=lane_bytes/(last_body-first_body)");
    print_line("PRETRANSFER_TIMING excluded_from_transfer=yes",
               std::fixed, std::setprecision(6),
               " total_s=", interval_seconds(
                   input.startup.program_start_ns, transfer_start_ns),
               " total_scope=process_start_to_iteration",
               " prior_measured_iterations_in_total=",
               prior_measured_iteration ? "yes" : "no",
               " credentials_s=", interval_seconds(
                   input.startup.credentials_begin_ns,
                   input.startup.credentials_end_ns),
               " catalog_load_s=", interval_seconds(
                   input.startup.catalog_begin_ns,
                   input.startup.catalog_end_ns),
               " catalog_objects=", input.startup.catalog_objects,
               " catalog_size_metadata=all",
               " catalog_etags=", input.startup.catalog_etags,
               " catalog_head_requests=0",
               " query_head_requests=0",
               " gpu_allocate_plan_s=", interval_seconds(
                   input.startup.allocation_begin_ns,
                   input.startup.allocation_end_ns),
               " arena_reactor_prepare_s=", interval_seconds(
                   input.startup.reactor_prepare_begin_ns,
                   input.startup.reactor_prepare_end_ns),
               " preconnect_s=", interval_seconds(
                   input.startup.preconnect_begin_ns,
                   input.startup.preconnect_end_ns),
               " preconnect_mode=transport_only",
               " preconnect_connections=",
               input.startup.preconnect_connections,
               " preconnect_ktls_connections=",
               input.startup.preconnect_ktls_connections,
               " preconnect_first_tls_ready_s=", interval_seconds(
                   input.startup.preconnect_begin_ns,
                   input.startup.preconnect_first_tls_ready_ns),
               " preconnect_tls_50_s=", interval_seconds(
                   input.startup.preconnect_begin_ns,
                   input.startup.preconnect_tls_50_ns),
               " preconnect_tls_90_s=", interval_seconds(
                   input.startup.preconnect_begin_ns,
                   input.startup.preconnect_tls_90_ns),
               " preconnect_tls_100_s=", interval_seconds(
                   input.startup.preconnect_begin_ns,
                   input.startup.preconnect_tls_100_ns),
               " preconnect_http_requests=0 preconnect_s3_body_bytes=0",
               " preconnect_h2d_bytes=0",
               " prime_s=", interval_seconds(
                   input.startup.prime_begin_ns,
                   input.startup.prime_end_ns),
               " prime_mode=head_object",
               " prime_connections=", input.startup.prime_connections,
               " prime_ktls_connections=",
               input.startup.prime_ktls_connections,
               " prime_http_requests=", input.startup.prime_http_requests,
               " prime_responses=", input.startup.prime_responses,
               " prime_s3_body_bytes=0 prime_h2d_bytes=0",
               " prime_ktls_recv_calls=",
               input.startup.prime_ktls_recv_calls,
               " prime_retries=", input.startup.prime_retries,
               " prime_reconnects=", input.startup.prime_reconnects);
    print_line("GET_RAMP timer_origin=before_reactor_threads",
               " pool_mode=head_primed",
               " pool_connections_at_start=", input.pool_connections_at_start,
               " configured_connections=", input.configured_connections,
               " expected_tasked_connections=", expected_get_connections,
               " tls_ready=", reactor_telemetry.tls_ready,
               " tls_ready_scope=new_handshakes_this_iteration",
               " first_tls_ready_s=", elapsed_seconds(
                   reactor_telemetry.first_tls_ready_ns),
               " tls_50_s=", elapsed_seconds(get_tls_50_ns),
               " tls_90_s=", elapsed_seconds(get_tls_90_ns),
               " tls_100_s=", elapsed_seconds(get_tls_100_ns),
               " first_request_sent_s=", elapsed_seconds(
                   reactor_telemetry.first_request_ns),
               " first_headers_s=", elapsed_seconds(
                   reactor_telemetry.first_headers_ns),
               " first_body_s=", first_body_seconds,
               " requests_sent=", reactor_telemetry.requests_sent,
               " headers_validated=", reactor_telemetry.headers_validated,
               " payload_connections_peak_sampled=",
               payload_connections_peak_sampled,
               " counters=per_reactor_cacheline final_aggregation=yes");
    if (!input.ramp_samples.empty()) {
        std::ostringstream ramp;
        ramp << "RAMP_100MS format=ms/tls/req/hdr/payload/ranges/body_MiB samples=";
        for (size_t i = 0; i < input.ramp_samples.size(); ++i) {
            if (i != 0) ramp << ',';
            const RampSample &sample = input.ramp_samples[i];
            ramp << sample.elapsed_ms << '/' << sample.tls_ready << '/'
                 << sample.requests_sent << '/' << sample.headers_validated << '/'
                 << sample.payload_connections << '/' << sample.active_ranges << '/'
                 << sample.body_bytes / MiB;
        }
        print_line(ramp.str());
    }
    if (opt.receive_only) {
        print_line("TIMING first_body_s=", std::fixed, std::setprecision(6),
                   first_body_seconds,
                   " network_complete_s=", network_complete_seconds,
                   " receive_complete_s=", sink_complete_seconds,
                   " end_to_end_s=", wall,
                   " network_to_receive_drain_s=", network_to_sink_seconds);
        print_line("THROUGHPUT network_complete_gbps=",
                   gbps(expected, network_complete_seconds),
                   " receive_complete_gbps=", gbps(expected, sink_complete_seconds),
                   " end_to_end_gbps=", gbps(expected, wall),
                   " first_to_last_body_gbps=", gbps(expected, network_active_seconds));
        print_line("wall_s=", std::fixed, std::setprecision(6), wall,
                   " expected_object_bytes=", expected,
                   " total_s3_body_bytes=", body,
                   " total_receive_only_bytes=", receive_only,
                   " useful_object_gbps=", gbps(expected, wall),
                   " aggregate_s3_end_to_end_gbps=", gbps(body, wall),
                   " payload_sink=receive_only_REGISTERED_PINNED_DIAGNOSTIC");
    } else {
        print_line("TIMING first_body_s=", std::fixed, std::setprecision(6),
                   first_body_seconds,
                   " network_complete_s=", network_complete_seconds,
                   " device_complete_s=", sink_complete_seconds,
                   " end_to_end_s=", wall,
                   " network_to_device_drain_s=", network_to_sink_seconds);
        print_line("THROUGHPUT network_complete_gbps=",
                   gbps(expected, network_complete_seconds),
                   " device_complete_gbps=", gbps(expected, sink_complete_seconds),
                   " end_to_end_gbps=", gbps(expected, wall),
                   " first_to_last_body_gbps=", gbps(expected, network_active_seconds));
        print_line("wall_s=", std::fixed, std::setprecision(6), wall,
                   " expected_object_bytes=", expected,
                   " total_s3_body_bytes=", body,
                   " total_completed_h2d_bytes=", h2d,
                   " useful_object_gbps=", gbps(expected, wall),
                   " aggregate_s3_end_to_end_gbps=", gbps(body, wall),
                   " aggregate_h2d_end_to_end_gbps=", gbps(h2d, wall));
    }
    for (const auto &[nic, after] : input.nic_after) {
        const uint64_t before = input.nic_before.at(nic);
        print_line("NIC_FINAL nic=", nic, " rx_bytes=", after - before,
                   " rx_gbps=", gbps(after - before, wall));
    }
    for (const Lane &lane : lanes) {
        const LaneCounters &lane_stats = *stats.lane.at(
            static_cast<size_t>(lane.id));
        const LaneTelemetryAggregate &lane_telemetry =
            reactor_telemetry.lane.at(static_cast<size_t>(lane.id));
        const uint64_t bytes = lane_stats.body_bytes.load();
        const uint64_t lane_first_ns = lane_telemetry.first_body_ns;
        const uint64_t lane_last_ns = lane_telemetry.last_body_ns;
        const double lane_active_seconds = interval_seconds(
            lane_first_ns, lane_last_ns);
        const double post_network_idle_seconds =
            lane_last_ns != 0 && network_complete_ns >= lane_last_ns
                ? static_cast<double>(network_complete_ns -
                                      lane_last_ns) / 1.0e9
                : 0.0;
        const double lane_cpu = lane_reactor_cpu_seconds[lane.id];
        const double lane_reactor_wall = lane_reactor_wall_seconds[lane.id];
        print_line("LANE_FINAL id=", lane.id, " nic=", lane.nic,
                   " gpu=", lane.gpu, " body_bytes=", bytes,
                   " end_to_end_body_contribution_gbps=", gbps(bytes, wall),
                   " first_body_s=", elapsed_seconds(lane_first_ns),
                   " last_body_s=", elapsed_seconds(lane_last_ns),
                   " first_to_last_body_gbps=",
                   gbps(bytes, lane_active_seconds),
                   " post_network_idle_s=", post_network_idle_seconds,
                   " ranges_total=", lane_ranges_total.at(
                       static_cast<size_t>(lane.id)),
                   " network_ranges_complete=",
                   lane_telemetry.network_ranges_completed,
                   " device_ranges_complete=",
                   lane_telemetry.device_ranges_completed,
                   " reactor_cpu_s=", lane_cpu,
                   " reactor_avg_util_percent=",
                   lane_reactor_wall > 0.0
                       ? lane_cpu / lane_reactor_wall * 100.0 : 0.0);
    }
    for (const auto &[gpu, counters] : stats.gpu)
        print_line("GPU_FINAL gpu=", gpu,
                   " h2d_completed_bytes=", counters->h2d_completed_bytes.load(),
                   " end_to_end_h2d_contribution_gbps=",
                   gbps(counters->h2d_completed_bytes.load(), wall),
                   " outstanding_copies=", counters->outstanding_copies.load(),
                   " objects=", gpu_object_count[gpu],
                   " objects_complete=", gpu_objects_complete[gpu],
                   " allocations_live=", gpu_allocations_live[gpu]);
    print_line("ranges_completed=", stats.completed_ranges.load(),
               " ranges_total=", scheduler.task_count(),
               " retries=", stats.retries.load(),
               " connection_reconnects=", stats.reconnects.load(),
               " http_errors=", stats.http_errors.load(),
               " tls_errors=", stats.tls_errors.load(),
               " cuda_errors=", stats.cuda_errors.load());
    print_line("RETRY_WORK useful_object_bytes=", expected,
               " physical_s3_body_bytes=", body,
               " retry_s3_body_bytes=", retry_body_bytes,
               " physical_h2d_bytes=", h2d,
               " retry_h2d_bytes=", retry_h2d_bytes);
    if (opt.receive_only) {
        print_line("RECEIVE_ONLY_FINAL bytes=", receive_only,
                   " chunks=", receive_only_chunks,
                   " cuda_copy_submissions=", stats.h2d_submitted_copies.load());
    } else {
        print_line("h2d_submitted_copies=", stats.h2d_submitted_copies.load(),
                   " h2d_completed_copies=", stats.h2d_completed_copies.load(),
                   " h2d_inline_batches=", stats.h2d_inline_batches.load(),
                   " h2d_inline_batch_avg_copies=",
                   stats.h2d_inline_batches.load() != 0
                       ? static_cast<double>(stats.h2d_submitted_copies.load()) /
                             static_cast<double>(stats.h2d_inline_batches.load())
                       : 0.0,
                   " h2d_inline_event_queries=",
                   stats.h2d_inline_event_queries.load(),
                   " average_completed_h2d_copy_KiB=",
                   stats.h2d_completed_copies.load() != 0
                       ? static_cast<double>(h2d) /
                             static_cast<double>(stats.h2d_completed_copies.load()) / KiB
                       : 0.0);
    }
    print_line("pinned_hwm_bytes=", input.pinned_hwm,
               " pinned_registered_bytes=", input.pinned_bytes,
               " pinned_slots_total=", input.total_slots,
               " pinned_peak_slots=", stats.pinned_peak.load(),
               " pinned_peak_active_bytes=", stats.pinned_peak.load() * opt.slot_bytes,
               " final_slots_in_use=", stats.pinned_used.load());
    print_line("ring_stall_connection_seconds=", ring_seconds,
               " ring_full_percent_of_configured_connection_time=", ring_percent,
               " ssl_want_read=", stats.want_read.load(),
               " ssl_want_write=", stats.want_write.load(),
               " ktls_recv_calls=", ktls_recv_calls,
               " ktls_recv_eagain=", ktls_recv_eagain);
    print_line("REACTORS_FINAL count=", reactors.size(),
               " cpu_s=", reactor_cpu_seconds,
               " avg_utilization_percent=",
               reactor_wall_seconds > 0.0
                   ? reactor_cpu_seconds / reactor_wall_seconds * 100.0 : 0.0,
               " max_utilization_percent=", reactor_max_utilization,
               " epoll_ctl_calls=", epoll_ctl_calls,
               " epoll_ctl_noop_skips=", epoll_ctl_skips);
    print_line("getrusage_transfer_user_s=", user_seconds,
               " getrusage_transfer_system_s=", system_seconds,
               " process_total_user_s=", user_total,
               " process_total_system_s=", system_total,
               " maxrss_kib=", input.usage.ru_maxrss,
               " involuntary_context_switches=", input.usage.ru_nivcsw,
               " voluntary_context_switches=", input.usage.ru_nvcsw);
    for (const char *name : kRequiredTlsStats) {
        if (input.tls_before_available && input.tls_after_available) {
            const int64_t before = tls_stat_value(input.tls_before, name);
            const int64_t after = tls_stat_value(input.tls_after, name);
            print_line("TLS_STAT name=", name, " before=", before,
                       " after=", after, " delta=", after - before);
        } else {
            print_line("TLS_STAT name=", name, " unavailable=yes");
        }
    }
    print_line("tls_connections_established=", tls_total,
               " new_tls_connections_this_iteration=", new_tls_connections,
               " persistent_connections_live=", live_connections,
               " persistent_ktls_connections_live=", live_ktls_connections,
               " ktls_rx_connections=", stats.ktls_rx_enabled.load(),
               " ktls_rx_manual_connections=", stats.ktls_rx_manual.load(),
               " every_connection_rx_ktls=", all_ktls ? "yes" : "NO",
               " every_connection_direct_recvmsg=", all_ktls ? "yes" : "NO",
               " ktls_control_records=", stats.ktls_control_records.load(),
               " ktls_session_tickets=", stats.ktls_session_tickets.load(),
               " ktls_rx_rekeys=", stats.ktls_rx_rekeys.load(),
               " ktls_alerts=", stats.ktls_alerts.load(),
               " no_pad_attempted=", stats.no_pad_attempted.load(),
               " no_pad_succeeded=", stats.no_pad_succeeded.load(),
               " every_eligible_no_pad=", all_no_pad ? "yes" : "NO",
               " TlsRxSw_increase_required=",
               tls_rx_sw_required ? "yes" : "no(pool_reuse)",
               " TlsRxSw_increased=",
               tls_rx_sw_required
                   ? (tls_rx_sw_observed ? "yes" : "NO")
                   : "not_required");
    print_line("OBJECTS_FINAL objects=", objects.size(),
               " objects_complete=", objects_complete,
               " allocations_live=", allocations_live,
               " expected_bytes=", expected,
               " completed_bytes=", object_completed_bytes,
               " device_contents_valid=", opt.receive_only ? "NO" : "yes",
               " ranges_completed=", stats.completed_ranges.load(),
               " ranges_total=", scheduler.task_count());
    for (const auto &object : objects) {
        const bool object_complete =
            object->completed_bytes.load() == object->size &&
            object->completed_ranges.load() == object->tasks.size();
        const bool allocation_live =
            object->size == 0 || object->device.pointer != nullptr;
        if (!object_complete || !allocation_live) {
            print_line("OBJECT_FINAL id=", object->id, " gpu=", object->gpu,
                       " expected=", object->size,
                       " completed_bytes=", object->completed_bytes.load(),
                       " completed_ranges=", object->completed_ranges.load(),
                       " ranges=", object->tasks.size(),
                       " allocation_live=", allocation_live ? "yes" : "NO",
                       " complete=", object_complete ? "yes" : "NO",
                       " uri=", object->display_uri);
        }
    }
    print_line("OVERALL ", pass ? "PASS" : "FAIL",
               pass ? "" : " reason=", pass ? "" : fatal.message());

    std::ostringstream json;
    json << std::boolalpha << std::setprecision(9)
         << "{\"pass\":" << pass
         << ",\"iteration_role\":\"measured\""
         << ",\"iteration_sequence\":" << input.sequence
         << ",\"measured_iteration\":" << input.measured_index
         << ",\"measured_iterations_configured\":" << opt.iterations
         << ",\"scan_warmup_iterations\":0"
         << ",\"connection_pool_mode\":\"head_primed\""
         << ",\"pool_connections_at_start\":"
         << input.pool_connections_at_start
         << ",\"pool_connections_live_at_end\":" << live_connections
         << ",\"pool_ktls_connections_live_at_end\":"
         << live_ktls_connections
         << ",\"new_tls_connections\":" << new_tls_connections
         << ",\"wall_s\":" << wall
         << ",\"first_body_s\":" << first_body_seconds
         << ",\"network_complete_s\":" << network_complete_seconds
         << ",\"network_active_s\":" << network_active_seconds
         << ",\"payload_sink\":\""
         << (opt.receive_only ? "receive_only" : "h2d") << '"'
         << ",\"sink_complete_s\":" << sink_complete_seconds
         << ",\"network_to_sink_drain_s\":" << network_to_sink_seconds
         << ",\"network_complete_gbps\":"
         << gbps(expected, network_complete_seconds)
         << ",\"sink_complete_gbps\":"
         << gbps(expected, sink_complete_seconds);
    if (opt.receive_only) {
        json << ",\"receive_complete_s\":" << sink_complete_seconds
             << ",\"network_to_receive_drain_s\":" << network_to_sink_seconds
             << ",\"receive_complete_gbps\":"
             << gbps(expected, sink_complete_seconds)
             << ",\"device_complete_s\":null"
             << ",\"network_to_device_drain_s\":null"
             << ",\"device_complete_gbps\":null";
    } else {
        json << ",\"device_complete_s\":" << sink_complete_seconds
             << ",\"network_to_device_drain_s\":" << network_to_sink_seconds
             << ",\"device_complete_gbps\":"
             << gbps(expected, sink_complete_seconds);
    }
    json
         << ",\"first_to_last_body_gbps\":"
         << gbps(expected, network_active_seconds)
         << ",\"pretransfer_total_s\":" << interval_seconds(
                input.startup.program_start_ns, transfer_start_ns)
         << ",\"pretransfer_credentials_s\":" << interval_seconds(
                input.startup.credentials_begin_ns,
                input.startup.credentials_end_ns)
         << ",\"catalog_load_s\":" << interval_seconds(
                input.startup.catalog_begin_ns,
                input.startup.catalog_end_ns)
         << ",\"catalog_objects\":" << input.startup.catalog_objects
         << ",\"catalog_sizes\":" << input.startup.catalog_objects
         << ",\"catalog_etags\":" << input.startup.catalog_etags
         << ",\"head_requests\":0"
         << ",\"catalog_head_requests\":0"
         << ",\"query_head_requests\":0"
         << ",\"pretransfer_gpu_allocate_plan_s\":" << interval_seconds(
                input.startup.allocation_begin_ns,
                input.startup.allocation_end_ns)
         << ",\"pretransfer_arena_reactor_prepare_s\":" << interval_seconds(
                input.startup.reactor_prepare_begin_ns,
                input.startup.reactor_prepare_end_ns)
         << ",\"preconnect_mode\":\"transport_only\""
         << ",\"preconnect_s\":" << interval_seconds(
                input.startup.preconnect_begin_ns,
                input.startup.preconnect_end_ns)
         << ",\"preconnect_connections\":"
         << input.startup.preconnect_connections
         << ",\"preconnect_ktls_connections\":"
         << input.startup.preconnect_ktls_connections
         << ",\"preconnect_tls_ready\":"
         << input.startup.preconnect_tls_ready
         << ",\"preconnect_first_tls_ready_s\":" << interval_seconds(
                input.startup.preconnect_begin_ns,
                input.startup.preconnect_first_tls_ready_ns)
         << ",\"preconnect_tls_50_s\":" << interval_seconds(
                input.startup.preconnect_begin_ns,
                input.startup.preconnect_tls_50_ns)
         << ",\"preconnect_tls_90_s\":" << interval_seconds(
                input.startup.preconnect_begin_ns,
                input.startup.preconnect_tls_90_ns)
         << ",\"preconnect_tls_100_s\":" << interval_seconds(
                input.startup.preconnect_begin_ns,
                input.startup.preconnect_tls_100_ns)
         << ",\"preconnect_retries\":"
         << input.startup.preconnect_retries
         << ",\"preconnect_reconnects\":"
         << input.startup.preconnect_reconnects
         << ",\"preconnect_http_requests\":0"
         << ",\"preconnect_s3_body_bytes\":0"
         << ",\"preconnect_h2d_bytes\":0"
         << ",\"preconnect_tls_rx_sw_delta\":";
    if (input.startup.preconnect_tls_stats_available)
        json << input.startup.preconnect_tls_rx_sw_delta;
    else
        json << "null";
    json
         << ",\"prime_mode\":\"head_object\""
         << ",\"prime_s\":" << interval_seconds(
                input.startup.prime_begin_ns,
                input.startup.prime_end_ns)
         << ",\"prime_connections\":" << input.startup.prime_connections
         << ",\"prime_ktls_connections\":"
         << input.startup.prime_ktls_connections
         << ",\"prime_http_requests\":"
         << input.startup.prime_http_requests
         << ",\"prime_responses\":" << input.startup.prime_responses
         << ",\"prime_s3_body_bytes\":0"
         << ",\"prime_h2d_bytes\":0"
         << ",\"prime_ktls_recv_calls\":"
         << input.startup.prime_ktls_recv_calls
         << ",\"prime_retries\":" << input.startup.prime_retries
         << ",\"prime_reconnects\":" << input.startup.prime_reconnects
         << ",\"get_connections_configured\":" << input.configured_connections
         << ",\"get_connections_expected\":" << expected_get_connections
         << ",\"get_tls_ready\":" << reactor_telemetry.tls_ready
         << ",\"get_first_tls_ready_s\":"
         << elapsed_seconds(reactor_telemetry.first_tls_ready_ns)
         << ",\"get_tls_50_s\":" << elapsed_seconds(get_tls_50_ns)
         << ",\"get_tls_90_s\":" << elapsed_seconds(get_tls_90_ns)
         << ",\"get_tls_100_s\":" << elapsed_seconds(get_tls_100_ns)
         << ",\"first_range_request_s\":"
         << elapsed_seconds(reactor_telemetry.first_request_ns)
         << ",\"first_range_headers_s\":"
         << elapsed_seconds(reactor_telemetry.first_headers_ns)
         << ",\"range_requests_sent\":" << reactor_telemetry.requests_sent
         << ",\"range_headers_validated\":"
         << reactor_telemetry.headers_validated
         << ",\"payload_connections_peak_sampled\":"
         << payload_connections_peak_sampled
         << ",\"startup_counters_per_reactor_cacheline\":true"
         << ",\"live_rate_scope\":\"aggregate_delta_per_sample_interval\""
         << ",\"final_entity_rate_scope\":\"entity_bytes_per_end_to_end_s\""
         << ",\"expected_bytes\":" << expected
         << ",\"object_count\":" << objects.size()
         << ",\"objects_complete\":" << objects_complete
         << ",\"allocations_live\":" << allocations_live
         << ",\"device_contents_valid\":" << !opt.receive_only
         << ",\"s3_body_bytes\":" << body
         << ",\"retry_s3_body_bytes\":" << retry_body_bytes
         << ",\"h2d_completed_bytes\":" << h2d
         << ",\"retry_h2d_bytes\":" << retry_h2d_bytes
         << ",\"h2d_completed_copies\":" << stats.h2d_completed_copies.load()
         << ",\"h2d_inline_batches\":" << stats.h2d_inline_batches.load()
         << ",\"h2d_inline_batch_avg_copies\":"
         << (stats.h2d_inline_batches.load() != 0
                 ? static_cast<double>(stats.h2d_submitted_copies.load()) /
                       static_cast<double>(stats.h2d_inline_batches.load())
                 : 0.0)
         << ",\"h2d_inline_event_queries\":"
         << stats.h2d_inline_event_queries.load()
         << ",\"h2d_copy_avg_bytes\":"
         << (stats.h2d_completed_copies.load() != 0
                 ? static_cast<double>(h2d) /
                       static_cast<double>(stats.h2d_completed_copies.load())
                 : 0.0)
         << ",\"receive_only_bytes\":" << receive_only
         << ",\"receive_only_chunks\":" << receive_only_chunks
         << ",\"h2d_mode\":\""
         << (opt.receive_only ? "disabled" : "same_reactor_batch")
         << '"'
         << ",\"useful_object_gbps\":" << gbps(expected, wall)
         << ",\"s3_end_to_end_gbps\":" << gbps(body, wall)
         << ",\"h2d_end_to_end_gbps\":" << gbps(h2d, wall)
         << ",\"ranges\":" << scheduler.task_count()
         << ",\"ranges_completed\":" << stats.completed_ranges.load()
         << ",\"retries\":" << stats.retries.load()
         << ",\"reconnects\":" << stats.reconnects.load()
         << ",\"http_errors\":" << stats.http_errors.load()
         << ",\"tls_errors\":" << stats.tls_errors.load()
         << ",\"cuda_errors\":" << stats.cuda_errors.load()
         << ",\"slot_bytes\":" << opt.slot_bytes
         << ",\"range_bytes\":" << opt.range_bytes
         << ",\"pinned_hwm_bytes\":" << input.pinned_hwm
         << ",\"pinned_registered_bytes\":" << input.pinned_bytes
         << ",\"pinned_peak_slots\":" << stats.pinned_peak.load()
         << ",\"ring_stall_s\":" << ring_seconds
         << ",\"ring_full_percent\":" << ring_percent
         << ",\"tls_connections\":" << tls_total
         << ",\"ktls_rx_connections\":" << stats.ktls_rx_enabled.load()
         << ",\"ktls_rx_manual_connections\":" << stats.ktls_rx_manual.load()
         << ",\"direct_recvmsg\":" << all_ktls
         << ",\"ktls_recv_calls\":" << ktls_recv_calls
         << ",\"ktls_recv_eagain\":" << ktls_recv_eagain
         << ",\"epoll_ctl_calls\":" << epoll_ctl_calls
         << ",\"epoll_ctl_noop_skips\":" << epoll_ctl_skips
         << ",\"ktls_control_records\":" << stats.ktls_control_records.load()
         << ",\"ktls_session_tickets\":" << stats.ktls_session_tickets.load()
         << ",\"ktls_rx_rekeys\":" << stats.ktls_rx_rekeys.load()
         << ",\"ktls_alerts\":" << stats.ktls_alerts.load()
         << ",\"no_pad_attempted\":" << stats.no_pad_attempted.load()
         << ",\"no_pad_succeeded\":" << stats.no_pad_succeeded.load()
         << ",\"no_pad_ok\":" << all_no_pad
         << ",\"tls_rx_sw_increase_required\":" << tls_rx_sw_required
         << ",\"tls_rx_sw_increased\":" << tls_rx_sw_observed
         << ",\"tls_rx_sw_validation_ok\":" << tls_rx_sw_validation_ok
         << ",\"ramp_100ms\":[";
    for (size_t i = 0; i < input.ramp_samples.size(); ++i) {
        if (i != 0) json << ',';
        const RampSample &sample = input.ramp_samples[i];
        json << "[" << sample.elapsed_ms << ',' << sample.tls_ready << ','
             << sample.requests_sent << ',' << sample.headers_validated << ','
             << sample.payload_connections << ',' << sample.active_ranges << ','
             << sample.body_bytes << ']';
    }
    json << "],\"lanes\":[";
    for (size_t i = 0; i < lanes.size(); ++i) {
        if (i != 0) json << ',';
        const Lane &lane = lanes[i];
        const LaneTelemetryAggregate &lane_telemetry =
            reactor_telemetry.lane.at(static_cast<size_t>(lane.id));
        json << "{\"id\":" << lane.id << ",\"nic\":\""
             << json_escape(lane.nic) << "\",\"nic_numa\":" << lane.nic_numa
             << ",\"gpu\":" << lane.gpu << ",\"gpu_numa\":" << lane.gpu_numa
             << ",\"end_to_end_body_contribution_gbps\":" << gbps(
                    stats.lane.at(static_cast<size_t>(lane.id))->body_bytes.load(), wall)
             << ",\"first_body_s\":" << elapsed_seconds(
                    lane_telemetry.first_body_ns)
             << ",\"last_body_s\":" << elapsed_seconds(
                    lane_telemetry.last_body_ns)
             << ",\"first_to_last_body_gbps\":" << gbps(
                    stats.lane.at(static_cast<size_t>(lane.id))->body_bytes.load(),
                    interval_seconds(
                        lane_telemetry.first_body_ns,
                        lane_telemetry.last_body_ns))
             << ",\"post_network_idle_s\":"
             << (lane_telemetry.last_body_ns != 0 &&
                         network_complete_ns >= lane_telemetry.last_body_ns
                     ? static_cast<double>(network_complete_ns -
                           lane_telemetry.last_body_ns) / 1.0e9
                     : 0.0)
             << ",\"ranges_total\":"
             << lane_ranges_total.at(static_cast<size_t>(lane.id))
             << ",\"network_ranges_complete\":"
             << lane_telemetry.network_ranges_completed
             << ",\"device_ranges_complete\":"
             << lane_telemetry.device_ranges_completed
             << ",\"cpus\":[";
        for (size_t c = 0; c < lane.reactor_cpus.size(); ++c) {
            if (c != 0) json << ',';
            json << lane.reactor_cpus[c];
        }
        json << "]}";
    }
    json << "],\"gpus\":[";
    size_t gpu_index = 0;
    for (const auto &[gpu, counters] : stats.gpu) {
        if (gpu_index++ != 0) json << ',';
        json << "{\"id\":" << gpu
             << ",\"h2d_bytes\":" << counters->h2d_completed_bytes.load()
             << ",\"end_to_end_h2d_contribution_gbps\":"
             << gbps(counters->h2d_completed_bytes.load(), wall) << '}';
    }
    json << "],\"tls_stat_delta\":{";
    for (size_t i = 0; i < kRequiredTlsStats.size(); ++i) {
        if (i != 0) json << ',';
        const std::string name(kRequiredTlsStats[i]);
        json << '"' << name << "\":";
        if (input.tls_before_available && input.tls_after_available)
            json << tls_stat_value(input.tls_after, name) -
                    tls_stat_value(input.tls_before, name);
        else
            json << "null";
    }
    json << '}';
    if (!pass) json << ",\"error\":\"" << json_escape(fatal.message()) << '"';
    json << '}';
    print_line("RESULT_JSON=", json.str());
    return pass;
}

void require_self_test(bool condition, const std::string &label) {
    if (!condition) fail("self-test failed: " + label);
}

void run_self_tests() {
    require_self_test(sha256_hex("") == kEmptySha256, "SHA256 empty payload");
    std::array<unsigned char, 20> hmac_key{};
    hmac_key.fill(0x0b);
    const auto hmac = hmac_sha256(hmac_key.data(), hmac_key.size(), "Hi There");
    require_self_test(
        hex_lower(hmac.data(), hmac.size()) ==
            "b0344c61d8db38535ca8afceaf0bf12b"
            "881dc200c9833da726e9376c2e32cff7",
        "RFC 4231 HMAC-SHA256 vector");

    // RFC 8448's server application traffic secret and derived AES-128-GCM
    // key/IV catch label framing, HMAC, and traffic-secret extraction errors
    // before any socket can be transitioned to kTLS.
    constexpr std::string_view rfc8448_secret_hex =
        "a11af9f05531f856ad47116b45a95032"
        "8204b4f44bfb6b3a4b4f1f3fcb631643";
    std::array<unsigned char, 32> rfc8448_secret{};
    for (size_t i = 0; i < rfc8448_secret.size(); ++i) {
        const int high = hex_nibble(rfc8448_secret_hex[i * 2U]);
        const int low = hex_nibble(rfc8448_secret_hex[i * 2U + 1U]);
        require_self_test(high >= 0 && low >= 0,
                          "RFC 8448 vector hex parsing");
        rfc8448_secret[i] =
            static_cast<unsigned char>((high << 4) | low);
    }
    std::array<unsigned char, 16> rfc8448_key{};
    std::array<unsigned char, 12> rfc8448_iv{};
    tls13_hkdf_expand_label("SHA256", 32, rfc8448_secret.data(),
                            rfc8448_secret.size(), "key",
                            rfc8448_key.data(), rfc8448_key.size());
    tls13_hkdf_expand_label("SHA256", 32, rfc8448_secret.data(),
                            rfc8448_secret.size(), "iv",
                            rfc8448_iv.data(), rfc8448_iv.size());
    require_self_test(
        hex_lower(rfc8448_key.data(), rfc8448_key.size()) ==
            "9f02283b6c9c07efc26bb9f2ac92e356" &&
        hex_lower(rfc8448_iv.data(), rfc8448_iv.size()) ==
            "cf782b88dd83549aadf1e984",
        "RFC 8448 TLS 1.3 HKDF-Expand-Label vectors");
    std::array<unsigned char, 48> sha384_secret{};
    for (size_t i = 0; i < sha384_secret.size(); ++i)
        sha384_secret[i] = static_cast<unsigned char>(i);
    std::array<unsigned char, 32> sha384_key{};
    std::array<unsigned char, 12> sha384_iv{};
    std::array<unsigned char, 48> sha384_update{};
    tls13_hkdf_expand_label("SHA384", 48, sha384_secret.data(),
                            sha384_secret.size(), "key",
                            sha384_key.data(), sha384_key.size());
    tls13_hkdf_expand_label("SHA384", 48, sha384_secret.data(),
                            sha384_secret.size(), "iv",
                            sha384_iv.data(), sha384_iv.size());
    tls13_hkdf_expand_label("SHA384", 48, sha384_secret.data(),
                            sha384_secret.size(), "traffic upd",
                            sha384_update.data(), sha384_update.size());
    require_self_test(
        hex_lower(sha384_key.data(), sha384_key.size()) ==
            "6877d022f1c61d24ebb7487c16752d9a"
            "4798e40431c75b39320e537c90e23225" &&
        hex_lower(sha384_iv.data(), sha384_iv.size()) ==
            "42822531a0fe88648fc09e9f" &&
        hex_lower(sha384_update.data(), sha384_update.size()) ==
            "401331b63e9d59f202e8f041042d9516"
            "f4cd7fa2e2ee14631d3b49fc340d7af3"
            "7fc2c0c9f252d8036f81ec5b85cbe5db",
        "TLS 1.3 SHA384 key/IV/traffic-update vectors");
    TlsRxState captured;
    const std::string keylog_line =
        "SERVER_TRAFFIC_SECRET_0 " + std::string(64, '0') + " " +
        std::string(rfc8448_secret_hex);
    captured.capture_keylog_line(keylog_line);
    require_self_test(!captured.capture_failed &&
                          captured.server_traffic_secret_size == 32 &&
                          CRYPTO_memcmp(captured.server_traffic_secret.data(),
                                       rfc8448_secret.data(), 32) == 0,
                      "OpenSSL key-log traffic-secret capture");
    const std::array<unsigned char, 14> minimal_ticket{{
        0, 0, 0, 0,  // lifetime
        0, 0, 0, 0,  // age_add
        0,           // nonce length
        0, 1, 0xaa, // ticket length + ticket
        0, 0         // extension length
    }};
    require_self_test(valid_tls13_new_session_ticket(
                          minimal_ticket.data(), minimal_ticket.size()),
                      "TLS 1.3 NewSessionTicket framing");
    require_self_test(percent_decode("a%20b%2Fc") == "a b/c",
                      "percent decoding");
    require_self_test(uri_encode_path("a b/%") == "/a%20b/%25",
                      "SigV4 URI path encoding");
    require_self_test(parse_cpu_list("1-3,5") == std::vector<int>({1, 2, 3, 5}),
                      "CPU-list parsing");
    require_self_test(
        normalize_bool_module_parameter("Y\n") == "enabled" &&
            normalize_bool_module_parameter("0") == "disabled" &&
            normalize_bool_module_parameter("") == "unavailable",
        "optional driver module-parameter reporting");
    const std::string wire =
        "HTTP/1.1 206 Partial Content\r\n"
        "Content-Length: 3\r\n"
        "Content-Range: bytes 7-9/10\r\n"
        "Connection: keep-alive\r\n\r\nABC";
    HeaderAccumulator accumulator;
    size_t body_offset = 0;
    require_self_test(accumulator.consume(
                          reinterpret_cast<const unsigned char *>(wire.data()),
                          wire.size(), body_offset),
                      "incremental HTTP terminator");
    require_self_test(wire.substr(body_offset) == "ABC",
                      "header/body in-place offset");
    require_self_test(accumulator.parsed().status == 206,
                      "HTTP status parsing");
    require_self_test(accumulator.parsed().content_length ==
                          std::optional<uint64_t>(3),
                      "Content-Length parsing");
    const ContentRange range = parse_content_range(
        accumulator.parsed().get("content-range").value_or(""));
    require_self_test(range.start == 7 && range.end == 9 && range.total == 10,
                      "Content-Range parsing");
    Credentials credentials;
    credentials.access_key = "AKIDEXAMPLE";
    credentials.secret_key = "secret-example";
    credentials.session_token = "token-example";
    const std::string signed_request = make_signed_request(
        "GET", {"bucket.s3.us-east-2.amazonaws.com", "/a%20b"},
        std::make_pair<uint64_t, uint64_t>(0, 9), "\"etag-example\"",
        "us-east-2", credentials, true);
    require_self_test(signed_request.find("Range: bytes=0-9\r\n") !=
                          std::string::npos &&
                      signed_request.find("Authorization: AWS4-HMAC-SHA256 ") !=
                          std::string::npos &&
                      signed_request.find("x-amz-security-token: token-example\r\n") !=
                          std::string::npos &&
                      signed_request.find("If-Match: \"etag-example\"\r\n") !=
                          std::string::npos &&
                      signed_request.find("secret-example") == std::string::npos,
                      "SigV4 request construction");
    const std::string signed_head = make_signed_request(
        "HEAD", {"bucket.s3.us-east-2.amazonaws.com", "/a%20b"},
        std::nullopt, std::nullopt, "us-east-2", credentials, true);
    require_self_test(starts_with(signed_head, "HEAD /a%20b HTTP/1.1\r\n") &&
                          signed_head.find("Range:") == std::string::npos &&
                          signed_head.find("If-Match:") == std::string::npos &&
                          signed_head.find("Connection: keep-alive\r\n") !=
                              std::string::npos,
                      "bodyless HEAD-prime request construction");
    const std::string head_wire =
        "HTTP/1.1 200 OK\r\n"
        "Content-Length: 10\r\n"
        "ETag: \"etag-example\"\r\n"
        "Connection: keep-alive\r\n\r\n";
    HeaderAccumulator head_accumulator;
    size_t head_body_offset = 0;
    require_self_test(
        head_accumulator.consume(
            reinterpret_cast<const unsigned char *>(head_wire.data()),
            head_wire.size(), head_body_offset) &&
            head_body_offset == head_wire.size() &&
            head_accumulator.parsed().status == 200 &&
            head_accumulator.parsed().content_length ==
                std::optional<uint64_t>(10) &&
            head_accumulator.parsed().get("etag") ==
                std::optional<std::string>("\"etag-example\"") &&
            !head_accumulator.parsed().connection_close,
        "bodyless HEAD-prime response parsing");
    require_self_test(json_escape("a\n\"b") == "a\\n\\\"b",
                      "JSON escaping");
    const CatalogObjectSpec hinted = parse_catalog_snapshot_line(
        "size=123\tetag=\"abc-2\"\ts3://bucket/path/a.parquet", 1);
    require_self_test(hinted.size == 123 &&
                          hinted.etag == std::optional<std::string>("\"abc-2\"") &&
                          hinted.bucket == "bucket" &&
                          hinted.raw_key == "path/a.parquet",
                      "catalog size/ETag parsing");
    bool uri_only_rejected = false;
    try {
        (void)parse_catalog_snapshot_line("s3://bucket/path/b.parquet", 2);
    } catch (const std::exception &) {
        uri_only_rejected = true;
    }
    require_self_test(uri_only_rejected,
                      "catalog rejects entries without exact sizes");
    require_self_test(
        transfer_byte_accounting_matches(false, 100, 125, 125, 125, 0) &&
            transfer_byte_accounting_matches(true, 100, 125, 0, 0, 125) &&
            !transfer_byte_accounting_matches(false, 100, 99, 99, 99, 0),
        "retry bytes remain physical telemetry while useful bytes stay exact");
    Object learned_etag;
    require_self_test(validate_or_learn_object_etag(learned_etag, "\"v1\"") &&
                          validate_or_learn_object_etag(learned_etag, "\"v1\"") &&
                          !validate_or_learn_object_etag(learned_etag, "\"v2\"") &&
                          object_etag_for_request(learned_etag) ==
                              std::optional<std::string>("\"v1\""),
                      "first-GET ETag snapshot");

    // Exercise the off-hot-path iteration reset without CUDA or a socket.
    // This catches stale range attempts/object counters before a remote pooled
    // query can overwrite a real device allocation.
    {
        std::vector<std::unique_ptr<Object>> reset_objects;
        auto object = std::make_unique<Object>();
        object->id = 0;
        object->gpu = 0;
        object->size = 10;
        reset_objects.push_back(std::move(object));
        Lane lane;
        lane.id = 0;
        lane.gpu = 0;
        std::vector<Lane> reset_lanes{lane};
        RunStats reset_stats(1, std::set<int>{0});
        FatalState reset_fatal;
        Scheduler reset_scheduler(
            reset_objects, reset_lanes, 4, reset_stats, reset_fatal);
        const std::vector<Object *> preconnect_targets =
            reset_scheduler.preconnect_targets(0, 5);
        require_self_test(
            preconnect_targets.size() == 5 &&
                std::all_of(preconnect_targets.begin(),
                            preconnect_targets.end(),
                            [&](const Object *candidate) {
                                return candidate == reset_objects[0].get();
                            }) &&
                reset_scheduler.remaining() == reset_scheduler.task_count(),
            "preconnect target selection consumes no Range tasks");
        for (size_t i = 0; i < reset_scheduler.task_count(); ++i) {
            RangeTask *task = reset_scheduler.pop(0);
            require_self_test(task != nullptr, "iteration reset task pop");
            task->received = task->length;
            task->h2d_completed_attempt = task->length;
            reset_scheduler.received_all(*task);
            require_self_test(reset_scheduler.complete_if_ready(*task),
                              "iteration reset task completion");
        }
        require_self_test(reset_scheduler.remaining() == 0 &&
                              reset_objects[0]->completed_bytes.load() == 10,
                          "iteration reset initial completion");
        reset_scheduler.reset_for_iteration();
        require_self_test(
            reset_scheduler.remaining() == reset_scheduler.task_count() &&
                reset_objects[0]->completed_bytes.load() == 0 &&
                reset_objects[0]->completed_ranges.load() == 0,
            "iteration reset scheduler/object accounting");
        reset_stats.body_bytes.store(10);
        reset_stats.active_connections.store(3);
        reset_stats.tls_established.store(3);
        reset_stats.reset_transfer_counters();
        require_self_test(reset_stats.body_bytes.load() == 0 &&
                              reset_stats.active_connections.load() == 3 &&
                              reset_stats.tls_established.load() == 3,
                          "iteration reset preserves connection/TLS audit");
    }
    OPENSSL_cleanse(rfc8448_secret.data(), rfc8448_secret.size());
    OPENSSL_cleanse(rfc8448_key.data(), rfc8448_key.size());
    OPENSSL_cleanse(rfc8448_iv.data(), rfc8448_iv.size());
    OPENSSL_cleanse(sha384_secret.data(), sha384_secret.size());
    OPENSSL_cleanse(sha384_key.data(), sha384_key.size());
    OPENSSL_cleanse(sha384_iv.data(), sha384_iv.size());
    OPENSSL_cleanse(sha384_update.data(), sha384_update.size());
    print_line("SELF_TEST PASS sha256=yes hmac_sha256=yes tls13_hkdf=yes "
               "keylog_capture=yes tls_control=yes uri=yes http=yes "
               "content_range=yes sigv4=yes json=yes catalog_snapshot=yes "
               "retry_accounting=yes transport_only_preconnect=yes "
               "head_connection_prime=yes");
}

int run_program(int argc, char **argv) {
    const Options opt = parse_cli(argc, argv);
    if (opt.help) {
        usage(argv[0]);
        return 0;
    }
    if (opt.self_test) {
        run_self_tests();
        return 0;
    }

    StartupTimings startup;
    startup.program_start_ns = now_ns();

    bool process_tls_stats_available = false;
    (void)read_tls_stats(process_tls_stats_available);
    const std::vector<NicInfo> nics = discover_nics();
    const CpuInfo cpus = discover_cpus();
    int cuda_driver = 0;
    int cuda_runtime = 0;
    std::vector<GpuInfo> gpus = discover_gpus(cuda_driver, cuda_runtime);
    std::vector<Lane> lanes = build_lanes(opt, nics, gpus, cpus);
    const std::set<int> gpu_ids = enabled_gpu_ids(lanes);
    RunStats stats(lanes.size(), gpu_ids);
    FatalState fatal;
    startup.credentials_begin_ns = now_ns();
    const Credentials credentials = load_credentials();
    startup.credentials_end_ns = now_ns();
    validate_credentials(credentials);
    const std::vector<std::string> endpoint_overrides =
        load_endpoint_overrides(opt);
    startup.catalog_begin_ns = now_ns();
    const std::vector<CatalogObjectSpec> specs = read_catalog_snapshot(opt);
    std::vector<std::unique_ptr<Object>> objects =
        make_objects(specs, opt, endpoint_overrides);
    startup.catalog_end_ns = now_ns();
    SslContext ssl_context;

    size_t catalog_etag_count = 0;
    uint64_t total_object_bytes = 0;
    for (const auto &object : objects) {
        total_object_bytes += object->size;
        if (object->etag_from_catalog) ++catalog_etag_count;
    }
    startup.catalog_objects = objects.size();
    startup.catalog_etags = catalog_etag_count;
    print_line("CATALOG_SNAPSHOT objects=", objects.size(),
               " size_metadata=all",
               " etag_metadata=", catalog_etag_count,
               " head_requests=0",
               " catalog_head_requests=0",
               " bytes=", total_object_bytes);

    startup.allocation_begin_ns = now_ns();
    assign_and_allocate_objects(objects, gpus, lanes, opt.gpu_reserve_bytes);
    Scheduler scheduler(objects, lanes, opt.range_bytes, stats, fatal);
    startup.allocation_end_ns = now_ns();

    if (opt.slot_bytes > std::numeric_limits<size_t>::max())
        fail("slot size exceeds size_t");
    const long host_page_size = ::sysconf(_SC_PAGESIZE);
    if (host_page_size <= 0 ||
        opt.slot_bytes % static_cast<uint64_t>(host_page_size) != 0)
        fail("slot size must be a multiple of the host page size");
    startup.reactor_prepare_begin_ns = now_ns();
    std::vector<std::unique_ptr<PinnedArena>> arenas;
    arenas.reserve(lanes.size());
    uint64_t pinned_bytes = 0;
    uint64_t total_slots = 0;
    for (const Lane &lane : lanes) {
        auto arena = std::make_unique<PinnedArena>(
            lane, static_cast<size_t>(opt.slot_bytes));
        pinned_bytes += arena->bytes();
        total_slots += arena->slots();
        arenas.push_back(std::move(arena));
    }
    if (pinned_bytes > opt.pinned_hwm_bytes)
        fail("internal error: registered pinned bytes exceed HWM");

    std::vector<std::unique_ptr<Reactor>> reactors;
    uint64_t configured_connections = 0;
    for (size_t lane_index = 0; lane_index < lanes.size(); ++lane_index) {
        const Lane &lane = lanes[lane_index];
        const std::vector<Object *> lane_preconnect_targets =
            scheduler.preconnect_targets(lane.id, lane.connections);
        size_t target_offset = 0;
        const size_t reactor_count = lane.reactor_cpus.size();
        const size_t slots_per_reactor = lane.slots / reactor_count;
        const unsigned base_connections =
            lane.connections / static_cast<unsigned>(reactor_count);
        const unsigned extra_connections =
            lane.connections % static_cast<unsigned>(reactor_count);
        for (size_t r = 0; r < reactor_count; ++r) {
            const unsigned connection_count = base_connections +
                (r < extra_connections ? 1U : 0U);
            std::vector<Object *> reactor_preconnect_targets(
                lane_preconnect_targets.begin() +
                    static_cast<std::ptrdiff_t>(target_offset),
                lane_preconnect_targets.begin() +
                    static_cast<std::ptrdiff_t>(target_offset + connection_count));
            target_offset += connection_count;
            configured_connections += connection_count;
            reactors.push_back(std::make_unique<Reactor>(
                lane, r, lane.reactor_cpus[r],
                std::move(reactor_preconnect_targets),
                *arenas[lane_index], r * slots_per_reactor,
                slots_per_reactor, ssl_context.get(), opt, credentials,
                scheduler, stats, fatal));
        }
        if (target_offset != lane_preconnect_targets.size())
            fail("reactor connection partition does not cover its lane");
    }
    verify_gpu_reserve(gpu_ids, opt.gpu_reserve_bytes,
                       "stream/event/pinned-arena initialization");
    startup.reactor_prepare_end_ns = now_ns();
    const int telemetry_cpu = choose_telemetry_cpu(lanes);

    print_config(opt, nics, gpus, lanes, objects, credentials,
                 endpoint_overrides, telemetry_cpu,
                 cuda_driver, cuda_runtime, process_tls_stats_available,
                 pinned_bytes, pinned_bytes);

    // Establish transport state only.  No Range is popped and no HTTP request
    // is constructed or sent in this phase.  validate_and_report_preconnect()
    // makes that zero-payload contract a runtime invariant rather than a
    // methodology promise.
    stats.reset_transfer_counters();
    for (auto &reactor : reactors) reactor->prepare_for_preconnect();
    bool preconnect_tls_before_available = false;
    const TlsStatMap preconnect_tls_before =
        read_tls_stats(preconnect_tls_before_available);
    const auto preconnect_start = Clock::now();
    startup.preconnect_begin_ns = clock_time_ns(preconnect_start);
    print_line("PRECONNECT_START mode=transport_only",
               " configured_connections=", configured_connections,
               " http_requests=0 payload_bytes=0 scheduler_tasks_consumed=0");
    for (auto &reactor : reactors) reactor->start();
    for (auto &reactor : reactors) reactor->join();
    const auto preconnect_end = Clock::now();
    startup.preconnect_end_ns = clock_time_ns(preconnect_end);
    bool preconnect_tls_after_available = false;
    const TlsStatMap preconnect_tls_after =
        read_tls_stats(preconnect_tls_after_available);
    if (!validate_and_report_preconnect(
            objects, reactors, scheduler, stats, fatal,
            configured_connections, preconnect_start, preconnect_end,
            preconnect_tls_before, preconnect_tls_after,
            preconnect_tls_before_available, preconnect_tls_after_available,
            startup))
        return 2;

    // Exercise the S3 HTTP request path once on every retained transport
    // without transferring object payload or consuming a Range.  This makes
    // the measured pool S3-ready while keeping the distinction between
    // connection priming and scan/data warming mechanically auditable.
    stats.reset_transfer_counters();
    for (auto &reactor : reactors) reactor->prepare_for_prime();
    const auto prime_start = Clock::now();
    startup.prime_begin_ns = clock_time_ns(prime_start);
    print_line("PRIME_START mode=head_object",
               " configured_connections=", configured_connections,
               " expected_http_requests=", configured_connections,
               " expected_payload_bytes=0 scheduler_tasks_consumed=0");
    for (auto &reactor : reactors) reactor->start();
    for (auto &reactor : reactors) reactor->join();
    const auto prime_end = Clock::now();
    startup.prime_end_ns = clock_time_ns(prime_end);
    if (!validate_and_report_prime(
            objects, reactors, scheduler, stats, fatal,
            configured_connections, prime_start, prime_end, startup))
        return 2;

    bool pass = false;
    for (unsigned sequence = 1; sequence <= opt.iterations; ++sequence) {
        if (sequence > 1) scheduler.reset_for_iteration();
        stats.reset_transfer_counters();
        for (auto &reactor : reactors) reactor->prepare_for_iteration();

        const unsigned measured_index = sequence;
        const uint64_t pool_connections_at_start =
            live_reactor_connections(reactors);
        const uint64_t pool_ktls_at_start =
            live_reactor_ktls_connections(reactors);
        if (pool_connections_at_start != pool_ktls_at_start)
            fail("preconnected pool contains a live connection without RX kTLS");
        if (sequence == 1 && pool_connections_at_start != configured_connections)
            fail("first measured query did not start with the full preconnected pool");
        print_line("ITERATION_START role=measured",
                   " sequence=", sequence,
                   " measured_index=", measured_index,
                   " pool_mode=head_primed",
                   " pool_connections_at_start=", pool_connections_at_start,
                   " pool_ktls_connections_at_start=", pool_ktls_at_start);

        bool iteration_tls_before_available = false;
        const TlsStatMap iteration_tls_before =
            read_tls_stats(iteration_tls_before_available);
        const uint64_t tls_connections_before = stats.tls_established.load();
        const std::map<std::string, uint64_t> nic_before =
            sample_nic_rx(lanes);
        rusage usage_before{};
        if (::getrusage(RUSAGE_SELF, &usage_before) != 0)
            fail("getrusage before transfer: " + errno_string());
        const auto transfer_start = Clock::now();
        Telemetry telemetry(lanes, reactors, stats, transfer_start,
                            configured_connections, telemetry_cpu);
        telemetry.start();
        for (auto &reactor : reactors) reactor->start();
        for (auto &reactor : reactors) reactor->join();
        const auto transfer_end = Clock::now();
        telemetry.stop();
        const std::map<std::string, uint64_t> nic_after =
            sample_nic_rx(lanes);
        bool iteration_tls_after_available = false;
        const TlsStatMap iteration_tls_after =
            read_tls_stats(iteration_tls_after_available);
        rusage usage_after{};
        if (::getrusage(RUSAGE_SELF, &usage_after) != 0)
            fail("getrusage after transfer: " + errno_string());

        const bool last = sequence == opt.iterations;
        pass = print_iteration_checkpoint(
            opt, objects, reactors, scheduler, stats, fatal,
            transfer_start, transfer_end,
            sequence, measured_index,
            pool_connections_at_start, tls_connections_before);
        if (!last) {
            if (!pass) break;
            continue;
        }

        FinalInputs final;
        final.start = transfer_start;
        final.end = transfer_end;
        final.nic_before = nic_before;
        final.nic_after = nic_after;
        final.tls_before = iteration_tls_before;
        final.tls_after = iteration_tls_after;
        final.tls_before_available = iteration_tls_before_available;
        final.tls_after_available = iteration_tls_after_available;
        final.usage = usage_after;
        final.usage_before = usage_before;
        final.pinned_hwm = opt.pinned_hwm_bytes;
        final.pinned_bytes = pinned_bytes;
        final.total_slots = total_slots;
        final.configured_connections = configured_connections;
        final.pool_connections_at_start = pool_connections_at_start;
        final.tls_connections_before = tls_connections_before;
        final.sequence = sequence;
        final.measured_index = measured_index;
        final.startup = startup;
        final.ramp_samples = telemetry.ramp_samples();
        pass = print_final_summary(opt, lanes, objects, reactors, scheduler,
                                   stats, fatal, final) && pass;
    }
    // DeviceAllocation members remain alive until after the summary and
    // RESULT_JSON have been emitted.  Their destructors cudaFree only on exit.
    return pass ? 0 : 2;
}

}  // namespace ref

int main(int argc, char **argv) {
    try {
        return ref::run_program(argc, argv);
    } catch (const std::exception &e) {
        ref::print_line("STARTUP_FAIL: ", e.what());
        ref::print_line("RESULT_JSON={\"pass\":false,\"startup_error\":\"",
                        ref::json_escape(e.what()), "\"}");
        return 2;
    }
}
