// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// const_emit_microbench — isolated A/B of "constant string emission" strategies for the JSON
// output formatter glue (session 2026-07-14; engine A/B was a WASH, this isolates the ceiling).
//
// Arms (same byte output, same destination pattern):
//   imm         chunked 8/4/2/1 immediate stores (the writeConstantBytes / bytes-folded-into-IR
//               shape; on x86 each 8B chunk = movabs r64,imm64 (10-byte insn) + store)
//   cpy_inline  memcpy(dst, OPAQUE src, CONST len) -> clang inline-expands to load+store pairs;
//               src pointer laundered so the bytes are invisible (the engine JIT's inttoptr
//               situation, in its IDEAL lowering)
//   cpy_call    same, but through a volatile fn pointer -> a real libc memcpy CALL per constant
//               (what the nautilus JIT ACTUALLY emitted: memcpy$pltgot, ~5.7% self at W1)
//   cpy_visible memcpy(dst, constexpr literal, CONST len) -> clang sees the bytes AND the
//               length; whatever it picks is the compiler's own preferred lowering (referee)
//
// Workloads:
//   glue-only    p17 p17 p14 p13 s2 = 63 B/row back-to-back (max constant density = max gap)
//   interleaved  the bid_star json row shape: prefix + variable 4..13B value copy x4 + suffix
//                (~95 B/row; value copies identical across arms, unpredictable dst advance
//                defeats cross-field store merging - the realistic case)
//
// GOTCHAS baked in: arms ALTERNATE inside every rep (thermal drift neutralized - the iouring
// sessions' lesson); dst is a 128 KiB ring (matches the engine's operator_buffer_size, L2-not-L1
// resident); src prefixes for the opaque arms live in heap copies behind an asm launder; buffer
// is checksummed after every arm (DCE guard); each arm is noinline so `objdump -d` can verify
// the codegen assumption per arm (imm: movabs+mov, cpy_inline: mov load+store, cpy_call:
// call *%r__, cpy_visible: compiler's choice).
//
// Build & run (host, same clang the repo builds with):
//   c++ -O3 -march=native -std=c++20 -o /tmp/const_emit scripts/const_emit_microbench.cpp
//   /tmp/const_emit            # ~30 s, prints a table; median of 7 alternating reps
//   objdump -d --no-show-raw-insn /tmp/const_emit | less   # verify per-arm codegen

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace
{

// The bid_star json prefixes (exactly the engine's fieldPrefixes for SELECT * on bid).
constexpr char P0[] = "{\"BID$TIMESTAMP\":"; // 17
constexpr char P1[] = ",\"BID$AUCTIONID\":"; // 17
constexpr char P2[] = ",\"BID$BIDDER\":"; // 14
constexpr char P3[] = ",\"BID$PRICE\":"; // 13
constexpr char SUF[] = "}\n"; // 2
constexpr size_t L0 = sizeof(P0) - 1, L1 = sizeof(P1) - 1, L2 = sizeof(P2) - 1, L3 = sizeof(P3) - 1, LS = sizeof(SUF) - 1;

// Heap copies whose pointers are laundered per use -> bytes invisible to the optimizer.
char* opaque[5];

inline const char* launder(const char* p)
{
    __asm__ volatile("" : "+r"(p));
    return p;
}

// Volatile fn pointer -> the call cannot be inlined or recognized as builtin.
using MemcpyFn = void* (*)(void*, const void*, size_t);
volatile MemcpyFn g_memcpy = &std::memcpy;

// constexpr little-endian chunk extraction for the imm arm.
template <size_t K>
constexpr uint64_t c64(const char (&s)[K], size_t o)
{
    uint64_t v = 0;
    for (int i = 7; i >= 0; --i)
        v = (v << 8) | static_cast<uint8_t>(s[o + i]);
    return v;
}
template <size_t K>
constexpr uint32_t c32(const char (&s)[K], size_t o)
{
    uint32_t v = 0;
    for (int i = 3; i >= 0; --i)
        v = (v << 8) | static_cast<uint8_t>(s[o + i]);
    return v;
}
template <size_t K>
constexpr uint16_t c16(const char (&s)[K], size_t o)
{
    return static_cast<uint16_t>(static_cast<uint8_t>(s[o]) | (static_cast<uint8_t>(s[o + 1]) << 8));
}

inline void st64(uint8_t* d, uint64_t v)
{
    std::memcpy(d, &v, 8);
}
inline void st32(uint8_t* d, uint32_t v)
{
    std::memcpy(d, &v, 4);
}
inline void st16(uint8_t* d, uint16_t v)
{
    std::memcpy(d, &v, 2);
}

// ── per-arm constant emitters (one method per prefix -> true interleaving possible) ───────────
// imm: 17 = 8+8+1, 14 = 8+4+2, 13 = 8+4+1, 2 = 2 (same chunking as writeConstantBytes)
struct ArmImm
{
    static inline uint8_t* p0(uint8_t* d)
    {
        st64(d, c64(P0, 0));
        st64(d + 8, c64(P0, 8));
        d[16] = P0[16];
        return d + L0;
    }
    static inline uint8_t* p1(uint8_t* d)
    {
        st64(d, c64(P1, 0));
        st64(d + 8, c64(P1, 8));
        d[16] = P1[16];
        return d + L1;
    }
    static inline uint8_t* p2(uint8_t* d)
    {
        st64(d, c64(P2, 0));
        st32(d + 8, c32(P2, 8));
        st16(d + 12, c16(P2, 12));
        return d + L2;
    }
    static inline uint8_t* p3(uint8_t* d)
    {
        st64(d, c64(P3, 0));
        st32(d + 8, c32(P3, 8));
        d[12] = P3[12];
        return d + L3;
    }
    static inline uint8_t* suf(uint8_t* d)
    {
        st16(d, c16(SUF, 0));
        return d + LS;
    }
};

struct ArmCpyInline
{
    static inline uint8_t* p0(uint8_t* d)
    {
        std::memcpy(d, launder(opaque[0]), L0);
        return d + L0;
    }
    static inline uint8_t* p1(uint8_t* d)
    {
        std::memcpy(d, launder(opaque[1]), L1);
        return d + L1;
    }
    static inline uint8_t* p2(uint8_t* d)
    {
        std::memcpy(d, launder(opaque[2]), L2);
        return d + L2;
    }
    static inline uint8_t* p3(uint8_t* d)
    {
        std::memcpy(d, launder(opaque[3]), L3);
        return d + L3;
    }
    static inline uint8_t* suf(uint8_t* d)
    {
        std::memcpy(d, launder(opaque[4]), LS);
        return d + LS;
    }
};

struct ArmCpyCall
{
    static inline uint8_t* p0(uint8_t* d)
    {
        g_memcpy(d, launder(opaque[0]), L0);
        return d + L0;
    }
    static inline uint8_t* p1(uint8_t* d)
    {
        g_memcpy(d, launder(opaque[1]), L1);
        return d + L1;
    }
    static inline uint8_t* p2(uint8_t* d)
    {
        g_memcpy(d, launder(opaque[2]), L2);
        return d + L2;
    }
    static inline uint8_t* p3(uint8_t* d)
    {
        g_memcpy(d, launder(opaque[3]), L3);
        return d + L3;
    }
    static inline uint8_t* suf(uint8_t* d)
    {
        g_memcpy(d, launder(opaque[4]), LS);
        return d + LS;
    }
};

struct ArmCpyVisible
{
    static inline uint8_t* p0(uint8_t* d)
    {
        std::memcpy(d, P0, L0);
        return d + L0;
    }
    static inline uint8_t* p1(uint8_t* d)
    {
        std::memcpy(d, P1, L1);
        return d + L1;
    }
    static inline uint8_t* p2(uint8_t* d)
    {
        std::memcpy(d, P2, L2);
        return d + L2;
    }
    static inline uint8_t* p3(uint8_t* d)
    {
        std::memcpy(d, P3, L3);
        return d + L3;
    }
    static inline uint8_t* suf(uint8_t* d)
    {
        std::memcpy(d, SUF, LS);
        return d + LS;
    }
};

// ── workloads ────────────────────────────────────────────────────────────────
constexpr size_t BUF_SIZE = 128 * 1024; // engine operator buffer size
constexpr size_t ROW_MAX = 63 + 4 * 13 + 8; // worst-case row bytes + slack

// Variable-length "values": digit strings 4..13 B from a 1 MiB source ring (mimics the lazy
// numeric passthrough copies; identical across arms; lengths from a fixed xorshift table so
// every arm copies the same bytes to the same relative offsets).
constexpr size_t VAL_TABLE = 4096;
uint32_t val_off[VAL_TABLE];
uint8_t val_len[VAL_TABLE];
std::vector<uint8_t> val_src(1 << 20);

inline uint8_t* copy_value(uint8_t* d, size_t i)
{
    const uint8_t len = val_len[i & (VAL_TABLE - 1)];
    const uint8_t* s = val_src.data() + val_off[i & (VAL_TABLE - 1)];
    std::memcpy(d, s, 16); // fixed 16B over-copy, advance by len: the engine's small-copy idiom
    return d + len;
}

// One measured pass = `rows` rows into the ring.
// GLUE_ONLY: the four prefixes + suffix back-to-back (adjacent constants MAY merge -- that is
// the point of this workload: the maximum possible gap between the strategies).
// Interleaved: prefix,value,prefix,value,... exactly like the real formatter -- variable-length
// value copies between prefixes defeat cross-constant merging.
template <class A, bool GLUE_ONLY>
__attribute__((noinline)) uint64_t run_arm(uint8_t* buf, size_t rows)
{
    uint8_t* d = buf;
    uint8_t* const end = buf + BUF_SIZE - ROW_MAX;
    uint64_t bytes = 0;
    for (size_t r = 0; r < rows; ++r)
    {
        if (d >= end)
            d = buf;
        uint8_t* const start = d;
        if constexpr (GLUE_ONLY)
        {
            d = A::p0(d);
            d = A::p1(d);
            d = A::p2(d);
            d = A::p3(d);
            d = A::suf(d);
        }
        else
        {
            d = A::p0(d);
            d = copy_value(d, r);
            d = A::p1(d);
            d = copy_value(d, r + 1);
            d = A::p2(d);
            d = copy_value(d, r + 2);
            d = A::p3(d);
            d = copy_value(d, r + 3);
            d = A::suf(d);
        }
        bytes += static_cast<uint64_t>(d - start);
    }
    return bytes;
}

uint64_t checksum(const uint8_t* buf)
{
    uint64_t h = 1469598103934665603ULL;
    for (size_t i = 0; i < BUF_SIZE; i += 64)
        h = (h ^ buf[i]) * 1099511628211ULL;
    return h;
}

struct Arm
{
    const char* name;
    uint64_t (*glue)(uint8_t*, size_t);
    uint64_t (*inter)(uint8_t*, size_t);
};

}

int main()
{
    // init opaque heap copies + value table
    static char heap[5][32];
    std::memcpy(heap[0], P0, sizeof(P0));
    std::memcpy(heap[1], P1, sizeof(P1));
    std::memcpy(heap[2], P2, sizeof(P2));
    std::memcpy(heap[3], P3, sizeof(P3));
    std::memcpy(heap[4], SUF, sizeof(SUF));
    for (int i = 0; i < 5; ++i)
        opaque[i] = heap[i];
    uint64_t x = 88172645463325252ULL;
    for (auto& b : val_src)
    {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        b = '0' + (x % 10);
    }
    for (size_t i = 0; i < VAL_TABLE; ++i)
    {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        val_len[i] = 4 + (x % 10); // 4..13
        val_off[i] = x % (val_src.size() - 64);
    }

    alignas(64) static uint8_t buf[BUF_SIZE];

    const Arm arms[] = {
        {"imm", run_arm<ArmImm, true>, run_arm<ArmImm, false>},
        {"cpy_inline", run_arm<ArmCpyInline, true>, run_arm<ArmCpyInline, false>},
        {"cpy_call", run_arm<ArmCpyCall, true>, run_arm<ArmCpyCall, false>},
        {"cpy_visible", run_arm<ArmCpyVisible, true>, run_arm<ArmCpyVisible, false>},
    };
    constexpr int NARMS = 4, REPS = 7;
    constexpr size_t ROWS_GLUE = 40'000'000, ROWS_INTER = 25'000'000;

    double ns_glue[NARMS][REPS], ns_inter[NARMS][REPS];
    uint64_t bytes_glue[NARMS]{}, bytes_inter[NARMS]{};
    volatile uint64_t sink = 0;

    // warmup once per arm, then REPS alternating rounds (thermal fairness)
    for (const auto& a : arms)
    {
        sink += a.glue(buf, 1'000'000) + a.inter(buf, 1'000'000);
    }
    for (int rep = 0; rep < REPS; ++rep)
    {
        for (int ai = 0; ai < NARMS; ++ai)
        {
            auto t0 = std::chrono::steady_clock::now();
            bytes_glue[ai] = arms[ai].glue(buf, ROWS_GLUE);
            auto t1 = std::chrono::steady_clock::now();
            sink += checksum(buf);
            ns_glue[ai][rep] = std::chrono::duration<double, std::nano>(t1 - t0).count() / ROWS_GLUE;

            t0 = std::chrono::steady_clock::now();
            bytes_inter[ai] = arms[ai].inter(buf, ROWS_INTER);
            t1 = std::chrono::steady_clock::now();
            sink += checksum(buf);
            ns_inter[ai][rep] = std::chrono::duration<double, std::nano>(t1 - t0).count() / ROWS_INTER;
        }
    }

    auto median = [](double* v)
    {
        std::sort(v, v + REPS);
        return v[REPS / 2];
    };
    std::printf("%-12s %14s %14s %16s %16s\n", "arm", "glue ns/row", "glue GB/s", "interleaved ns/row", "interleaved GB/s");
    for (int ai = 0; ai < NARMS; ++ai)
    {
        const double ng = median(ns_glue[ai]), ni = median(ns_inter[ai]);
        std::printf(
            "%-12s %14.3f %14.2f %16.3f %16.2f\n",
            arms[ai].name,
            ng,
            (double)bytes_glue[ai] / ROWS_GLUE / ng,
            ni,
            (double)bytes_inter[ai] / ROWS_INTER / ni);
    }
    std::printf("(sink=%llu)\n", (unsigned long long)sink);
    return 0;
}
