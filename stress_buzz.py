"""
Buzzer API 스트레스 테스트

사용법:
  # nginx 경유 (외부)
  python3 stress_buzz.py --url https://bc.yonsei.or.kr --api-prefix /hidden

  # 앱 직접 (내부, nginx 우회)
  python3 stress_buzz.py --url http://127.0.0.1:8007 --api-prefix ""
"""
import asyncio, aiohttp, argparse, random, time, statistics
from collections import Counter

parser = argparse.ArgumentParser()
parser.add_argument("--url",        default="https://bc.yonsei.or.kr")
parser.add_argument("--api-prefix", default="/hidden",  # nginx 경유: /hidden, 직접: ""
                    dest="api_prefix")
parser.add_argument("--users",      type=int, default=1000)
parser.add_argument("--conc",       type=int, default=100)
parser.add_argument("--vote",       default=None)
parser.add_argument("--admin",      default="hidden_admin")
parser.add_argument("--runner-id",  type=int, default=1)
args = parser.parse_args()

BASE   = args.url.rstrip("/")
PREFIX = args.api_prefix.rstrip("/")   # 예: "/hidden" 또는 ""

# API 엔드포인트 (PREFIX 적용)
EP_TOKEN  = f"{BASE}{PREFIX}/api/token"
EP_VOTES  = f"{BASE}{PREFIX}/api/votes"
EP_BUZZ   = f"{BASE}{PREFIX}/api/buzz"

def vote_ep(vid):   return f"{EP_VOTES}/{vid}"
def enable_ep(vid): return f"{EP_VOTES}/{vid}/enable"
def result_ep(vid): return f"{EP_VOTES}/{vid}/result"

def hdrs():
    return {"X-Admin-Password": args.admin, "Content-Type": "application/json"}

latencies, statuses = [], []
lock = asyncio.Lock()

# ── 토큰 생성 ─────────────────────────────────────────────
async def gen_token(session, sem):
    async with sem:
        try:
            async with session.post(EP_TOKEN,
                                    timeout=aiohttp.ClientTimeout(total=20)) as r:
                d = await r.json()
                return d.get("token_id")
        except Exception as e:
            return None

# ── 버즈 전송 ─────────────────────────────────────────────
async def do_buzz(session, sem, token_id, phone):
    async with sem:
        t0 = time.perf_counter()
        try:
            async with session.post(EP_BUZZ,
                json={"token_id": token_id, "phone_last4": phone},
                timeout=aiohttp.ClientTimeout(total=30)) as r:
                status = r.status
                await r.read()
        except asyncio.TimeoutError:
            status = -1
        except Exception:
            status = -2
        lat = (time.perf_counter() - t0) * 1000
        async with lock:
            latencies.append(lat)
            statuses.append(status)

# ── Vote 준비 ──────────────────────────────────────────────
async def ensure_vote(session):
    if args.vote:
        print(f"  [Runner {args.runner_id}] 기존 vote 사용: {args.vote}")
        return args.vote

    async with session.post(EP_VOTES,
        json={"title": "GitHub-StressTest", "count_max": 99999},
        headers=hdrs()) as r:
        v = await r.json()
        vid = v["id"]

    async with session.post(enable_ep(vid), headers=hdrs()) as r:
        await r.json()

    print(f"  [Runner {args.runner_id}] Vote 생성/활성화: {vid}")
    return vid

# ── Main ──────────────────────────────────────────────────
async def main():
    print(f"\n  [Runner {args.runner_id}] 시작")
    print(f"  URL      : {BASE}")
    print(f"  API 경로  : {PREFIX}/api/...")
    print(f"  사용자    : {args.users}명  동시: {args.conc}")

    sem  = asyncio.Semaphore(args.conc)
    conn = aiohttp.TCPConnector(limit=args.conc + 50, limit_per_host=args.conc + 50)

    async with aiohttp.ClientSession(connector=conn) as session:
        vote_id = await ensure_vote(session)

        # Phase 1: 토큰 생성
        print(f"  [Runner {args.runner_id}] 토큰 {args.users}개 생성 중...")
        t0 = time.perf_counter()
        tokens = await asyncio.gather(*[gen_token(session, sem) for _ in range(args.users)])
        tokens = [t for t in tokens if t]
        tok_elapsed = time.perf_counter() - t0
        print(f"  [Runner {args.runner_id}] 토큰 완료: {len(tokens)}개 ({tok_elapsed:.1f}s)")

        # Phase 2: 버즈 전송
        phones = [f"{random.randint(1000, 9999)}" for _ in tokens]
        print(f"  [Runner {args.runner_id}] 버즈 {len(tokens)}개 전송 중 (동시 {args.conc})...")
        t0 = time.perf_counter()
        await asyncio.gather(*[
            do_buzz(session, sem, tokens[i], phones[i])
            for i in range(len(tokens))
        ])
        elapsed = time.perf_counter() - t0

    # 결과
    ok  = statuses.count(200)
    err = len(statuses) - ok
    lat = sorted(latencies)
    p50 = statistics.median(lat)
    p95 = lat[int(len(lat) * 0.95)]
    p99 = lat[int(len(lat) * 0.99)]

    print(f"\n{'='*50}")
    print(f"  Runner #{args.runner_id} 결과")
    print(f"  성공: {ok:,}  실패: {err:,}  소요: {elapsed:.1f}s")
    print(f"  TPS : {ok/elapsed:.0f} req/s")
    print(f"  p50 : {p50:.0f}ms  p95: {p95:.0f}ms  p99: {p99:.0f}ms")
    if err > 0:
        cnt = Counter(statuses)
        for s, c in sorted(cnt.items()):
            if s != 200:
                label = {-1: "timeout", -2: "conn_err"}.get(s, f"HTTP_{s}")
                print(f"  에러 {label}: {c}건")
    print(f"{'='*50}")

asyncio.run(main())
