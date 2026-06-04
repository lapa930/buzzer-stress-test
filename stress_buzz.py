"""
Buzzer API 스트레스 테스트

사용법:
  # nginx 경유 (외부, HTTPS)
  python3 stress_buzz.py --url https://bc.yonsei.or.kr --api-prefix /hidden

  # 앱 직접 (내부, HTTP)
  python3 stress_buzz.py --url http://127.0.0.1:8007 --api-prefix ""
"""
import asyncio, aiohttp, argparse, random, time, statistics, ssl
from collections import Counter

parser = argparse.ArgumentParser()
parser.add_argument("--url",        default="https://bc.yonsei.or.kr")
parser.add_argument("--api-prefix", default="/hidden", dest="api_prefix")
parser.add_argument("--users",      type=int, default=1000)
parser.add_argument("--conc",       type=int, default=100)
parser.add_argument("--vote",       default=None)
parser.add_argument("--admin",      default="hidden_admin")
parser.add_argument("--runner-id",  type=int, default=1)
args = parser.parse_args()

BASE   = args.url.rstrip("/")
PREFIX = args.api_prefix.rstrip("/")

EP_TOKEN  = f"{BASE}{PREFIX}/api/token"
EP_VOTES  = f"{BASE}{PREFIX}/api/votes"
EP_BUZZ   = f"{BASE}{PREFIX}/api/buzz"

def enable_ep(vid): return f"{EP_VOTES}/{vid}/enable"
def result_ep(vid): return f"{EP_VOTES}/{vid}/result"

def hdrs():
    return {"X-Admin-Password": args.admin, "Content-Type": "application/json"}

# SSL 검증 비활성화 (자체 서명 인증서 등 대응)
ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode    = ssl.CERT_NONE

latencies, statuses, errors = [], [], []
lock = asyncio.Lock()

# ── 연결 테스트 ───────────────────────────────────────────
async def connectivity_check(session):
    """실제 호출 전 API 연결 상태 확인"""
    print(f"  [Runner {args.runner_id}] 연결 테스트: POST {EP_TOKEN}")
    try:
        async with session.post(EP_TOKEN,
                                timeout=aiohttp.ClientTimeout(total=10),
                                ssl=ssl_ctx) as r:
            text = await r.text()
            print(f"  [Runner {args.runner_id}] 응답 {r.status}: {text[:120]}")
            if r.status == 200:
                return True
            else:
                print(f"  [Runner {args.runner_id}] ❌ 연결 실패 — HTTP {r.status}")
                return False
    except Exception as e:
        print(f"  [Runner {args.runner_id}] ❌ 연결 오류: {type(e).__name__}: {e}")
        return False

# ── 토큰 생성 ─────────────────────────────────────────────
async def gen_token(session, sem):
    async with sem:
        try:
            async with session.post(EP_TOKEN,
                                    timeout=aiohttp.ClientTimeout(total=20),
                                    ssl=ssl_ctx) as r:
                if r.status != 200:
                    async with lock:
                        errors.append(f"token HTTP {r.status}")
                    return None
                d = await r.json()
                return d.get("token_id")
        except asyncio.TimeoutError:
            async with lock:
                errors.append("token timeout")
            return None
        except Exception as e:
            async with lock:
                errors.append(f"token {type(e).__name__}")
            return None

# ── 버즈 전송 ─────────────────────────────────────────────
async def do_buzz(session, sem, token_id, phone):
    async with sem:
        t0 = time.perf_counter()
        try:
            async with session.post(EP_BUZZ,
                json={"token_id": token_id, "phone_last4": phone},
                timeout=aiohttp.ClientTimeout(total=30),
                ssl=ssl_ctx) as r:
                status = r.status
                await r.read()
        except asyncio.TimeoutError:
            status = -1
        except Exception as e:
            status = -2
            async with lock:
                errors.append(f"buzz {type(e).__name__}")
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
        headers=hdrs(), ssl=ssl_ctx) as r:
        v = await r.json()
        vid = v["id"]

    async with session.post(enable_ep(vid), headers=hdrs(), ssl=ssl_ctx) as r:
        await r.json()

    print(f"  [Runner {args.runner_id}] Vote 생성/활성화: {vid}")
    return vid

# ── Main ──────────────────────────────────────────────────
async def main():
    print(f"\n  [Runner {args.runner_id}] ============================")
    print(f"  URL      : {BASE}")
    print(f"  API 경로  : {PREFIX}/api/...")
    print(f"  사용자    : {args.users}명  동시: {args.conc}")

    conn = aiohttp.TCPConnector(
        limit=args.conc + 50,
        limit_per_host=args.conc + 50,
        ssl=ssl_ctx,
    )

    async with aiohttp.ClientSession(connector=conn) as session:

        # ── 연결 확인 ──
        ok = await connectivity_check(session)
        if not ok:
            print(f"  [Runner {args.runner_id}] 서버 연결 불가 — 종료")
            return

        vote_id = await ensure_vote(session)

        # ── 토큰 생성 ──
        print(f"  [Runner {args.runner_id}] 토큰 {args.users}개 생성 중...")
        sem = asyncio.Semaphore(args.conc)
        t0  = time.perf_counter()
        tokens = await asyncio.gather(*[gen_token(session, sem) for _ in range(args.users)])
        tokens = [t for t in tokens if t]
        print(f"  [Runner {args.runner_id}] 토큰 완료: {len(tokens)}/{args.users}개 ({time.perf_counter()-t0:.1f}s)")

        if not tokens:
            print(f"  [Runner {args.runner_id}] ❌ 토큰이 0개 — 버즈 불가")
            err_cnt = Counter(errors)
            for e, c in err_cnt.most_common(5):
                print(f"    에러: {e} × {c}건")
            return

        # ── 버즈 전송 ──
        phones = [f"{random.randint(1000,9999)}" for _ in tokens]
        print(f"  [Runner {args.runner_id}] 버즈 {len(tokens)}개 전송 중 (동시 {args.conc})...")
        t0 = time.perf_counter()
        await asyncio.gather(*[
            do_buzz(session, sem, tokens[i], phones[i])
            for i in range(len(tokens))
        ])
        elapsed = time.perf_counter() - t0

    # ── 결과 ──
    ok_cnt = statuses.count(200)
    err_cnt = len(statuses) - ok_cnt
    lat = sorted(latencies) if latencies else [0]
    p50 = statistics.median(lat)
    p95 = lat[int(len(lat) * 0.95)]
    p99 = lat[int(len(lat) * 0.99)]

    print(f"\n{'='*50}")
    print(f"  Runner #{args.runner_id} 결과")
    print(f"  성공: {ok_cnt:,}  실패: {err_cnt:,}  소요: {elapsed:.1f}s")
    print(f"  TPS : {ok_cnt/elapsed:.0f} req/s")
    print(f"  p50 : {p50:.0f}ms  p95: {p95:.0f}ms  p99: {p99:.0f}ms")
    if errors:
        ec = Counter(errors)
        print(f"  에러 상세:")
        for e, c in ec.most_common():
            print(f"    {e}: {c}건")
    print(f"{'='*50}")

asyncio.run(main())
