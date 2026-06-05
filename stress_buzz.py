"""
Buzzer API 스트레스 테스트 v4 — 동기화 시작 지원
"""
import asyncio, aiohttp, argparse, random, time, statistics
from collections import Counter

parser = argparse.ArgumentParser()
parser.add_argument("--url",        default="https://bc.yonsei.or.kr")
parser.add_argument("--api-prefix", default="/hidden", dest="api_prefix")
parser.add_argument("--users",      type=int,   default=2000)
parser.add_argument("--conc",       type=int,   default=100)
parser.add_argument("--vote",       default=None)
parser.add_argument("--admin",      default="hidden_admin")
parser.add_argument("--runner-id",  type=int,   default=1)
parser.add_argument("--start-at",   type=float, default=0,
                    help="Unix timestamp: 이 시각에 일제히 buzz 시작 (0=즉시)")
args = parser.parse_args()

BASE   = args.url.rstrip("/")
PREFIX = args.api_prefix.rstrip("/")
EP_TOKEN = f"{BASE}{PREFIX}/api/token"
EP_VOTES = f"{BASE}{PREFIX}/api/votes"
EP_BUZZ  = f"{BASE}{PREFIX}/api/buzz"

def hdrs(): return {"X-Admin-Password": args.admin, "Content-Type": "application/json"}

latencies, statuses, err_list = [], [], []
lock = asyncio.Lock()
R = args.runner_id
def log(msg): print(f"  [Runner {R:02d}] {msg}", flush=True)

async def check_connection(session) -> bool:
    log(f"연결 테스트 → {EP_TOKEN}")
    try:
        async with session.post(EP_TOKEN, timeout=aiohttp.ClientTimeout(total=10)) as r:
            text = await r.text()
            log(f"응답 {r.status}: {text[:80]}")
            return r.status == 200
    except Exception as e:
        log(f"❌ {type(e).__name__}: {e}")
        return False

async def gen_token(session, sem):
    async with sem:
        try:
            async with session.post(EP_TOKEN, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    async with lock: err_list.append(f"HTTP {r.status}")
                    return None
                d = await r.json(content_type=None)
                return d.get("token_id")
        except asyncio.TimeoutError:
            async with lock: err_list.append("timeout")
        except Exception as e:
            async with lock: err_list.append(f"{type(e).__name__}")
        return None

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
        except Exception as e:
            status = -2
            async with lock: err_list.append(f"buzz {type(e).__name__}")
        async with lock:
            latencies.append((time.perf_counter() - t0) * 1000)
            statuses.append(status)

async def ensure_vote(session):
    if args.vote:
        log(f"vote: {args.vote}")
        return args.vote
    async with session.post(EP_VOTES,
        json={"title": "StressTest", "count_max": 9999}, headers=hdrs()) as r:
        vid = (await r.json(content_type=None))["id"]
    async with session.post(f"{EP_VOTES}/{vid}/enable", headers=hdrs()) as r:
        await r.read()
    log(f"vote 생성: {vid}")
    return vid

async def main():
    log(f"v4 | {BASE}{PREFIX}/api/ | users={args.users} conc={args.conc}")

    conn = aiohttp.TCPConnector(
        limit=args.conc + 100,
        limit_per_host=args.conc + 100,
        ssl=False,
    )
    async with aiohttp.ClientSession(connector=conn) as session:
        if not await check_connection(session):
            log("서버 도달 불가 — 종료"); return

        vote_id = await ensure_vote(session)
        sem = asyncio.Semaphore(args.conc)

        # ── 토큰 생성 ──────────────────────────────────────
        log(f"토큰 {args.users}개 생성 중...")
        t0 = time.perf_counter()
        tokens = await asyncio.gather(
            *[gen_token(session, sem) for _ in range(args.users)]
        )
        tokens = [t for t in tokens if t]
        log(f"토큰 완료: {len(tokens)}/{args.users}개 ({time.perf_counter()-t0:.1f}s)")

        if not tokens:
            log("❌ 토큰 0개 — 원인:")
            for e, c in Counter(err_list).most_common(5):
                log(f"    {e} ×{c}")
            return

        # ── 동기화 대기 ────────────────────────────────────
        if args.start_at > 0:
            wait_sec = args.start_at - time.time()
            if wait_sec > 0:
                log(f"⏳ 동기화 대기 {wait_sec:.1f}s (모든 runner 준비 완료 후 일제히 시작)")
                await asyncio.sleep(wait_sec)
            else:
                log(f"⚠️  start_at 이미 지남 ({-wait_sec:.1f}s) — 즉시 시작")

        # ── 버즈 일제 시작 ─────────────────────────────────
        phones = [f"{random.randint(1000,9999)}" for _ in tokens]
        log(f"🚀 버즈 {len(tokens)}개 시작! (동시 {args.conc})")
        t0 = time.perf_counter()
        await asyncio.gather(
            *[do_buzz(session, sem, tokens[i], phones[i]) for i in range(len(tokens))]
        )
        elapsed = time.perf_counter() - t0

    if not latencies:
        log("결과 없음"); return

    ok  = statuses.count(200)
    lat = sorted(latencies)

    print(f"\n{'='*54}")
    print(f"  Runner #{R:02d}  성공:{ok:,}  실패:{len(statuses)-ok:,}  {elapsed:.1f}s  TPS:{ok/elapsed:.0f}")
    print(f"  p50:{statistics.median(lat):.0f}ms  "
          f"p95:{lat[int(len(lat)*.95)]:.0f}ms  "
          f"p99:{lat[int(len(lat)*.99)]:.0f}ms")
    if err_list:
        for e, c in Counter(err_list).most_common(3):
            print(f"  에러 {e}: {c}건")
    print(f"{'='*54}")

asyncio.run(main())
