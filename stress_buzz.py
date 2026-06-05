"""
Buzzer API 스트레스 테스트 v5 — 단계별 타이밍 상세 출력
"""
import asyncio, aiohttp, argparse, random, time, statistics
from collections import Counter

parser = argparse.ArgumentParser()
parser.add_argument("--url",        default="https://bc.yonsei.or.kr")
parser.add_argument("--api-prefix", default="/hidden", dest="api_prefix")
parser.add_argument("--users",      type=int,   default=1500)
parser.add_argument("--conc",       type=int,   default=300)
parser.add_argument("--vote",       default=None)
parser.add_argument("--admin",      default="hidden_admin")
parser.add_argument("--runner-id",  type=int,   default=1)
parser.add_argument("--start-at",   type=float, default=0)
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
def log(msg): print(f"  [R{R:02d}] {msg}", flush=True)

async def check_connection(session) -> bool:
    try:
        async with session.post(EP_TOKEN, timeout=aiohttp.ClientTimeout(total=10)) as r:
            text = await r.text()
            log(f"연결확인 → {r.status}: {text[:60]}")
            return r.status == 200
    except Exception as e:
        log(f"❌ 연결실패: {type(e).__name__}: {e}")
        return False

async def gen_token(session, sem):
    async with sem:
        try:
            async with session.post(EP_TOKEN, timeout=aiohttp.ClientTimeout(total=20)) as r:
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
    T_SCRIPT_START = time.perf_counter()   # 스크립트 시작 시각
    log(f"v5 | users={args.users} conc={args.conc} | {BASE}{PREFIX}/api/")

    conn = aiohttp.TCPConnector(
        limit=args.conc + 100, limit_per_host=args.conc + 100, ssl=False
    )

    async with aiohttp.ClientSession(connector=conn) as session:

        # ── 연결 확인 ──────────────────────────────────────
        if not await check_connection(session):
            log("서버 도달 불가 — 종료"); return

        vote_id = await ensure_vote(session)
        sem = asyncio.Semaphore(args.conc)

        # ── Phase 1: 토큰 생성 ─────────────────────────────
        log(f"[Phase1] 토큰 {args.users}개 생성 시작")
        T_TOK_START = time.perf_counter()

        tokens = await asyncio.gather(
            *[gen_token(session, sem) for _ in range(args.users)]
        )
        tokens = [t for t in tokens if t]

        T_TOK_END  = time.perf_counter()
        tok_sec    = T_TOK_END - T_TOK_START
        tok_rps    = len(tokens) / tok_sec if tok_sec > 0 else 0

        log(f"[Phase1] 완료: {len(tokens)}/{args.users}개  "
            f"{tok_sec:.2f}s  ({tok_rps:.0f} tok/s)")

        if not tokens:
            log("❌ 토큰 0개 — 원인:")
            for e, c in Counter(err_list).most_common(5):
                log(f"    {e} ×{c}")
            return

        # ── Phase 2: 동기화 대기 ───────────────────────────
        T_WAIT_START = time.perf_counter()
        wait_sec = 0.0
        if args.start_at > 0:
            wait_sec = args.start_at - time.time()
            if wait_sec > 0:
                log(f"[Phase2] 동기화 대기 {wait_sec:.1f}s ...")
                await asyncio.sleep(wait_sec)
            else:
                log(f"[Phase2] start_at 이미 지남 ({-wait_sec:.1f}s) — 즉시")
            wait_sec = max(0.0, wait_sec)
        else:
            log("[Phase2] 동기화 없음 — 즉시 시작")

        # ── Phase 3: 버즈 ──────────────────────────────────
        phones = [f"{random.randint(1000,9999)}" for _ in tokens]
        log(f"[Phase3] 🚀 버즈 {len(tokens)}개 시작 (동시 {args.conc})")

        T_BUZZ_START = time.perf_counter()
        await asyncio.gather(
            *[do_buzz(session, sem, tokens[i], phones[i]) for i in range(len(tokens))]
        )
        T_BUZZ_END = time.perf_counter()
        buzz_sec   = T_BUZZ_END - T_BUZZ_START

        log(f"[Phase3] 버즈 완료: {buzz_sec:.2f}s")

    # ── 결과 ───────────────────────────────────────────────
    if not latencies:
        log("결과 없음"); return

    T_TOTAL = time.perf_counter() - T_SCRIPT_START

    ok      = statuses.count(200)
    fail    = len(statuses) - ok
    lat     = sorted(latencies)
    n       = len(lat)
    p50     = statistics.median(lat)
    p75     = lat[int(n * 0.75)]
    p90     = lat[int(n * 0.90)]
    p95     = lat[int(n * 0.95)]
    p99     = lat[int(n * 0.99)]
    lat_min = lat[0]
    lat_max = lat[-1]
    lat_avg = sum(lat) / n

    print(f"\n{'='*56}")
    print(f"  Runner #{R:02d}  단계별 타이밍")
    print(f"  {'─'*50}")
    print(f"  [Phase1] 토큰 생성   {tok_sec:7.2f}s  ({tok_rps:.0f} tok/s)")
    if args.start_at > 0:
        print(f"  [Phase2] 동기화 대기 {wait_sec:7.2f}s")
    print(f"  [Phase3] 버즈 전송   {buzz_sec:7.2f}s  "
          f"TPS {ok/buzz_sec:.0f} req/s")
    print(f"  {'─'*50}")
    print(f"  총 소요 (스크립트)   {T_TOTAL:7.2f}s")
    print(f"\n  결과: 성공 {ok:,}  실패 {fail:,}  "
          f"({ok/len(statuses)*100:.1f}%)")
    print(f"\n  응답시간 분포 (ms)")
    print(f"  min  {lat_min:7.1f}   avg  {lat_avg:7.1f}")
    print(f"  p50  {p50:7.1f}   p75  {p75:7.1f}")
    print(f"  p90  {p90:7.1f}   p95  {p95:7.1f}")
    print(f"  p99  {p99:7.1f}   max  {lat_max:7.1f}")

    if err_list:
        print(f"\n  에러 상세:")
        for e, c in Counter(err_list).most_common():
            print(f"    {e}: {c}건")
    print(f"{'='*56}")

asyncio.run(main())
