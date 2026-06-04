"""
Buzzer API 스트레스 테스트
사용법: python3 stress_buzz.py --url URL --users N --conc C --vote VOTE_ID --admin PW
"""
import asyncio, aiohttp, argparse, random, time, statistics
from collections import Counter

parser = argparse.ArgumentParser()
parser.add_argument("--url",   default="http://127.0.0.1:8007")
parser.add_argument("--users", type=int, default=1000)
parser.add_argument("--conc",  type=int, default=100)
parser.add_argument("--vote",  default=None)
parser.add_argument("--admin", default="hidden_admin")
parser.add_argument("--runner-id", type=int, default=1)
args = parser.parse_args()

BASE = args.url.rstrip("/")

def hdrs():
    return {"X-Admin-Password": args.admin, "Content-Type": "application/json"}

latencies, statuses = [], []
lock = asyncio.Lock()

async def gen_token(session, sem):
    async with sem:
        try:
            async with session.post(f"{BASE}/api/token",
                                    timeout=aiohttp.ClientTimeout(total=20)) as r:
                d = await r.json()
                return d.get("token_id")
        except:
            return None

async def do_buzz(session, sem, token_id, phone):
    async with sem:
        t0 = time.perf_counter()
        try:
            async with session.post(f"{BASE}/api/buzz",
                json={"token_id": token_id, "phone_last4": phone},
                timeout=aiohttp.ClientTimeout(total=30)) as r:
                status = r.status
                await r.read()
        except asyncio.TimeoutError:
            status = -1
        except:
            status = -2
        lat = (time.perf_counter() - t0) * 1000
        async with lock:
            latencies.append(lat)
            statuses.append(status)

async def ensure_vote(session):
    if args.vote:
        return args.vote
    async with session.post(f"{BASE}/api/votes",
        json={"title": f"StressTest", "count_max": 99999},
        headers=hdrs()) as r:
        v = await r.json()
        vid = v["id"]
    async with session.post(f"{BASE}/api/votes/{vid}/enable", headers=hdrs()) as r:
        await r.json()
    print(f"  [Runner {args.runner_id}] Vote created: {vid}")
    return vid

async def main():
    sem  = asyncio.Semaphore(args.conc)
    conn = aiohttp.TCPConnector(limit=args.conc + 50, limit_per_host=args.conc + 50)

    async with aiohttp.ClientSession(connector=conn) as session:
        vote_id = await ensure_vote(session)

        # 토큰 생성
        print(f"  [Runner {args.runner_id}] 토큰 {args.users}개 생성 중...")
        t0 = time.perf_counter()
        tokens = await asyncio.gather(*[gen_token(session, sem) for _ in range(args.users)])
        tokens = [t for t in tokens if t]
        print(f"  [Runner {args.runner_id}] 토큰 완료: {len(tokens)}개 ({time.perf_counter()-t0:.1f}s)")

        # 버즈 전송
        phones = [f"{random.randint(1000,9999)}" for _ in tokens]
        print(f"  [Runner {args.runner_id}] 버즈 {len(tokens)}개 전송 중 (동시 {args.conc})...")
        t0 = time.perf_counter()
        await asyncio.gather(*[
            do_buzz(session, sem, tokens[i], phones[i]) for i in range(len(tokens))
        ])
        elapsed = time.perf_counter() - t0

    # 결과
    ok  = statuses.count(200)
    err = len(statuses) - ok
    lat = sorted(latencies)
    p50 = statistics.median(lat)
    p95 = lat[int(len(lat)*0.95)]
    p99 = lat[int(len(lat)*0.99)]

    print(f"\n{'='*48}")
    print(f"  Runner #{args.runner_id} 결과")
    print(f"  성공: {ok:,}  실패: {err:,}  소요: {elapsed:.1f}s")
    print(f"  TPS: {ok/elapsed:.0f}  p50: {p50:.0f}ms  p95: {p95:.0f}ms  p99: {p99:.0f}ms")
    if err > 0:
        cnt = Counter(statuses)
        for s, c in cnt.items():
            if s != 200:
                print(f"  에러 HTTP {s}: {c}건")
    print(f"{'='*48}")

asyncio.run(main())
