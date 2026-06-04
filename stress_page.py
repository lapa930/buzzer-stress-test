"""
페이지 로딩 스트레스 테스트
사용법: python3 stress_page.py --url URL --total N --conc C
"""
import asyncio, aiohttp, argparse, time, statistics

parser = argparse.ArgumentParser()
parser.add_argument("--url",   default="https://bc.yonsei.or.kr")
parser.add_argument("--total", type=int, default=500)
parser.add_argument("--conc",  type=int, default=100)
parser.add_argument("--runner-id", type=int, default=1)
args = parser.parse_args()

URL = args.url.rstrip("/") + "/hidden/vote"
latencies, statuses = [], []
lock = asyncio.Lock()

async def fetch(session, sem):
    async with sem:
        t0 = time.perf_counter()
        try:
            async with session.get(URL,
                timeout=aiohttp.ClientTimeout(total=20),
                allow_redirects=True) as r:
                await r.read()
                status = r.status
        except asyncio.TimeoutError:
            status = -1
        except:
            status = -2
        async with lock:
            latencies.append((time.perf_counter()-t0)*1000)
            statuses.append(status)

async def main():
    sem  = asyncio.Semaphore(args.conc)
    conn = aiohttp.TCPConnector(limit=args.conc+50, limit_per_host=args.conc+50)
    print(f"  [Runner {args.runner_id}] 페이지 {args.total}회 로딩 (동시 {args.conc})...")
    t0 = time.perf_counter()
    async with aiohttp.ClientSession(connector=conn) as session:
        await asyncio.gather(*[fetch(session, sem) for _ in range(args.total)])
    elapsed = time.perf_counter() - t0
    ok  = statuses.count(200)
    lat = sorted(latencies)
    print(f"\n{'='*48}")
    print(f"  Runner #{args.runner_id} 페이지 로딩 결과")
    print(f"  성공: {ok:,}/{args.total:,}  소요: {elapsed:.1f}s  RPS: {ok/elapsed:.0f}")
    print(f"  p50: {statistics.median(lat):.0f}ms  p95: {lat[int(len(lat)*0.95)]:.0f}ms  p99: {lat[int(len(lat)*0.99)]:.0f}ms")
    print(f"{'='*48}")

asyncio.run(main())
