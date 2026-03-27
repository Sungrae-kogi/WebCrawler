# main.py
import csv
import random
import asyncio
import aiohttp
from pathlib import Path



# 파서 함수들도 비동기(async)로 바뀌어야 하므로
# 이름 앞에 await를 붙여서 호출할 예정입니다.
from parser import build_horse_url, parse_horse_page


# 기존 load_hrno_list_from_csv 함수는
# 통신이 아닌 단순 파일 읽기이므로 그대로 유지합니다.
def load_hrno_list_from_csv(
        csv_path: Path,
        col_name: str = "HRNO"
) -> list[str]:
    hrnos = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            v = (row.get(col_name) or "").strip()
            if v:
                hrnos.append(v)

    seen = set()
    uniq = []
    for x in hrnos:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


# -------------------------------
# 1. 단일 말 데이터 수집 코루틴 (세마포어 적용)
# -------------------------------
async def fetch_single_horse(
        hrno: str,
        idx: int,
        total: int,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore
) -> dict | None:
    # 톨게이트 진입: 동시에 최대 3개까지만 여기를 통과함
    async with sem:
        url = build_horse_url(hrno)

        try:
            # 사람인 척 불규칙하게 대기 (지터 적용)
            # 서버에 요청을 쏘기 직전에 쉬어줍니다.
            delay = random.uniform(1.0, 2.0)
            await asyncio.sleep(delay)

            # 파서 역시 비동기 함수로 변경해야 하므로 await 사용
            data = await parse_horse_page(
                url,
                hrno,
                session=session
            )

            print(f"[{idx}/{total}] OK HRNO={hrno}")
            return data

        except Exception as e:
            print(f"[{idx}/{total}] FAIL HRNO={hrno} / {e}")
            return None


# -------------------------------
# 2. 메인 비동기 실행 루프
# -------------------------------
async def run_async(
        hrno_list: list[str],
        max_visit: int | None = None
) -> list[dict]:
    if max_visit is not None:
        hrno_list = hrno_list[:max_visit]

    total = len(hrno_list)
    results = []

    # 동시 접속 3개로 제한하는 톨게이트 생성
    sem = asyncio.Semaphore(3)

    # aiohttp 세션 생성 (requests.Session을 대체함)
    async with aiohttp.ClientSession() as session:
        tasks = []

        for idx, hrno in enumerate(hrno_list, start=1):
            # 100마리의 작업 지시서를 일단 루프에 다 던져 넣음
            task = asyncio.create_task(
                fetch_single_horse(
                    hrno, idx, total, session, sem
                )
            )
            tasks.append(task)

        # 던져진 작업들이 세마포어 규칙(3개씩)에 맞춰 실행됨
        # gather는 모든 작업이 끝날 때까지 기다렸다가 결과를 모아줌   -> Journaling 관련 문제점 : 모든 작업이 끝날때까지 RAM메모리에 쥐고 있다가, 모든 작업이 종료되고나서 이후에 save_results_to_csv()가 실행이 된다, 즉, 중간에 문제가 발생하면 csv저장을 못하고 다 날아감
        gathered_results = await asyncio.gather(*tasks)

    # 에러가 나서 None이 반환된 경우를 제외하고 정상 데이터만 필터링
    return [r for r in gathered_results if r is not None]


# (save_results_to_csv 함수는 파일 저장 I/O이므로 기존 코드 그대로 사용)
def save_results_to_csv(results: list[dict], out_path: Path):
    if not results:
        print("[WARN] 저장할 데이터가 없습니다.")
        return
    fieldnames = list(results[0].keys())
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"[OK] CSV 저장 완료: {out_path}")


# -------------------------------
# 실행부 (Entry Point)
# -------------------------------
if __name__ == "__main__":
    import sys  # sys.maxsize를 사용하기 위해 필요합니다.

    base_dir = Path(__file__).resolve().parent
    hrno_csv = base_dir / "data2" / "HRNO_kyoto.csv"

    if not hrno_csv.exists():
        raise FileNotFoundError(f"CSV 없음: {hrno_csv}")

    hrnos = load_hrno_list_from_csv(hrno_csv, col_name="HRNO")
    print("HRNO 개수:", len(hrnos))

    # ==========================================
    # 1. 여기서 results 변수가 탄생합니다!
    # ==========================================
    results = asyncio.run(
        run_async(hrnos, max_visit=len(hrnos))
    )

    # ==========================================
    # 2. 순서 사전을 바탕으로 results 리스트 정렬
    # ==========================================
    order_map = {
        hrno: i for i, hrno in enumerate(hrnos)
    }

    # x.get("HR_NO")를 써서 파싱 실패 시에도 안전하게 넘깁니다.
    results.sort(
        key=lambda x: order_map.get(
            x.get("HR_NO"),
            sys.maxsize
        )
    )

    # ==========================================
    # 3. CSV 파일로 저장
    # ==========================================
    input_name = hrno_csv.stem
    output_name = f"{input_name}_result2.csv"

    out_csv = base_dir / "data2" / output_name

    save_results_to_csv(results, out_csv)