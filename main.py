# main.py
import csv
import random
import asyncio
import aiohttp
from pathlib import Path



# 파서 함수들도 비동기(async)로 바뀌어야 하므로
# 이름 앞에 await를 붙여서 호출할 예정입니다.
from parser import build_horse_url, parse_horse_page


def get_completed_hrnos(out_path: Path) -> set[str]:
    # 파일이 없으면 빈 세트(Set) 반환 (처음 실행하는 경우)
    if not out_path.exists():
        return set()

    completed = set()
    with open(out_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            val = row.get("HR_NO")
            if val:
                completed.add(val.strip())
    return completed

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
        sem: asyncio.Semaphore,
        lock: asyncio.Lock,
        out_path: Path
) -> None:
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

            #기존에는 서버에서 받아온 horse data를 반환.
            # print(f"[{idx}/{total}] OK HRNO={hrno}")
            # return data

            # 여러 코루틴이 동시에 파일접근하는 것을 방지할 lock 시스템
            async with lock:
                file_exists = out_path.exists()
                with open(out_path, "a", encoding="utf-8-sig", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=list(data.keys()))
                    # 파일이 처음 만들어질때만 헤더 작성
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow(data)
            print(f"[{idx}/{total}] OK HRNO={hrno}")

        except Exception as e:
            print(f"[{idx}/{total}] FAIL HRNO={hrno} / {e}")



# -------------------------------
# 2. 메인 비동기 실행 루프
# -------------------------------
async def run_async(
        hrno_list: list[str],
        out_path: Path  # 결과를 저장할 경로를 전달받음
) -> None:
    total = len(hrno_list)
    sem = asyncio.Semaphore(3)
    lock = asyncio.Lock()  # 파일 I/O 자물쇠 생성

    async with aiohttp.ClientSession() as session:
        tasks = []
        for idx, hrno in enumerate(hrno_list, start=1):
            task = asyncio.create_task(
                fetch_single_horse(
                    hrno, idx, total, session, sem, lock, out_path
                )
            )
            tasks.append(task)

        # 던져진 모든 작업이 끝날 때까지 대기
        await asyncio.gather(*tasks)


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
    base_dir = Path(__file__).resolve().parent
    hrno_csv = base_dir / "data2" / "HRNO_kyoto.csv"

    input_name = hrno_csv.stem
    out_csv = base_dir / "data2" / f"{input_name}_result2.csv"

    if not hrno_csv.exists():
        raise FileNotFoundError(f"CSV 없음: {hrno_csv}")

    # 1. 전체 크롤링 대상 명단 가져오기
    all_hrnos = load_hrno_list_from_csv(hrno_csv, col_name="HRNO")

    # 2. 이미 수집 완료된 명단(저널) 확인하기
    completed_set = get_completed_hrnos(out_csv)

    # 3. 전체 명단에서 완료된 명단을 빼서 '남은 작업'만 추려내기
    target_hrnos = [h for h in all_hrnos if h not in completed_set]

    print(f"전체 명단: {len(all_hrnos)} 건")
    print(f"이미 완료: {len(completed_set)} 건")
    print(f"진행 대상: {len(target_hrnos)} 건")

    if not target_hrnos:
        print("🎉 모든 크롤링이 이미 완료되었습니다!")
    else:
        # 4. 남은 작업에 대해서만 비동기 크롤링 실행
        asyncio.run(run_async(target_hrnos, out_csv))
        print(f"🎉 크롤링 종료! 결과 파일: {out_csv}")