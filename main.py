# main.py
import csv
import time
import random
from pathlib import Path
import requests
from parser import build_horse_url, parse_horse_page


def load_hrno_list_from_csv(csv_path: Path, col_name: str = "HRNO") -> list[str]:
    """data/unique_hrno.csv에서 HRNO 컬럼을 읽어 리스트로 반환 (중복 제거)"""
    hrnos = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if col_name not in reader.fieldnames:
            raise ValueError(f"CSV에 '{col_name}' 컬럼이 없습니다. 발견된 컬럼: {reader.fieldnames}")

        for row in reader:
            v = (row.get(col_name) or "").strip()
            if v:
                hrnos.append(v)

    # 중복 제거(순서 유지)
    seen = set()
    uniq = []
    for x in hrnos:
        if x in seen:
            continue
        seen.add(x)
        uniq.append(x)

    return uniq


def run(hrno_list: list[str], max_visit: int | None = None):
    """HRNO 리스트를 돌면서 말 페이지 파싱 결과를 출력(지금 단계에서는 저장 X)"""
    session = requests.Session()

    if max_visit is not None:
        hrno_list = hrno_list[:max_visit]

    results = []

    for idx, hrno in enumerate(hrno_list, start=1):
        url = build_horse_url(hrno)

        try:
            data = parse_horse_page(url, session=session)
            results.append(data)
            print(f"[{idx}/{len(hrno_list)}] OK HRNO={hrno} -> {data}")
        except Exception as e:
            print(f"[{idx}/{len(hrno_list)}] FAIL HRNO={hrno} / {e}")

        time.sleep(random.uniform(1.0, 2.0))

    return results

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://db.netkeiba.com/",
}

def save_results_to_csv(results: list[dict], out_path: Path):
    """파싱 결과(list[dict])를 CSV로 저장"""

    if not results:
        print("[WARN] 저장할 데이터가 없습니다.")
        return

    # 컬럼명 = dict key 기준
    fieldnames = list(results[0].keys())

    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(results)

    print(f"[OK] CSV 저장 완료: {out_path}")

"""
    HRNO 말 상세 정보 (출전이력 포함)
"""

if __name__ == "__main__":
    #✅ HRNOCrawler 폴더 기준으로 data/unique_hrno.csv 읽기
    base_dir = Path(__file__).resolve().parent
    hrno_csv = base_dir / "data2" / "HRNO_kyoto.csv"

    if not hrno_csv.exists():
        raise FileNotFoundError(f"HRNO CSV를 찾지 못했습니다: {hrno_csv}")

    hrnos = load_hrno_list_from_csv(hrno_csv, col_name="HRNO")
    print("HRNO 개수:", len(hrnos))

    results = run(hrnos, max_visit=len(hrnos))

    # ===============================
    # CSV 저장 경로 생성
    # ===============================
    input_name = hrno_csv.stem  # unique_hrno
    output_name = f"{input_name}_result.csv"

    out_csv = base_dir / "data2" / output_name

    save_results_to_csv(results, out_csv)
