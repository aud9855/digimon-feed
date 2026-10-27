"""공식 디지몬 도감(digimon.net/reference_ko) 기본 정보 수집

저장하는 것: 공식 목록 순서 기반 번호, 디렉터리명, 공식 한국어 이름, 영문 표기, 등급, 유형, 속성,
            X항체 여부, 관련된 디지몬(디렉터리명), 공식 상세 페이지 주소
저장하지 않는 것: 프로필 문장, 필살기, 그림 (공식 사이트 이용 조건 — 화면에서는 공식 페이지로 연결)

번호 규칙: 오래된 등록이 1번(공식 목록은 최신순이라 역순으로 매김). data/official/numbers.json 에 한 번 매긴 번호는 고정하고,
새로 생긴 디지몬은 등록 순서대로 뒤에 이어 붙임.

아프몬 도감(digimon.net/appmon_ko)도 같은 원칙으로 수집: 이름·등급·앱 이름·속성·관련 아프몬·공식 주소 → data/official/appmon.json

안전장치 (일시적 오류로 좋은 데이터를 덮어쓰지 않도록)
  - 상세 페이지 요청·해석이 실패하면 이전 수집값을 그대로 유지
  - 해석 실패가 너무 많으면(사이트 개편 의심) 저장하지 않고 중단
  - 목록이 이전보다 크게 줄면 저장하지 않고 중단

사용: python scraper/collect_official.py             # 디지몬 전체 수집 (상세 페이지 포함, 약 30분) + 아프몬
      python scraper/collect_official.py --list-only  # 디지몬 목록만 + 아프몬
      python scraper/collect_official.py --appmon-only
"""
import html
import os
import re
import sys
import time

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from jsonio import load_json, write_json  # noqa: E402

BASE = "https://digimon.net/reference_ko/"
OUT_DIR = "data/official"
NUMBERS_FILE = os.path.join(OUT_DIR, "numbers.json")
REFERENCE_FILE = os.path.join(OUT_DIR, "reference.json")
DELAY = 0.6  # 공식 서버 부담을 줄이기 위한 요청 간격(초)
MIN_KEEP_RATIO = 0.9  # 목록이 이전의 90% 미만이면 일시 오류로 보고 저장하지 않음
MAX_PARSE_FAIL_RATIO = 0.1  # 받아 온 상세 페이지 중 해석 실패가 10%를 넘으면 사이트 개편 의심 → 중단
MAX_FETCH_FAIL_RATIO = 0.3  # 상세 요청 실패가 30%를 넘으면 서버 문제로 보고 중단 (실패분은 이전 값 유지)
DETAIL_KEYS = ("name_en", "name_ko", "level", "type", "attribute", "related")

S = requests.Session()
S.headers.update({
    "User-Agent": "Mozilla/5.0 (digimon-feed non-commercial fan project)",
    "Referer": BASE,
    "X-Requested-With": "XMLHttpRequest",
})


class Maintenance(Exception):
    """공식 사이트 점검 중 (모든 요청이 /maint/ 안내 페이지로 넘어감)"""


def get(url, **kw):
    for attempt in range(4):
        try:
            r = S.get(url, timeout=30, **kw)
            if "/maint/" in r.url:
                raise Maintenance(r.url)
            if r.status_code == 200:
                return r
            print(f"  HTTP {r.status_code}: {r.url}")
        except Maintenance:
            raise
        except Exception as e:
            print(f"  error {e}: {url}")
        if attempt < 3:
            time.sleep(2 + attempt * 3)
    return None


def fetch_list():
    """공식 목록 순서 그대로 (request.php 페이지 단위)"""
    get(BASE)  # 세션 쿠키
    rows, offset, seen = [], 0, set()
    for _ in range(1000):  # 무한 반복 방지
        r = get(BASE + "request.php", params={
            "digimon_name": "", "digimon_level": "", "attribute": "", "type": "",
            "next": offset, "view_more": 1 if offset else "",
        })
        if r is None:
            raise SystemExit("목록 요청 실패")
        try:
            data = r.json()
        except ValueError:
            raise SystemExit(f"목록 응답이 JSON이 아닙니다: {r.text[:200]}")
        if not data:
            break
        if not isinstance(data, dict):
            raise SystemExit(f"목록 응답 형식이 예상과 다릅니다: {r.text[:200]}")
        for row in data.get("rows") or []:
            d = row.get("directory_name")
            if d and d not in seen:
                seen.add(d)
                rows.append(row)
        nxt = data.get("next", -1)
        print(f"  목록 {len(rows)}건 (next={nxt})")
        if nxt in (-1, None, "-1"):
            break
        try:
            nxt = int(nxt)
        except (TypeError, ValueError):
            raise SystemExit(f"목록 next 값이 이상합니다: {nxt!r}")
        if nxt == offset:
            break
        offset = nxt
        time.sleep(DELAY)
    return rows


def text(s):
    return html.unescape(re.sub(r"<[^>]+>", "", s or "")).strip()


def parse_detail(page):
    out = {}
    m = re.search(r'c-titleSet__sub">(.*?)</div>\s*<h3[^>]*>(.*?)</h3>', page, re.S)
    if m:
        if text(m.group(1)):
            out["name_en"] = text(m.group(1))
        if text(m.group(2)):
            out["name_ko"] = text(m.group(2))
    info = {text(k): text(v) for k, v in re.findall(r"<dt[^>]*>(.*?)</dt>\s*<dd>(.*?)</dd>", page, re.S)}
    for key, field in (("등급", "level"), ("유형", "type"), ("속성", "attribute")):
        if info.get(key):
            out[field] = info[key]
    rel = page.split('class="p-ref__related"', 1)
    if len(rel) == 2:
        out["related"] = re.findall(r'detail\.php\?directory_name=([^"&]+)"', rel[1].split("</section>", 1)[0])
    return out


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    print("공식 목록 수집…")
    rows = fetch_list()
    print(f"목록 {len(rows)}건")

    old = {e["directory_name"]: e for e in load_json(REFERENCE_FILE, {"digimons": []})["digimons"]}
    if not rows or (old and len(rows) < len(old) * MIN_KEEP_RATIO):
        raise SystemExit(f"목록이 비었거나 크게 줄었습니다 (이전 {len(old)} → 지금 {len(rows)}) — 저장하지 않고 중단")

    # 공식 목록은 최신 등록순 → 뒤에서부터(오래된 것부터) 번호를 매겨 새 디지몬이 항상 뒤에 붙도록 함
    numbers = load_json(NUMBERS_FILE, {})
    next_no = max(numbers.values(), default=0) + 1
    for row in reversed(rows):
        d = row["directory_name"]
        if d not in numbers:
            numbers[d] = next_no
            next_no += 1

    entries = []
    list_only = "--list-only" in sys.argv
    fetched = fetch_fail = parse_fail = 0
    for i, row in enumerate(rows):
        d = row["directory_name"]
        prev = old.get(d, {})
        e = {
            "no": numbers[d],
            "directory_name": d,
            "name_ko": row.get("name") or prev.get("name_ko"),
            "level": row.get("level") or prev.get("level"),
            "x_antibody": row.get("relate_word6") == "〇",
            "url": f"{BASE}detail.php?directory_name={d}",
        }
        if row.get("level_2"):
            e["level_2"] = row["level_2"]
        detail = None
        if not list_only:
            r = get(BASE + "detail.php", params={"directory_name": d})
            time.sleep(DELAY)
            if r is None:
                fetch_fail += 1
            else:
                fetched += 1
                parsed = parse_detail(r.text)
                if parsed.get("name_en"):  # 영문명이 잡혀야 정상 해석으로 봄 (Digi-API 연결의 기준)
                    detail = parsed
                else:
                    parse_fail += 1
                    print(f"  상세 해석 실패 — 이전 값 유지: {d}")
            if (i + 1) % 100 == 0:
                print(f"  상세 {i + 1}/{len(rows)} (요청 실패 {fetch_fail}, 해석 실패 {parse_fail})")
        if detail is not None:
            e.update(detail)
        else:
            # 목록만 수집했거나 상세 실패 → 이전 상세값 유지 (상세의 등급이 "성숙기(크로스워즈)"처럼 더 자세하므로 이전 값 우선)
            e.update({k: prev[k] for k in DETAIL_KEYS if prev.get(k)})
        if not e.get("name_ko"):
            e["name_ko"] = e.get("name_en") or d  # 이름이 None 으로 저장되지 않도록
        entries.append(e)

    if not list_only:
        if fetched and parse_fail > max(20, fetched * MAX_PARSE_FAIL_RATIO):
            raise SystemExit(f"상세 해석 실패 {parse_fail}/{fetched} — 사이트 구조 변경 의심, 저장하지 않고 중단")
        if fetch_fail > len(rows) * MAX_FETCH_FAIL_RATIO:
            raise SystemExit(f"상세 요청 실패 {fetch_fail}/{len(rows)} — 서버 문제 의심, 저장하지 않고 중단")

    entries.sort(key=lambda x: x["no"])
    write_json(NUMBERS_FILE, dict(sorted(numbers.items(), key=lambda kv: kv[1])), indent=1)
    write_json(REFERENCE_FILE, {
        "source": BASE,
        "note": "기본 정보만 저장 (프로필·필살기·그림은 공식 페이지 참고)",
        "total": len(entries),
        "digimons": entries,
    }, indent=1)
    print(f"저장: {REFERENCE_FILE} ({len(entries)}종), {NUMBERS_FILE} / 상세 요청 실패 {fetch_fail}, 해석 실패 {parse_fail} (이전 값 유지)")


APPMON_BASE = "https://digimon.net/appmon_ko/"
APPMON_FILE = os.path.join(OUT_DIR, "appmon.json")


def collect_appmon():
    """아프몬은 부가 데이터라 실패해도 디지몬 수집 결과에 영향을 주지 않도록 저장만 건너뜀 (기존 파일 유지)"""
    print("아프몬 도감 수집…")
    old = {a["id"]: a for a in load_json(APPMON_FILE, {"appmons": []}).get("appmons", [])}
    r = get(APPMON_BASE)
    if r is None:
        print("아프몬 목록 요청 실패 — 건너뜀 (기존 파일 유지)")
        return
    ids = []
    for m in re.finditer(r'href="\./detail\.php\?id=(\d+)"', r.text):
        if int(m.group(1)) not in ids:
            ids.append(int(m.group(1)))
    if not ids or (old and len(ids) < len(old) * MIN_KEEP_RATIO):
        print(f"아프몬 목록이 비었거나 크게 줄었음 (이전 {len(old)} → 지금 {len(ids)}) — 저장하지 않음 (기존 파일 유지)")
        return
    entries, failed = [], 0
    for i in ids:
        d = get(APPMON_BASE + "detail.php", params={"id": i})
        time.sleep(DELAY)
        page = d.text if d is not None else ""
        name = re.search(r'c-titleSet__main[^>]*>(.*?)</h3>', page, re.S)
        if not name or not text(name.group(1)):
            failed += 1
            if i in old:
                entries.append(old[i])  # 실패 → 이전 값 유지
            continue
        info = {text(k): text(v) for k, v in re.findall(r"<dt[^>]*>(.*?)</dt>\s*<dd>(.*?)</dd>", page, re.S)}
        rel = page.split('class="p-ref__related"', 1)
        related = [int(x) for x in re.findall(r'detail\.php\?id=(\d+)"', rel[1])] if len(rel) == 2 else []
        entries.append({
            "id": i,
            "name_ko": text(name.group(1)),
            "grade": info.get("등급", ""),
            "app": info.get("앱 이름", ""),
            "attribute": info.get("속성", ""),
            "related": [x for x in dict.fromkeys(related) if x != i],
            "url": f"{APPMON_BASE}detail.php?id={i}",
        })
    if failed > len(ids) * MAX_FETCH_FAIL_RATIO:
        print(f"아프몬 상세 실패 {failed}/{len(ids)} — 저장하지 않음 (기존 파일 유지)")
        return
    write_json(APPMON_FILE, {"source": APPMON_BASE, "note": "기본 정보만 저장 (프로필·필살기·그림은 공식 페이지 참고)",
                             "total": len(entries), "appmons": entries}, indent=1)
    print(f"저장: {APPMON_FILE} ({len(entries)}종, 실패 {failed}종은 이전 값 유지)")


if __name__ == "__main__":
    try:
        if "--appmon-only" not in sys.argv:
            main()
        collect_appmon()
    except Maintenance as e:
        # 일시적인 사이트 점검은 실패로 보지 않음 — 아무것도 저장하지 않고 다음 실행 때 다시 수집
        print(f"::warning::공식 사이트 점검 중이라 수집을 건너뜁니다 ({e}) — 기존 데이터 유지")
        sys.exit(0)
