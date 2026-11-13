import os
import sys
import json
import time
import hashlib
import re
import html
import tempfile
import email.utils
import threading
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import feedparser

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
FEEDS_FILE = os.path.join(DATA_DIR, "digimon_feeds.json")
DOCS_DATA_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docs", "digimon_feeds.json"))
PUBLIC_DATA_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "public", "digimon_feeds.json"))

# 한국 사이트(루리웹/디시)와 공식 사이트(일본, JST=KST)의 시간대 정보 없는 날짜는 KST 로 해석·표시
# (GitHub Actions 러너는 UTC 라서 naive datetime 을 쓰면 9시간 어긋남)
KST = timezone(timedelta(hours=9), "KST")
FEED_TIMEOUT = 15     # RSS 요청 타임아웃(초) — feedparser.parse(url) 은 타임아웃이 없어 멈출 수 있음
RETENTION_DAYS = 90
# 동시에 수집하는 소스 수 — 구글 뉴스 경유 소스가 많아 너무 높이면 429(요청 과다)가 날 수 있음
CRAWL_WORKERS = max(1, int(os.environ.get("CRAWL_WORKERS") or 6))


# ---------------------------------------------------------------------------
# HTTP · 오류 기록
# ---------------------------------------------------------------------------

_tls = threading.local()   # 스레드별 HTTP 세션 · 지금 수집 중인 소스 이름
_errors_lock = threading.Lock()
SOURCE_ERRORS = {}         # 소스 이름 → 오류 메시지 목록 (수집 상태 보고용)


def _session() -> requests.Session:
    """스레드별 세션 (연결 재사용 + 일시 오류 자동 재시도: 429·5xx·연결 끊김, 1s→2s 대기)"""
    s = getattr(_tls, "session", None)
    if s is None:
        retry = Retry(total=2, connect=2, read=1, backoff_factor=1,
                      status_forcelist=(429, 500, 502, 503, 504),
                      allowed_methods=frozenset(["GET"]), raise_on_status=False)
        s = requests.Session()
        adapter = HTTPAdapter(max_retries=retry)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        _tls.session = s
    return s


def http_get(url: str, headers=None, timeout=10):
    """재시도 붙은 GET. 최종 응답의 상태 코드 검사는 호출하는 쪽에서"""
    return _session().get(url, headers=headers or HEADERS, timeout=timeout)


def log_error(msg: str):
    """[Error] 출력 + 지금 수집 중인 소스의 오류로 기록"""
    print(f"[Error] {msg}")
    name = getattr(_tls, "source", None)
    if name:
        with _errors_lock:
            SOURCE_ERRORS.setdefault(name, []).append(msg[:300])


# ---------------------------------------------------------------------------
# 공통 도우미
# ---------------------------------------------------------------------------

def _kst_ts(y: int, m: int, d: int):
    """KST 자정 timestamp, 잘못된 날짜(13월 등)면 None"""
    try:
        return int(datetime(y, m, d, tzinfo=KST).timestamp())
    except (ValueError, OverflowError):
        return None


def parse_date(date_str):
    """다양한 날짜 형식을 Unix timestamp(초)로 정규화. 읽을 수 없으면 None"""
    if not date_str:
        return None
    date_str = str(date_str).strip()
    if not date_str:
        return None
    now = datetime.now(KST)

    # 1. RFC 2822 (RSS 포맷: Sun, 11 Oct 2026 16:54:48 GMT 등)
    try:
        dt = email.utils.parsedate_to_datetime(date_str)
        if dt.tzinfo is None:  # '-0000' 등 시간대 미상 → UTC
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        pass

    # 2. ISO 포맷 (2026-10-14T14:27:06Z, 2026-10-14 등) — 시간대가 없으면 KST
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return int(dt.timestamp())
    except Exception:
        pass

    # 3. 'HH:MM' (당일 등록된 글 - 루리웹/디시, KST)
    m = re.fullmatch(r"(\d{1,2}):(\d{2})(?::\d{2})?", date_str)
    if m:
        try:
            dt = now.replace(hour=int(m.group(1)), minute=int(m.group(2)), second=0, microsecond=0)
        except ValueError:
            return None
        if dt > now + timedelta(minutes=10):  # 자정 직후 수집 등으로 미래가 되면 전날 글
            dt -= timedelta(days=1)
        return int(dt.timestamp())

    # 4. 'YYYY.MM.DD' / 'YYYY-MM-DD' / 'YYYY/MM/DD'
    m = re.search(r"(?<!\d)(\d{4})[./-](\d{1,2})[./-](\d{1,2})(?!\d)", date_str)
    if m:
        return _kst_ts(int(m.group(1)), int(m.group(2)), int(m.group(3)))

    # 5. 'YY.MM.DD' (모바일 웹 축약형)
    m = re.search(r"(?<!\d)(\d{2})[./-](\d{1,2})[./-](\d{1,2})(?!\d)", date_str)
    if m:
        return _kst_ts(2000 + int(m.group(1)), int(m.group(2)), int(m.group(3)))

    # 6. 'MM.DD' (연도 생략 — 디시 모바일 등). 미래 날짜가 되면 작년 글
    m = re.fullmatch(r"(\d{1,2})[./-](\d{1,2})", date_str)
    if m:
        mo, d = int(m.group(1)), int(m.group(2))
        ts = _kst_ts(now.year, mo, d)
        if ts is not None and ts > now.timestamp() + 86400:
            ts = _kst_ts(now.year - 1, mo, d)
        return ts

    return None


def parse_to_timestamp(date_str) -> int:
    """parse_date 와 같지만 읽을 수 없으면 지금 시각"""
    ts = parse_date(date_str)
    return ts if ts is not None else int(time.time())


def format_display_date(timestamp: int) -> str:
    """화면 표시용 YYYY-MM-DD HH:MM 포맷 (KST)"""
    return datetime.fromtimestamp(timestamp, KST).strftime("%Y-%m-%d %H:%M")


def date_fields(raw) -> dict:
    """item 의 timestamp/created_at.
    날짜를 읽을 수 없으면 지금 시각을 넣되 keep_first_seen 표시 → 병합 때 처음 본 시각을 유지 (매시간 맨 위로 갱신되지 않도록)"""
    ts = parse_date(raw)
    if ts is None:
        ts = int(time.time())
        return {"timestamp": ts, "created_at": format_display_date(ts), "keep_first_seen": True}
    return {"timestamp": ts, "created_at": format_display_date(ts)}


def clean_text(text) -> str:
    """HTML 태그 제거 + 엔티티(&nbsp; &amp; …) 복원 + 공백 정리 (전각 공백은 그대로 둠)"""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", str(text))
    text = html.unescape(text).replace("\xa0", " ")
    return re.sub(r"[ \t\r\n]+", " ", text).strip()


def strip_query(url: str) -> str:
    """쿼리스트링·# 제거 (루리웹 검색 결과 링크의 search_key 때문에 같은 글이 검색어마다 다른 id 가 되는 문제)"""
    try:
        p = urlsplit(url)
        return urlunsplit((p.scheme, p.netloc, p.path, "", ""))
    except ValueError:
        return url


def get_hash_id(url: str) -> str:
    return hashlib.md5(url.strip().encode("utf-8")).hexdigest()


def make_item(*, title, link, source, badge, category, snippet="", image=None, raw_date=None, id_key=None, **extra):
    """피드 항목 dict 생성 (모든 크롤러 공통 형식). category = (카테고리, 카테고리 이름)
    id 는 링크(또는 id_key) 해시 — 기존 데이터와 id 가 같아야 중복·순서가 유지되므로 바꾸지 말 것"""
    return {
        "id": get_hash_id(id_key or link),
        "title": title,
        "snippet": snippet,
        "source": source,
        "source_badge": badge,
        "category": category[0],
        "category_name": category[1],
        "link": link,
        "image": image,
        **date_fields(raw_date),
        **extra,
    }


def fetch_feed(url: str, headers=None):
    """RSS 를 requests(타임아웃 있음)로 받아서 feedparser 로 파싱. 네트워크 오류는 예외로 올림"""
    res = http_get(url, headers=headers, timeout=FEED_TIMEOUT)
    res.raise_for_status()
    return feedparser.parse(res.content)


def feed_entries(url: str, limit: int):
    """RSS 항목 → (제목, 링크, 발행일, 요약) 목록. 제목·링크가 없는 항목은 건너뜀"""
    out = []
    for entry in fetch_feed(url).entries[:limit]:
        title = clean_text(entry.get("title", ""))
        link = (entry.get("link") or "").strip()
        if not title or not link:
            continue
        out.append((title, link, entry.get("published", ""), clean_text(entry.get("summary", ""))))
    return out


GN_LOCALES = {
    "ko": "hl=ko&gl=KR&ceid=KR:ko",
    "en": "hl=en-US&gl=US&ceid=US:en",
    "ja": "hl=ja&gl=JP&ceid=JP:ja",
}


def google_news_url(query: str, locale: str = "ko") -> str:
    return f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&{GN_LOCALES[locale]}"


def split_news_source(title: str, default: str):
    """구글 뉴스 '제목 - 언론사' → (제목, 언론사)"""
    if " - " in title:
        t, s = title.rsplit(" - ", 1)
        return t.strip(), s.strip()
    return title.strip(), default


def write_text_atomic(path: str, text: str):
    """같은 폴더 임시 파일에 쓴 뒤 os.replace — 중간에 죽어도 기존 파일이 잘리지 않음"""
    folder = os.path.dirname(path)
    os.makedirs(folder, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=folder, prefix=".tmp-", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def write_json_atomic(path: str, data, indent=2):
    write_text_atomic(path, json.dumps(data, ensure_ascii=False, indent=indent))


# ---------------------------------------------------------------------------
# 스팸/차단 판별
# ---------------------------------------------------------------------------

# 한 글자·짧은 일반어('릴'→릴리즈, '스핀'→스핀오프, '사설'→사설 기사)는 정상 글을 지우므로 구체적인 표현만 사용
SPAM_KEYWORDS = [
    "릴게임", "바다이야기", "슬롯", "카지노", "바카라", "토토", "베팅", "환전", "먹튀",
    "꽁머니", "룰렛", "홀덤", "파워볼", "사설토토", "사설 토토", "사설카지노", "사설사이트", "배팅",
    "spinkr", "프리스핀", "잭팟"
]

# 출처명·링크에 부분 문자열로 들어 있으면 차단
BLOCKED_SOURCES = [
    "calgary", "roughnecks", "spinkr", "casino",
    "p-bandai.jp", "p-bandai.com", "premium bandai", "프리미엄 반다이",
    "prom.pudding", "jawasjunkyard", "reddit"
]

# 짧은 도박 단어는 부분 문자열로 찾으면 'wikimon.net/Betamon', 'alphabet' 같은 정상 글이 걸림
# → 링크의 도메인 라벨과 출처명의 단어 단위로만 검사
BLOCKED_WORD_RE = re.compile(r"^\d*(bet|bets|betting|slot|slots|toto)\d*$")


def _has_blocked_word(source: str, link: str) -> bool:
    try:
        host = urlsplit(link).hostname or ""
    except ValueError:
        host = ""
    tokens = re.split(r"[^a-z0-9]+", host.lower()) + re.split(r"[^a-z0-9]+", source.lower())
    return any(BLOCKED_WORD_RE.match(t) for t in tokens if t)


TRUSTED_NEWS_SOURCES = [
    "인벤", "게임메카", "디스이즈게임", "지디넷", "아이뉴스24", "연합뉴스",
    "kbs", "sbs", "mbc", "jtbc", "ytn", "한국경제", "매일경제", "전자신문",
    "경향신문", "한겨레", "조선일보", "중앙일보", "동아일보", "뉴시스", "뉴스1",
    "스포츠조선", "스포츠서울", "데일리스포츠", "게임포커스", "게임샷", "게임톡",
    "스마트pc사랑", "반다이", "youtube", "유튜브", "포모스", "경향게임스",
    "디지몬", "haveagood-holiday", "x.com", "twitter", "트위터", "toei", "토에이",
    "digimoncard", "wikimon", "bandai", "dengeki", "hobby.watch", "impress",
    "animenewsnetwork", "crunchyroll", "ign", "kotaku", "siliconera", "famitsu", "4gamer", "gamebiz",
    "oricon", "natalie", "mantan-web", "daewon", "대원",
    "digimonmeta", "egmanevents", "amiami", "goodsmile", "hlj", "arca.live", "naver", "네이버"
]


def is_spam(item: dict) -> bool:
    """불법 도박/스팸 사이트, 차단 사이트(레딧, 프반), 무관 인스타 판별"""
    title = (item.get("title") or "").lower()
    snippet = (item.get("snippet") or "").lower()
    source = (item.get("source") or "").lower()
    link = (item.get("link") or "").lower()
    sbadge = item.get("source_badge") or ""

    # 1. 차단 소스 / 도메인 / 뱃지 체크 (레딧, 프리미엄 반다이 직접 링크 차단)
    if sbadge == "레딧" or any(b in source or b in link for b in BLOCKED_SOURCES) or "market/board" in link or "board/1020" in link:
        return True
    if _has_blocked_word(source, link):
        return True

    # 2. 스팸 키워드 포함 여부 체크
    for kw in SPAM_KEYWORDS:
        if kw in title or kw in snippet:
            return True

    # 3. 구글 뉴스 출처 검증 (검증된 언론사/공식 채널만 허용)
    if sbadge == "뉴스":
        matched_trusted = any(t in source for t in TRUSTED_NEWS_SOURCES)
        if not matched_trusted:
            return True

    # 4. 글로벌/해외/일본 뉴스, 하비, 인스타그램 디지몬 키워드 엄격 검증
    if sbadge in ["해외뉴스", "일본뉴스", "프반/하비", "하비뉴스", "애니뉴스", "인스타그램"]:
        full = f"{title} {snippet} {link}".lower()
        digi_kws = ["디지몬", "digimon", "デジモン", "디지바이스", "digivice", "デジヴァイス", "vpet", "v-pet", "아구몬", "agumon", "vital", "바이탈", "seekers", "liberator"]
        if not any(k in full for k in digi_kws):
            return True

    # 5. 인스타그램은 디지몬 키워드가 제목/본문에 직접 있어야 하며 프로필 페이지는 제외
    if sbadge == "인스타그램":
        if any(b in link or b in title for b in ["prom.pudding", "jawasjunkyard"]):
            return True
        if not any(k in title for k in ["디지몬", "digimon", "デジモン", "디지바이스", "아구몬"]):
            return True

    return False


# ---------------------------------------------------------------------------
# 카테고리 분류
# ---------------------------------------------------------------------------

def _kw(k):
    """3글자 이하 영문/숫자 키워드(ost, mr, pv, tcg…)는 단어 경계로만 매칭 ('ghost'·'most'·'next' 오탐 방지)"""
    if isinstance(k, re.Pattern):
        return k
    k = k.lower()
    if re.fullmatch(r"[a-z0-9.\-]{1,3}", k):
        return re.compile(rf"(?<![a-z0-9]){re.escape(k)}(?![a-z0-9])")
    return k


def kw_list(*words):
    return [_kw(w) for w in words]


def has_kw(text: str, kws) -> bool:
    return any((k.search(text) if isinstance(k, re.Pattern) else k in text) for k in kws)


# 카드 세트 코드 (BT18, EX-07 …) — 'bt'/'ex' 를 부분 문자열로 찾으면 'next'·'debt' 등이 걸림
SET_CODE_RE = re.compile(r"(?<![a-z0-9])(bt|ex)-?\d{1,2}(?!\d)")

# 신작/속보 무누락 휴리스틱 트리거 (New Project Zero-Miss Triggers)
NEW_PROJECT_TRIGGERS = kw_list(
    # 한국어
    "신작", "신프로젝트", "신시리즈", "제작 결정", "방영 결정", "발매 결정", "신규 디지몬",
    "티저", "공식 발표", "새로운 프로젝트", "25주년", "신규 애니", "신규 게임", "신규 tcg", "신규 프로젝트",
    # 영어
    "new project", "new anime", "new game", "new series", "announcement", "teaser", "reveal",
    "unveiled", "new digimon", "25th anniversary", "upcoming", "brand new",
    # 일본어
    "新作", "新プロジェクト", "新シリーズ", "制作決定", "発表", "解禁", "アニメ化", "ゲーム化", "25周年"
)
NEW_CARD_KWS = kw_list("카드", "tcg", "digimontcg", "디지카", "부스터", "덱")
NEW_GOODS_KWS = kw_list("피규어", "프라모델", "쿠지", "완구", "디지바이스", "굿즈")
SNS_KWS = kw_list(
    "butter-fly", "butterfly", "brave heart", "커버", "cover", "tula",
    "노래", "ost", "음악", "라이브", "불러보았다", "mr", "target", "an endless tale"
)
YOUTUBE_OFFICIAL_KWS = kw_list("공식", "pv", "트레일러", "발매", "신작")
FIGURE_KWS = kw_list(
    "피규어", "프라모델", "굿즈", "쿠지", "이치방쿠지", "반다이", "figurerise",
    "shf", "스탠다드", "예약", "발매", "디지바이스", "완구", "아크릴", "인형",
    "가챠", "캡슐", "스피릿", "프론티어", "아머체", "d-arts", "g.e.m", "gem",
    "프레임", "스탠드", "펜들럼", "바이탈 브레스", "vital bracelet", "バイタルブレス", "컬러", "디지몬 컬러"
)
CARD_KWS = kw_list(
    "디지카", "tcg", "digimontcg", "카드", "부스터", "패러렐", "시크릿", "덱리", "덱 리스트",
    "스타터", "챔피언십", "에이스", "디지몬카드", "시크릿 크라이시스", SET_CODE_RE,
    "리베레이터", "liberator"
)
GAME_KWS = kw_list(
    "사이버 슬루스", "서바이브", "신작 게임", "스팀", "극장판", "애니메이션",
    "더빙", "방영", "닌텐도", "플레이스테이션", "공식 pv", "티저 pv", "시커즈", "seekers",
    "고스트 게임", "어드벤처", "테이머즈", "세이버즈", "크로스워즈", "어플몬", "비트브레이크"
)


def classify_category(title: str, default_cat: str = "general", source_badge: str = "") -> tuple:
    title_lower = (title or "").lower()

    # [0순위] 신작/속보 트리거 감지 (새로운 타이틀이라도 신작 속보로 즉시 캐치)
    if has_kw(title_lower, NEW_PROJECT_TRIGGERS):
        if has_kw(title_lower, NEW_CARD_KWS):
            return "card", "신작 카드(TCG)"
        if has_kw(title_lower, NEW_GOODS_KWS):
            return "figure", "신작 굿즈/완구"
        return "game", "신작/공식속보"

    # [1순위] 유튜브 노래 커버곡 / 팬 음악 / OST (무조건 SNS 카테고리로 격리)
    if has_kw(title_lower, SNS_KWS) or (source_badge == "유튜브" and not has_kw(title_lower, YOUTUBE_OFFICIAL_KWS)):
        return "sns", "SNS/영상"

    # [2순위] 피규어 / 굿즈 / 쿠지(이치방쿠지) / 완구(디지바이스, 펜들럼) / 프론티어 25주년
    if has_kw(title_lower, FIGURE_KWS):
        return "figure", "피규어/굿즈"

    # [3순위] 카드게임 (TCG / 디지카 / 리베레이터 연계)
    if has_kw(title_lower, CARD_KWS):
        return "card", "카드게임(TCG)"

    # [4순위] 정식 게임 / 공식 극장판/애니 / 웹코믹/소설
    if has_kw(title_lower, GAME_KWS):
        return "game", "게임/공식애니"

    # [5순위] 소스 기반 매핑
    if source_badge in ["X(트위터)", "유튜브", "인스타그램"]:
        return "sns", "SNS/해외"
    if source_badge in ["디시", "아카라이브"]:
        return "community", "커뮤니티"
    if source_badge in ["루리웹", "프반/하비", "하비뉴스"]:
        return "figure", "피규어/굿즈"
    if source_badge == "디지카":
        return "card", "카드게임(TCG)"
    if source_badge in ["애니뉴스", "네이버"]:
        return "game", "게임/공식애니"
    if source_badge in ["위키몬", "위키몬 신규"]:
        return "general", "도감/위키"

    return default_cat, "일반/뉴스"


# 크롤러가 제목에 붙이는 출처 접두사 — 재분류·번역 때는 떼고 원래 제목으로 판단
SOURCE_PREFIXES = [
    "해외공식", "영상", "트윗", "인스타", "해외뉴스", "일본뉴스", "디지카", "하비뉴스",
    "애니뉴스", "위키몬 신규", "공식 상품", "공식", "루리웹", "아카라이브", "네이버"
]
SOURCE_PREFIX_RE = re.compile(r"^(\[(?:" + "|".join(map(re.escape, SOURCE_PREFIXES)) + r")\]\s*)(.*)$", re.S)


def split_prefix(title: str):
    """'[해외뉴스] 제목' → ('[해외뉴스] ', '제목'). 크롤러 접두사가 아니면 ('', 제목)"""
    m = SOURCE_PREFIX_RE.match(title or "")
    if m:
        return m.group(1), m.group(2).strip()
    return "", (title or "").strip()


# 수집할 때 카테고리를 고정으로 정하는 출처 → 기존 항목 재분류 때도 그대로 유지
FIXED_CATEGORY_SOURCES = {
    "Digimon Meta", "Egman Events",                      # crawl_digimon_meta_and_egman (card)
    "AmiAmi", "아미아미", "굿스마일", "HLJ",                # crawl_figure_shops (figure)
    "루리웹 콘솔뉴스", "루리웹 PC뉴스", "루리웹 애니정보",      # crawl_ruliweb_game_and_anime (game)
    "Wikimon",                                           # crawl_wikimon_updates (general)
}


def category_is_fixed(item: dict) -> bool:
    return item.get("source") in FIXED_CATEGORY_SOURCES or (item.get("title") or "").startswith("[공식 상품]")


# ---------------------------------------------------------------------------
# 크롤러
# ---------------------------------------------------------------------------

def crawl_google_news_digimon():
    items = []
    # 굿즈/쿠지/프론티어 25주년/카드게임/신작/전시리즈를 아우르는 다중 검색 쿼리
    queries = [
        "디지몬 굿즈 OR 디지몬 피규어 OR 디지몬 쿠지 OR 디지몬 이치방쿠지 OR \"디지몬 프론티어\" OR 디지바이스 OR \"디지몬 25주년\"",
        "디지카 OR \"디지몬 카드게임\" OR \"디지몬 리베레이터\" OR \"디지몬 시커즈\"",
        "디지몬 신작 OR \"디지몬 어드벤처\" OR \"디지몬 테이머즈\" OR \"고스트 게임\" OR \"디지몬 월드\" OR \"사이버 슬루스\"",
        "디지몬"
    ]

    for q in queries:
        try:
            for title, link, published, summary in feed_entries(google_news_url(q, "ko"), 15):
                title, source_name = split_news_source(title, "뉴스")
                items.append(make_item(
                    title=title, link=link, source=source_name, badge="뉴스",
                    category=classify_category(title, "general", "뉴스"),
                    snippet=summary[:120].strip() if summary else f"{source_name} 보도자료",
                    raw_date=published,
                ))
        except Exception as e:
            log_error(f"Google News ({q}) crawling failed: {e}")
    return items


def _ruliweb_rows(url: str, limit: int):
    """루리웹 게시판 목록 → (제목, 링크(쿼리 제거), 행) 목록"""
    res = http_get(url)
    if res.status_code != 200:
        log_error(f"Ruliweb HTTP {res.status_code}: {url}")
        return []
    soup = BeautifulSoup(res.text, "html.parser")
    out = []
    for row in soup.select("tr.table_body")[:limit]:
        title_elem = row.select_one("td.subject a.text_over") or row.select_one("td.subject a")
        if not title_elem:
            continue
        title = clean_text(title_elem.get_text(strip=True))
        link = title_elem.get("href", "")
        if not link or "read/" not in link or "#cmt" in link or "notice" in row.get("class", []):
            continue
        if not link.startswith("http"):
            link = "https://bbs.ruliweb.com" + link
        # 검색 결과 링크의 ?search_type=…&search_key=… 를 떼어 같은 글은 같은 id 가 되도록
        link = strip_query(link)
        title = re.sub(r'\(\d+\)$', '', title).strip()
        out.append((title, link, row))
    return out


def _row_time(row) -> str:
    time_elem = row.select_one("td.time")
    return time_elem.get_text(strip=True) if time_elem else ""


def crawl_ruliweb_hobby():
    items = []
    # 디지몬, 프론티어, 쿠지, 완구/디지바이스 검색 교차 수집
    search_keys = ["디지몬", "프론티어", "이치방쿠지 디지몬", "디지바이스"]
    for skey in search_keys:
        url = f"https://bbs.ruliweb.com/family/242/board/300017?search_type=subject&search_key={requests.utils.quote(skey)}"
        try:
            for title, link, row in _ruliweb_rows(url, 18):
                if "board/300017/read/" not in link:
                    continue

                # 검색어 관련성 검증
                title_l = title.lower()
                if skey == "프론티어" and not any(k in title_l for k in ["디지몬", "스피릿", "아그니몬", "볼프몬"]):
                    continue
                if skey == "디지바이스" and not any(k in title_l for k in ["디지몬", "디지바이스", "펜들럼", "컬러"]):
                    continue

                img_elem = row.select_one("img.thumb")
                img_src = img_elem.get("src") if img_elem else None
                if img_src and img_src.startswith("//"):
                    img_src = "https:" + img_src

                items.append(make_item(
                    title=title, link=link, source="루리웹 모형정보", badge="루리웹",
                    category=classify_category(title, "figure", "루리웹"),
                    snippet="루리웹 피규어/프라모델/굿즈 정보 게시판 소식입니다.",
                    image=img_src, raw_date=_row_time(row),
                ))
        except Exception as e:
            log_error(f"Ruliweb ({skey}) crawling failed: {e}")
    return items


def crawl_dc_gallery(gallery_id: str, badge_name: str, default_cat: str):
    items = []
    # 모바일 웹은 차단이 적고 심플함
    url = f"https://m.dcinside.com/board/{gallery_id}?recommend=1"
    m_headers = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",
        "Referer": "https://m.dcinside.com/"
    }
    try:
        res = http_get(url, headers=m_headers)
        if res.status_code != 200:
            log_error(f"DC {gallery_id} HTTP {res.status_code}")
            return items
        soup = BeautifulSoup(res.text, "html.parser")
        for row in soup.select("ul.gall-detail-lst > li")[:15]:
            title_elem = row.select_one(".subjectin")
            link_elem = row.select_one("a:not(.btn-reply-more)")
            if not title_elem or not link_elem:
                continue
            title = clean_text(title_elem.get_text(strip=True))
            link = link_elem.get("href", "")
            if not title or not link:
                continue
            if not link.startswith("http"):
                link = "https://m.dcinside.com" + link

            time_elem = row.select_one(".ginfo li:nth-child(2)")
            items.append(make_item(
                title=title, link=link, source=f"디시 {badge_name}", badge="디시",
                category=classify_category(title, default_cat, "디시"),
                snippet=f"디시인사이드 {badge_name} 갤러리 추천 개념글입니다.",
                raw_date=time_elem.get_text(strip=True) if time_elem else "",
            ))
    except Exception as e:
        log_error(f"DC {gallery_id} crawling failed: {e}")
    return items


def crawl_withthewill_news():
    items = []
    # 2026-11 확인: 예전 뉴스 게시판(digimon-news.20)이 없어지고 announcements-and-news.10 으로 통합됨
    feed_url = "https://withthewill.net/forums/announcements-and-news.10/index.rss"
    try:
        for title, link, published, summary in feed_entries(feed_url, 10):
            items.append(make_item(
                title=f"[해외공식] {title}", link=link, source="WithTheWill", badge="공식/글로벌",
                category=classify_category(title, "game", "공식/글로벌"),
                snippet=summary[:120].strip() or "WithTheWill 글로벌 디지몬 최신 뉴스입니다.",
                raw_date=published,
            ))
    except Exception as e:
        log_error(f"WithTheWill crawling failed: {e}")
    return items


def crawl_youtube_digimon():
    items = []
    try:
        for title, link, published, _ in feed_entries(google_news_url('site:youtube.com "디지몬"', "ko"), 15):
            title = title.replace(" - YouTube", "").strip()
            # 노래/커버/음악은 SNS/영상으로 자동 분류됨
            items.append(make_item(
                title=f"[영상] {title}", link=link, source="YouTube", badge="유튜브",
                category=classify_category(title, "sns", "유튜브"),
                snippet="YouTube 최신 디지몬 공식/인기 영상입니다.",
                raw_date=published,
            ))
    except Exception as e:
        log_error(f"YouTube crawling failed: {e}")
    return items


# 굿즈, 쿠지, 프론티어, 공식 계정, 신작, 해외/일본 공식 계정을 아우르는 X 검색
TWITTER_QUERY = (
    'site:x.com OR site:twitter.com ("digimonweb_net" OR "digimon_tcg" OR "toei_anim_goods" OR "デジモン" '
    'OR "DigimonTCG" OR "디지몬 굿즈" OR "디지몬 쿠지" OR "디지몬 프론티어" OR "디지몬 신작" OR "디지몬")'
)


def crawl_twitter_digimon():
    items = []
    try:
        for title, link, published, _ in feed_entries(google_news_url(TWITTER_QUERY, "ko"), 20):
            if " on X: " in title:
                author, text = title.split(" on X: ", 1)
                title = f"@{author}: {text.replace(chr(34), '').strip()}"
            items.append(make_item(
                title=f"[트윗] {title}", link=link, source="X(트위터)", badge="X(트위터)",
                category=classify_category(title, "sns", "X(트위터)"),
                snippet="X(트위터) 디지몬 공식/굿즈 속보 트윗입니다.",
                raw_date=published,
            ))
    except Exception as e:
        log_error(f"Twitter crawling failed: {e}")
    return items


def crawl_instagram_digimon():
    """인스타그램은 비로그인 직접 접근이 막혀 있어 구글뉴스 RSS site: 검색으로 공개 디지몬 게시물(/p/, /reel/)만 수집"""
    items = []
    queries = [
        'site:instagram.com/p/ "#디지몬"',
        'site:instagram.com/p/ "#digimon"',
        'site:instagram.com/p/ "#デジモン"',
        'site:instagram.com/p/ "디지몬"',
        'site:instagram.com/p/ "digimon"',
        'site:instagram.com/reel/ "디지몬"',
    ]
    seen = set()
    digi_check = ["디지몬", "digimon", "デジモン", "디지바이스", "아구몬", "테이머즈", "v-pet"]
    for q in queries:
        try:
            for raw_title, link, published, _ in feed_entries(google_news_url(q, "ko"), 15):
                if link in seen:
                    continue
                seen.add(link)
                title = re.sub(r"\s*[-•|]\s*Instagram.*$", "", raw_title).strip()
                title = re.sub(r"^[•·\s]+", "", title).strip()
                if not title:
                    continue
                # 제목/본문에 디지몬 관련 단어가 명시되지 않은 일반 계정/글은 즉시 배제
                if not any(k in title.lower() for k in digi_check):
                    continue
                items.append(make_item(
                    title=f"[인스타] {title}", link=link, source="Instagram", badge="인스타그램",
                    category=classify_category(title, "sns", "인스타그램"),
                    snippet="인스타그램 디지몬 관련 공개 게시물입니다.",
                    raw_date=published,
                ))
        except Exception as e:
            log_error(f"Instagram crawling failed ({q}): {e}")
    return items


def _crawl_google_news_world(queries, locale, badge, default_source, limit):
    """구글 뉴스 해외(영어/일본어) 피드 공통 수집"""
    items = []
    seen = set()
    for q in queries:
        try:
            for title, link, published, summary in feed_entries(google_news_url(q, locale), limit):
                if link in seen:
                    continue
                seen.add(link)
                title, source_name = split_news_source(title, default_source)
                items.append(make_item(
                    title=f"[{badge}] {title}", link=link, source=source_name, badge=badge,
                    category=classify_category(title, "general", badge),
                    snippet=summary[:120].strip() if summary else f"{source_name} 보도자료",
                    raw_date=published,
                ))
        except Exception as e:
            log_error(f"{badge} ({q}) crawling failed: {e}")
    return items


def crawl_google_news_global_en():
    """구글 뉴스 글로벌 영어 피드 수집 (US:en)"""
    queries = [
        '"Digimon" OR "Digimon Card Game" OR "Digimon Liberator" OR "Digimon Seekers"',
        '"Digimon Adventure" OR "Digimon World" OR "Digivice" OR "Vital Bracelet"',
        '"Digimon" ("Bandai" OR "Toei Animation" OR "Anime")'
    ]
    return _crawl_google_news_world(queries, "en", "해외뉴스", "Global News", 12)


def crawl_google_news_global_ja():
    """구글 뉴스 일본어 피드 수집 (JP:ja)"""
    queries = [
        '"デジモン" OR "デジモンカードゲーム" OR "デジモンリベレイター" OR "デジモンシーカーズ"',
        '"デジモン フィギュア" OR "デジヴァイス" OR "デジタルモンスター" OR "バイタルブレス"',
        '"デジモン" ("バンダイ" OR "東映アニメーション" OR "プレミアムバンダイ")'
    ]
    return _crawl_google_news_world(queries, "ja", "일본뉴스", "일본 뉴스", 12)


def _crawl_site_search(queries, locale, limit, snippet_tpl, label, fixed_category=None):
    """구글 뉴스 site: 검색 공통 수집. queries = [(검색어, 출처명, 뱃지, 기본 카테고리)]
    fixed_category=(카테고리, 이름) 이면 제목 분류 없이 고정"""
    items = []
    seen = set()
    for q, sname, badge, def_cat in queries:
        try:
            for title, link, published, summary in feed_entries(google_news_url(q, locale), limit):
                if link in seen:
                    continue
                seen.add(link)
                title, _ = split_news_source(title, sname)
                items.append(make_item(
                    title=f"[{badge}] {title}", link=link, source=sname, badge=badge,
                    category=fixed_category or classify_category(title, def_cat, badge),
                    snippet=summary[:120].strip() or snippet_tpl.format(sname=sname),
                    raw_date=published,
                ))
        except Exception as e:
            log_error(f"{label} ({q}) crawling failed: {e}")
    return items


def crawl_tcg_portals():
    """디지몬 카드게임 전문 포털 수집 (digimoncard.io, digimoncard.co.kr, digimoncard.com)"""
    queries = [
        ('site:digimoncard.io', 'DigimonCard.io', '디지카', 'card'),
        ('site:digimoncard.co.kr', '공식 디지카KR', '디지카', 'card'),
        ('site:digimoncard.com', '반다이 공식 디지카', '디지카', 'card'),
    ]
    return _crawl_site_search(queries, "ko", 10, "{sname} 디지몬 카드게임 소식입니다.", "TCG portal")


def crawl_wikimon_updates():
    """위키몬(Wikimon) 신규 등록 문서(New Pages) 전용 수집 (오타 수정 제외, 100% 공개 본문 링크)"""
    items = []
    url = "https://wikimon.net/api.php?action=query&list=recentchanges&rctype=new&rcnamespace=0&rclimit=15&format=json"
    try:
        res = http_get(url, headers={"User-Agent": "Mozilla/5.0 (DigimonTracker/1.0)"})
        if res.status_code != 200:
            log_error(f"Wikimon API HTTP {res.status_code}")
            return items
        data = res.json()
        rcs = data.get("query", {}).get("recentchanges", []) if isinstance(data, dict) else []
        for rc in rcs:
            raw_title = (rc.get("title") or "").strip()
            if not raw_title:
                continue
            # 로그인 필요한 diff= 대신 100% 공개되는 문서 본문 링크로 생성
            clean_name = raw_title.replace(" ", "_")
            raw_title = clean_text(raw_title)
            clean_link = f"https://wikimon.net/{requests.utils.quote(clean_name)}"
            items.append(make_item(
                title=f"[위키몬 신규] {raw_title}", link=clean_link, source="Wikimon", badge="위키몬",
                category=("general", "도감/위키"),
                snippet=f"Wikimon에 새로 등록된 공식 신규 문서 ({raw_title})입니다.",
                raw_date=rc.get("timestamp", ""),
            ))
    except Exception as e:
        log_error(f"Wikimon API crawling failed: {e}")
    return items


def crawl_premium_bandai_hobby():
    """하비 전문 매체 수집 (IP 차단 없는 전격하비, 하비워치만 수집)"""
    queries = [
        ('site:hobby.dengeki.com "デジモン"', '전격하비', '하비뉴스', 'figure'),
        ('site:hobby.watch.impress.co.jp "デジモン"', '하비워치', '하비뉴스', 'figure'),
    ]
    return _crawl_site_search(queries, "ja", 8, "{sname} 디지몬 하비/완구 소식입니다.", "Hobby portal")


def crawl_global_anime_news():
    """글로벌 애니메이션 웹진 디지몬 소식 수집"""
    queries = [
        ('site:animenewsnetwork.com "Digimon"', 'Anime News Network', '애니뉴스', 'game'),
        ('site:crunchyroll.com/news "Digimon"', 'Crunchyroll News', '애니뉴스', 'game'),
    ]
    return _crawl_site_search(queries, "en", 8, "{sname} 디지몬 글로벌 보도입니다.", "Anime news")


def crawl_digimon_meta_and_egman():
    """디지몬 카드게임 대회 우승 덱 & 메타 분석 사이트 (Digimon Meta, Egman Events)"""
    queries = [
        ('site:digimonmeta.com "deck"', 'Digimon Meta', '디지카', 'card'),
        ('site:digimonmeta.com', 'Digimon Meta', '디지카', 'card'),
        ('site:egmanevents.com "digimon"', 'Egman Events', '디지카', 'card'),
    ]
    return _crawl_site_search(queries, "en", 8, "{sname} 디지카 대회 우승 덱/메타 정보입니다.", "TCG meta",
                              fixed_category=("card", "카드게임(TCG)"))


def crawl_figure_shops():
    """피규어/굿즈 예약 전문 샵 (AmiAmi, GoodSmile, HLJ)"""
    queries = [
        ('site:amiami.com "Digimon"', 'AmiAmi', '하비뉴스', 'figure'),
        ('site:amiami.jp "デジモン"', '아미아미', '하비뉴스', 'figure'),
        ('site:goodsmile.info "デジモン" OR site:goodsmile.com "Digimon"', '굿스마일', '하비뉴스', 'figure'),
        ('site:hlj.com "Digimon"', 'HLJ', '하비뉴스', 'figure'),
    ]
    return _crawl_site_search(queries, "ja", 8, "{sname} 디지몬 피규어/굿즈 예약 정보입니다.", "Figure shop",
                              fixed_category=("figure", "피규어/굿즈"))


def crawl_ruliweb_game_and_anime():
    """루리웹 콘솔/PC 게임 정보 및 애니메이션 정보 게시판"""
    items = []
    boards = [
        ("https://bbs.ruliweb.com/news/board/1001?search_type=subject&search_key=디지몬", "루리웹 콘솔뉴스", "game", "게임/공식애니"),
        ("https://bbs.ruliweb.com/news/board/1003?search_type=subject&search_key=디지몬", "루리웹 PC뉴스", "game", "게임/공식애니"),
        ("https://bbs.ruliweb.com/family/211/board/300015?search_type=subject&search_key=디지몬", "루리웹 애니정보", "game", "게임/공식애니"),
    ]
    for url, sname, cat, cat_name in boards:
        try:
            for title, link, row in _ruliweb_rows(url, 10):
                items.append(make_item(
                    title=f"[루리웹] {title}", link=link, source=sname, badge="루리웹",
                    category=(cat, cat_name),
                    snippet=f"{sname} 디지몬 최신 소식입니다.",
                    raw_date=_row_time(row),
                ))
        except Exception as e:
            log_error(f"Ruliweb board ({url}) crawling failed: {e}")
    return items


def crawl_arcalive_digimon():
    """아카라이브 디지몬 채널 인기 정보글"""
    return _crawl_site_search([('site:arca.live/b/digimon "디지몬"', "아카라이브", "아카라이브", "community")],
                              "ko", 12, "아카라이브 디지몬 채널 게시글입니다.", "Arcalive")


def crawl_naver_game():
    """네이버 게임 라운지 및 공식 소식"""
    return _crawl_site_search([('site:game.naver.com "디지몬"', "네이버 게임", "네이버", "game")],
                              "ko", 8, "네이버 게임 디지몬 관련 소식입니다.", "Naver Game")


OFFICIAL_SITE = "https://digimon.net/"
OFFICIAL_HEADERS = {"User-Agent": "Mozilla/5.0 (digimon-feed fan project)"}


def _official_date(text: str) -> str:
    """'2026/10/14' · '2026年10月03日発売' → '2026-10-14' (parse_date 가 읽는 형식)"""
    m = re.search(r"(\d{4})\D(\d{1,2})\D(\d{1,2})", text or "")
    return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}" if m else ""


def _official_cards(html_text: str):
    """digimon.net 목록 카드(<li class="c-cardList ...">)에서 링크·제목·태그·날짜 추출"""
    for block in re.findall(r'<li class="c-cardList c-card">(.*?)</a></li>', html_text, re.S):
        link = re.search(r'href="([^"]+)"', block)
        title = re.search(r'<p class="c-card__tit">(.*?)</p>', block, re.S)
        if not link or not title:
            continue
        tag = re.search(r'<li class="c-cardTagList">(.*?)</li>', block, re.S)
        date = re.search(r'<p class="c-card__date">(.*?)</p>', block, re.S)
        release = re.search(r'発売日\s*:\s*</dt>\s*<dd[^>]*>(.*?)</dd>', block, re.S)
        price = re.search(r'価\s*格\s*:\s*</dt>\s*<dd[^>]*>(.*?)</dd>', block, re.S)
        # 상품 id 가 링크+제목으로 정해지므로 기존과 같은 방식으로 추출 (화면 표시용 정리는 clean_text 로 따로)
        strip = lambda m: html.unescape(re.sub(r"<[^>]+>", "", m.group(1))).strip() if m else ""
        yield {
            "link": link.group(1),
            "title": strip(title),
            "tag": strip(tag),
            "date": strip(date),
            "release": strip(release),
            "price": strip(price),
        }


def crawl_official_topics():
    """공식 사이트 뉴스(digimon.net/topic) — 제목·날짜·링크만 저장 (본문은 공식 페이지로 연결)"""
    items = []
    url = OFFICIAL_SITE + "topic/?cat=ALL"
    try:
        res = http_get(url, headers=OFFICIAL_HEADERS, timeout=20)
        res.raise_for_status()
        for c in list(_official_cards(res.text))[:20]:
            title = clean_text(c["title"])
            if not title:
                continue
            items.append(make_item(
                title=f"[공식] {title}", link=requests.compat.urljoin(url, c["link"]),
                source="digimon.net", badge="디지몬웹",
                category=classify_category(title, "game", "디지몬웹"),
                snippet=f"디지몬 공식 사이트 {clean_text(c['tag']) or '뉴스'}",
                raw_date=_official_date(c["date"]),
            ))
    except Exception as e:
        log_error(f"Official topics crawling failed: {e}")
    return items


def crawl_official_products():
    """공식 사이트 상품(digimon.net/product/…) — 상품명·발매일·가격·링크만 저장"""
    items = []
    sections = [("card", "card", "카드게임(TCG)"), ("game", "game", "게임/공식애니"),
                ("goods", "figure", "피규어/굿즈"), ("others", "figure", "피규어/굿즈")]
    now_ts = int(time.time())
    for path, cat, cat_name in sections:
        url = f"{OFFICIAL_SITE}product/{path}/"
        try:
            res = http_get(url, headers=OFFICIAL_HEADERS, timeout=20)
            res.raise_for_status()
            for c in list(_official_cards(res.text))[:8]:
                title = clean_text(c["title"])
                if not title:
                    continue
                raw_link = requests.compat.urljoin(url, c["link"])
                # item_type=1 은 p-bandai.jp 로 302 강제 리다이렉트되어 해외 접속 시 차단됨
                # 차단당하지 않는 디지몬웹 해당 카테고리 공식 목록 페이지로 안전 연결 (방안 A)
                safe_link = url if "item_type=1" in raw_link else raw_link
                release = _official_date(c["release"])
                release_ts = parse_date(release) if release else None
                # 발매 예정 상품은 미래 날짜라 지금 시각으로 맞춤 (발매일은 요약에 표시)
                ts = min(release_ts, now_ts) if release_ts is not None else now_ts
                info = " · ".join(clean_text(x) for x in [f"발매 {release}" if release else c["release"], c["price"]] if x)
                items.append(make_item(
                    title=f"[공식 상품] {title}", link=safe_link,
                    id_key=f"{raw_link}_{c['title']}",  # 기존 id 유지를 위해 원래 추출값 사용
                    source="digimon.net", badge="디지몬웹", category=(cat, cat_name),
                    snippet=info or "디지몬 공식 사이트 상품 정보",
                    # 날짜는 위에서 계산한 값, 병합 때 처음 본 시각 유지
                    timestamp=ts, created_at=format_display_date(ts), keep_first_seen=True,
                ))
        except Exception as e:
            log_error(f"Official products crawling failed ({path}): {e}")
    return items


# ---------------------------------------------------------------------------
# 수집 소스 목록 — 새 소스는 (이름, 함수) 한 줄 추가로 끝
# (이름은 수집 상태 보고에 쓰이므로 소스마다 달라야 함. 결과는 이 순서대로 병합)
# ---------------------------------------------------------------------------

SOURCES = [
    ("Google News (KR)", crawl_google_news_digimon),
    ("Google News (EN)", crawl_google_news_global_en),
    ("Google News (JA)", crawl_google_news_global_ja),
    ("루리웹 모형정보", crawl_ruliweb_hobby),
    ("디시 디지몬", partial(crawl_dc_gallery, "digimon", "디지몬", "community")),
    ("디시 디지카", partial(crawl_dc_gallery, "tcg_digimon", "디지카", "card")),
    ("TCG 포털", crawl_tcg_portals),
    ("WithTheWill", crawl_withthewill_news),
    ("Wikimon 신규 문서", crawl_wikimon_updates),
    ("YouTube", crawl_youtube_digimon),
    ("X(트위터)", crawl_twitter_digimon),
    ("Instagram", crawl_instagram_digimon),
    ("하비 매체", crawl_premium_bandai_hobby),
    ("해외 애니 뉴스", crawl_global_anime_news),
    ("공식 뉴스 (digimon.net)", crawl_official_topics),
    ("공식 상품 (digimon.net)", crawl_official_products),
    ("TCG 메타 (Digimon Meta·Egman)", crawl_digimon_meta_and_egman),
    ("피규어 샵", crawl_figure_shops),
    ("루리웹 게임·애니", crawl_ruliweb_game_and_anime),
    ("아카라이브", crawl_arcalive_digimon),
    ("네이버 게임", crawl_naver_game),
]


def _run_source(name, fn):
    """소스 하나 수집 → (항목 목록, 상태). 크롤러 밖으로 새어 나온 예외도 여기서 잡아 다른 소스는 계속"""
    _tls.source = name
    started = time.time()
    try:
        items = fn() or []
    except Exception as e:
        log_error(f"{name} crawler crashed: {e!r}")
        items = []
    finally:
        _tls.source = None
    with _errors_lock:
        errors = list(SOURCE_ERRORS.get(name, []))
    status = {
        "name": name,
        "count": len(items),
        "errors": len(errors),
        "seconds": round(time.time() - started, 1),
    }
    if errors:
        status["last_error"] = errors[-1]
    return items, status


def crawl_all(sources=None, workers=None):
    """모든 소스를 동시에 수집 (네트워크 대기 위주라 스레드로 충분). 결과는 SOURCES 순서대로 이어 붙임"""
    sources = SOURCES if sources is None else sources
    workers = workers or CRAWL_WORKERS
    with _errors_lock:
        SOURCE_ERRORS.clear()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(lambda s: _run_source(*s), sources))
    all_items, statuses = [], []
    for items, status in results:
        all_items.extend(items)
        statuses.append(status)
        flag = "OK " if status["count"] and not status["errors"] else "!! "
        print(f"  {flag}{status['name']}: {status['count']}건, 오류 {status['errors']}회 ({status['seconds']}s)")
    return all_items, statuses


def report_source_health(statuses):
    """GitHub Actions 실행 화면에 0건·오류 소스를 경고로 표시 (사이트 구조 변경으로 조용히 깨진 스크래퍼 발견용)"""
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return
    for s in statuses:
        if s["count"] == 0:
            detail = f" — {s['last_error']}" if s.get("last_error") else ""
            print(f"::warning title=피드 소스 0건::{s['name']}{detail}")
        elif s["errors"]:
            print(f"::notice title=피드 소스 일부 오류::{s['name']} ({s['errors']}회) — {s.get('last_error', '')}")


# ---------------------------------------------------------------------------
# 제목 번역 (Gemini)
# ---------------------------------------------------------------------------

TITLE_CACHE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "translations", "feed_titles.json")
NAMES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "translations", "names.json")
KO_RE = re.compile(r"[가-힣]")
JA_RE = re.compile(r"[぀-ヿ一-鿿]")
EN_WORD_RE = re.compile(r"[a-zA-Z]{3,}")
TRANSLATE_BATCH = 30
TRANSLATE_BUDGET = 900  # 제목 번역 전체에 쓰는 최대 시간(초) — 넘으면 남은 건 다음 실행에서


def _load_json_dict(path: str, label: str):
    """JSON 객체 파일 읽기. 없으면 {}, 깨졌으면 None (호출하는 쪽에서 안전하게 건너뜀)"""
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("JSON 객체가 아님")
        return data
    except (OSError, ValueError) as e:
        print(f"[Warning] {label} 읽기 실패: {e}")
        return None


def fix_dub_names(ko: str, src: str, names: dict) -> str:
    """'원작 이름(더빙 이름)' 표기 맞춤 — 모델이 괄호 더빙명을 빠뜨리거나 공백을 넣은 경우만 보정 (괄호 중복 방지)"""
    for key, full in names.items():
        if key not in src:
            continue
        m = re.fullmatch(r"\s*(.+?)\s*[(（]\s*(.+?)\s*[)）]\s*", full)
        if not m:
            continue
        base, dub = m.groups()
        canon = f"{base}({dub})"
        spaced = re.compile(re.escape(base) + r"\s*[(（]\s*" + re.escape(dub) + r"\s*[)）]")
        if spaced.search(ko):
            ko = spaced.sub(lambda _m: canon, ko)
        else:
            # 이미 다른 괄호 표기가 붙어 있으면 건드리지 않음
            ko = re.sub(re.escape(base) + r"(?!\s*[(（])", lambda _m: canon, ko, count=1)
    return ko


def translate_feed_titles(items):
    """공식 사이트(일본어) 및 해외 글로벌 피드(영어/일본어) 제목을 Gemini로 한국어 번역.
    원제는 title_orig (일어인 경우 title_ja)에 보관, 번역 결과는 feed_titles.json에 캐시 (현재 피드에 없는 항목은 정리).
    디지몬 고증 이름은 names.json(인물) 및 terms.json(용어, gemini.py 가 읽음) 기준 표기.
    GEMINI_API_KEY 가 없거나 실패하면 원문 그대로 둠. 번역 파일이 손상됐으면 이번 실행은 번역을 건너뜀."""
    try:
        import gemini
    except ImportError:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        import gemini

    loaded_cache = _load_json_dict(TITLE_CACHE, "번역 캐시(feed_titles.json)")
    names_raw = _load_json_dict(NAMES_FILE, "인물 이름표(names.json)")
    if loaded_cache is None or names_raw is None:
        print("[Warning] 번역 파일이 손상되어 이번 실행은 제목 번역을 건너뜁니다 (캐시는 덮어쓰지 않음)")
        return
    cache = {k: v for k, v in loaded_cache.items() if isinstance(v, str) and v}
    names = {k: v for k, v in names_raw.items() if not k.startswith("_") and isinstance(v, str)}

    targets = []
    for item in items:
        prefix, body = split_prefix(item.get("title", ""))
        body = item.get("title_orig") or item.get("title_ja") or body

        # 한글이 전혀 없고, 일본어 또는 영어 단어가 포함된 경우 번역 대상
        if body and not KO_RE.search(body) and (JA_RE.search(body) or EN_WORD_RE.search(body)):
            targets.append((item, prefix, body))

    # 캐시에 없는 항목만 번역
    missing = list(dict.fromkeys(b for _, _, b in targets if b not in cache))
    if missing and gemini.available():
        started = time.time()
        for i in range(0, len(missing), TRANSLATE_BATCH):
            left = TRANSLATE_BUDGET - (time.time() - started)
            if left < 60:
                print(f"[Info] 번역 시간 예산 소진 - 남은 {len(missing) - i}건은 다음 실행에서")
                break
            chunk = missing[i:i + TRANSLATE_BATCH]
            gl = {}
            for t in chunk:
                gl.update({k: v for k, v in names.items() if k in t})
            try:
                out = gemini.translate_map({str(n): t for n, t in enumerate(chunk)}, gl, max_seconds=left)
            except (gemini.BadResponse, gemini.Busy) as e:
                # 이 묶음만 실패 (안전 차단·해석 불가·혼잡) → 건너뛰고 나머지 묶음은 계속
                print(f"[Warning] 제목 번역 묶음 {i // TRANSLATE_BATCH + 1} 건너뜀: {e}")
                continue
            except Exception as e:
                # 일일 한도(QuotaExceeded)·키 오류(AuthError)·모델 목록 조회 실패 → 나머지도 실패하므로 중단
                print(f"[Warning] Feed title translation stopped: {e}")
                break
            for n, t in enumerate(chunk):
                ko = out.get(str(n))
                if ko:
                    cache[t] = fix_dub_names(ko, t, names)
    elif missing:
        print(f"[Info] GEMINI_API_KEY 미설정 ({len(missing)}건 번역 대기 - 원문 유지)")

    for item, prefix, body in targets:
        if body in cache:
            item["title_orig"] = body
            if JA_RE.search(body):
                item["title_ja"] = body
            item["title"] = prefix + cache[body]

    # 현재 피드에서 쓰는 번역만 남김 (보존 기간이 지난 제목이 캐시에 계속 쌓이지 않도록)
    used = {b for _, _, b in targets}
    pruned = {k: v for k, v in cache.items() if k in used}
    if pruned != loaded_cache:
        write_json_atomic(TITLE_CACHE, pruned, indent=1)


# ---------------------------------------------------------------------------
# 병합·저장
# ---------------------------------------------------------------------------

def load_existing_feeds():
    """기존 피드 읽기. 파일이 없으면 [], 읽을 수 없으면 히스토리를 덮어쓰지 않도록 즉시 중단(exit 1)"""
    if not os.path.exists(FEEDS_FILE):
        return []
    try:
        with open(FEEDS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        feeds = data.get("feeds") if isinstance(data, dict) else None
        if not isinstance(feeds, list):
            raise ValueError("'feeds' 목록이 없음")
    except Exception as e:
        print(f"[Fatal] 기존 피드 파일을 읽을 수 없어 중단합니다 (히스토리 보호): {FEEDS_FILE}: {e}")
        sys.exit(1)
    return [x for x in feeds if isinstance(x, dict) and x.get("id")]


def normalize_existing(item: dict):
    """기존 항목 보정 (차단 소스 숙청, 루리웹 링크 정규화, 엔티티 정리, KST 표시, 최신 규칙 재분류). 버릴 항목이면 None"""
    # 차단 소스 (레딧, 프반, 무관 인스타 등) 즉시 탈락
    if is_spam(item):
        return None
    link = item.get("link") or ""

    # 디지몬웹 프반 302 리다이렉트 상품 링크 안전 처리 (방안 A)
    if item.get("source_badge") == "디지몬웹" and "item_type=1" in link:
        item["link"] = "https://digimon.net/product/goods/"

    # 위키몬: 신규 등록 문서([위키몬 신규])만 유지하고 과거 일반 수정글은 제외
    if item.get("source_badge") == "위키몬" and not (item.get("title") or "").startswith("[위키몬 신규]"):
        return None

    # 루리웹: 검색어 쿼리를 뗀 링크로 id 재계산 (같은 글이 검색어별로 중복 저장된 문제)
    if "ruliweb.com" in link:
        clean = strip_query(link)
        if clean != link:
            item["link"] = clean
            item["id"] = get_hash_id(clean)

    # 남아 있는 HTML 엔티티(&nbsp; 등) 정리
    for key in ("title", "snippet", "title_orig", "title_ja"):
        if isinstance(item.get(key), str):
            item[key] = clean_text(item[key])

    ts = item.get("timestamp")
    if not isinstance(ts, (int, float)) or isinstance(ts, bool):
        ts = parse_to_timestamp(item.get("created_at", ""))
    item["timestamp"] = int(ts)
    item["created_at"] = format_display_date(item["timestamp"])  # KST 표기로 통일

    # category가 리스트나 튜플 형태인 경우 문자열로 복구
    if isinstance(item.get("category"), (list, tuple)):
        item["category"] = item["category"][0] if item["category"] else "general"

    # 카테고리 최신 규칙에 따라 재분류 — 출처 접두사를 뗀 원래 제목으로, 수집 때 고정한 카테고리는 유지
    if not category_is_fixed(item):
        orig_title = item.get("title_orig") or split_prefix(item.get("title", ""))[1]
        item["category"], item["category_name"] = classify_category(
            orig_title, item.get("category") or "general", item.get("source_badge", ""))

    item.pop("keep_first_seen", None)
    return item


def _dedupe_key(item: dict):
    """구글 뉴스 경유 항목의 (뱃지, 출처, 정규화 제목). 대상이 아니면 None"""
    if not (item.get("link") or "").startswith("https://news.google.com/"):
        return None
    title = item.get("title_orig") or split_prefix(item.get("title", ""))[1]
    title = re.sub(r"\s+", " ", title).strip().lower()
    if not title:
        return None
    return item.get("source_badge", ""), item.get("source", ""), title


def dedupe_feeds(items):
    """같은 출처의 같은 제목(구글 뉴스 재배포 기사, 같은 제목 반복 글)은 가장 먼저 올라온 항목 하나만 남김"""
    best = {}
    for item in items:
        key = _dedupe_key(item)
        if key is None:
            continue
        cur = best.get(key)
        if cur is None or item.get("timestamp", 0) < cur.get("timestamp", 0):
            best[key] = item
    keep = {id(x) for x in best.values()}
    return [x for x in items if _dedupe_key(x) is None or id(x) in keep]


def merge_feeds(existing_items, new_items, now_ts=None):
    """기존 + 새 수집 병합 → 중복 제거 → 보존 기간 필터. (번역·정렬 전 목록)"""
    now_ts = int(time.time()) if now_ts is None else now_ts

    # 기존 데이터 보정 + id 기준 중복 제거 (루리웹 중복은 먼저 올라온 쪽 유지)
    feed_map = {}
    for item in existing_items:
        item = normalize_existing(item)
        if item is None:
            continue
        prev = feed_map.get(item["id"])
        if prev is None or item["timestamp"] < prev["timestamp"]:
            feed_map[item["id"]] = item

    # 새 데이터 병합 (최신 데이터 우선)
    sticky = set()  # 처음 본 시각을 유지하는 항목 중 이번에도 수집된 것 → 보존 기간이 지나도 목록에 있는 동안은 유지
    for item in new_items:
        if is_spam(item):
            continue
        # 새로 수집된 아이템도 category 정규화
        if isinstance(item.get("category"), (list, tuple)):
            item["category"] = item["category"][0] if item["category"] else "general"
        # 발매 예정 상품·날짜 미상 글처럼 날짜가 '지금'으로 찍히는 항목은 처음 수집한 시각을 유지 (매시간 맨 위로 올라오지 않도록)
        keep_first = item.pop("keep_first_seen", False)
        prev = feed_map.get(item["id"])
        if keep_first:
            sticky.add(item["id"])
            if prev:
                item["timestamp"] = prev.get("timestamp", item["timestamp"])
                item["created_at"] = prev.get("created_at", item["created_at"])
        feed_map[item["id"]] = item

    merged = dedupe_feeds(list(feed_map.values()))

    # [핵심 1] 최근 90일(약 3달) 이내 데이터 보관 (사용자가 앱에서 기간 필터 선택 가능)
    # 단, 처음 본 시각을 유지하는 항목은 아직 수집되는 동안 남겨 둠 (지웠다가 다음 실행에 '새 글'로 다시 올라오는 문제 방지)
    cutoff_timestamp = now_ts - RETENTION_DAYS * 86400
    return [
        item for item in merged
        if item.get("timestamp", 0) >= cutoff_timestamp or item["id"] in sticky
    ]


def main():
    print(f"[{datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')} KST] Starting Digimon feeds crawl...")

    # 기존 데이터를 먼저 읽음 — 손상됐으면 크롤링 전에 중단
    existing_items = load_existing_feeds()

    print(f"Crawling {len(SOURCES)} sources ({CRAWL_WORKERS} workers)...")
    all_new_items, source_status = crawl_all()
    report_source_health(source_status)

    print(f"Total fetched: {len(all_new_items)} items")

    filtered_feeds = merge_feeds(existing_items, all_new_items)

    translate_feed_titles(filtered_feeds)

    # [핵심 2] 최신순 내림차순 정렬 (가장 최신글이 index 0)
    filtered_feeds.sort(key=lambda x: x.get("timestamp", 0), reverse=True)

    result = {
        "last_updated": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "total_count": len(filtered_feeds),
        "retention_days": RETENTION_DAYS,
        # 소스별 이번 수집 결과 (건수·오류) — 0건이 이어지는 소스는 사이트 구조 변경 등으로 깨졌을 가능성
        "sources": source_status,
        "feeds": filtered_feeds
    }

    # public 및 docs 폴더에도 복사하여 GitHub Pages와 웹 프론트엔드가 바로 서빙할 수 있도록 함 (모두 원자적 쓰기)
    text = json.dumps(result, ensure_ascii=False, indent=2)
    for target in [FEEDS_FILE, PUBLIC_DATA_FILE, DOCS_DATA_FILE]:
        write_text_atomic(target, text)

    print(f"Successfully filtered (last {RETENTION_DAYS} days) & sorted newest-first: {len(filtered_feeds)} items saved.")


if __name__ == "__main__":
    main()
