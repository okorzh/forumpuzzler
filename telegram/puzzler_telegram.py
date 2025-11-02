#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scrape forum.puzzler.su for posts from today/yesterday and post them to a Telegram channel.

Usage:
  export TG_TOKEN="8353725143:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
  export TG_CHAT_ID="@forumpuzzler"
  python3 puzzler_telegram.py

What it does:
  - Crawls several "recent" phpBB pages (active topics, new posts, unread, homepage)
  - Detects posts marked "Сегодня" / "Вчера" or with <time datetime="..."> on those dates
  - Sends each new link to Telegram via sendMessage
  - Remembers what it has posted in .puzzler_sent.json to avoid duplicates

Puzzler.su -> Telegram
- Posts ONLY items within last WITHIN_HOURS that have >=1 image >= 200x200.
- Sends up to MAX_POSTS_TO_SEND most recent.
- Confirms timestamp from the post's *permalink* (#pNNNN).
- Robust de-dup (topic canonicalization + post permalink uniqueness).
- State file now stores timestamps and EXPIRES old entries automatically.

Env options:
  TG_TOKEN                (required)
  TG_CHAT_ID              default: @forumpuzzler
  MAX_POSTS_TO_SEND       default: 25
  WITHIN_HOURS            default: 2
  MIN_IMG_W, MIN_IMG_H    default: 200, 200
  STATE_FILE              default: .puzzler_sent.json
  STATE_TTL_DAYS          default: 7   (expire "already-sent" entries after N days)
  FORCE_RESEND_HOURS      default: 0   (if >0, allow resend if last sent older than this)
  CLEAR_STATE             if "1", wipe the state at start
  DEBUG_LIST              if "1", print which candidates were skipped due to state
"""

import os, re, json, time, html, logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse, parse_qs, urlencode

import requests
from bs4 import BeautifulSoup
from PIL import ImageFile

# ---- Config ----
BASE = "https://forum.puzzler.su/"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; puzzler-scraper/10.0)", "Accept-Language": "ru,en;q=0.9"}

MAX_POSTS_TO_SEND = int(os.getenv("MAX_POSTS_TO_SEND", "25"))
WITHIN_HOURS = int(os.getenv("WITHIN_HOURS", "2"))
MIN_IMG_W = int(os.getenv("MIN_IMG_W", "200"))
MIN_IMG_H = int(os.getenv("MIN_IMG_H", "200"))
STATE_FILE = os.getenv("STATE_FILE", ".puzzler_sent.json")
STATE_TTL_DAYS = int(os.getenv("STATE_TTL_DAYS", "7"))
FORCE_RESEND_HOURS = int(os.getenv("FORCE_RESEND_HOURS", "0"))
CLEAR_STATE = os.getenv("CLEAR_STATE") == "1"
DEBUG_LIST = os.getenv("DEBUG_LIST") == "1"

ASSUMED_TZ = timezone(timedelta(hours=3))  # Moscow time

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ---- RU month maps ----
RU_MONTHS = {
    "янв":1,"фев":2,"мар":3,"апр":4,"май":5,"мая":5,"июн":6,"июл":7,"авг":8,"сен":9,"сент":9,"окт":10,"ноя":11,"дек":12,
    "января":1,"февраля":2,"марта":3,"апреля":4,"июня":6,"июля":7,"августа":8,"сентября":9,"октября":10,"ноября":11,"декабря":12,
}

# ---- State (with TTL + back-compat) ----
def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def load_state(path=STATE_FILE):
    if CLEAR_STATE:
        logging.info("CLEAR_STATE=1 -> starting with empty state.")
        return {"sent": {}}  # url -> {"last_sent": iso}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        # old format fallback
        return {"sent": {}}

    # Back-compat: old list -> convert to dict with ancient timestamps
    if isinstance(data, dict) and "sent_urls" in data and isinstance(data["sent_urls"], list):
        converted = {u: {"last_sent": "1970-01-01T00:00:00Z"} for u in data["sent_urls"]}
        return {"sent": converted}
    if isinstance(data, dict) and "sent" in data and isinstance(data["sent"], dict):
        return data

    # anything else -> reset
    return {"sent": {}}

def save_state(state, path=STATE_FILE):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def prune_state(state):
    """Remove entries older than STATE_TTL_DAYS."""
    if STATE_TTL_DAYS <= 0:
        return
    cutoff = datetime.now(timezone.utc) - timedelta(days=STATE_TTL_DAYS)
    sent = state.get("sent", {})
    to_del = []
    for url, meta in sent.items():
        try:
            dt = datetime.fromisoformat(meta.get("last_sent", "1970-01-01T00:00:00Z").replace("Z", "+00:00"))
        except Exception:
            dt = datetime(1970,1,1, tzinfo=timezone.utc)
        if dt < cutoff:
            to_del.append(url)
    for u in to_del:
        sent.pop(u, None)
    if to_del:
        logging.info("Pruned %d expired entries from state (TTL=%d days).", len(to_del), STATE_TTL_DAYS)

def already_sent(state, url):
    """Return True if URL was sent and still within FORCE_RESEND_HOURS (if set)."""
    sent = state.get("sent", {})
    if url not in sent:
        return False
    if FORCE_RESEND_HOURS <= 0:
        return True
    try:
        last = datetime.fromisoformat(sent[url]["last_sent"].replace("Z", "+00:00"))
    except Exception:
        return True
    return (datetime.now(timezone.utc) - last) < timedelta(hours=FORCE_RESEND_HOURS)

def mark_sent(state, url):
    state.setdefault("sent", {})[url] = {"last_sent": _now_iso()}

# ---- HTTP ----
def mk_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    try:
        from urllib3.util.retry import Retry
        from requests.adapters import HTTPAdapter
        retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429,500,502,503,504])
        s.mount("https://", HTTPAdapter(max_retries=retry))
        s.mount("http://", HTTPAdapter(max_retries=retry))
    except Exception:
        pass
    return s

def fetch(session, url_or_path):
    full = url_or_path if url_or_path.startswith("http") else urljoin(BASE, url_or_path)
    r = session.get(full, timeout=25)
    r.raise_for_status()
    return full, r.text

# ---- URL normalization ----
_UNSTABLE_QUERY_KEYS = {"sid","view","start","hilit","st","p"}

def canonical_topic_url(url: str) -> str:
    pu = urlparse(url)
    qs = parse_qs(pu.query, keep_blank_values=True)
    f = qs.get("f", [None])[-1]
    t = qs.get("t", [None])[-1]
    if not f or not t:
        return pu.path
    return f"{pu.path}?{urlencode({'f': f, 't': t})}"

def strip_unstable_params(url: str) -> str:
    pu = urlparse(url)
    qs = parse_qs(pu.query, keep_blank_values=True)
    for k in list(qs.keys()):
        if k in _UNSTABLE_QUERY_KEYS:
            qs.pop(k, None)
    new_q = urlencode({k: v[-1] for k, v in qs.items()}) if qs else ""
    return pu._replace(query=new_q).geturl()

# ---- Date/time parsing ----
def parse_ru_datetime(text):
    t = (text or "").replace("\xa0", " ").strip()

    m = re.search(r"\b(\d{1,2})\.(\d{1,2})\.(\d{2,4})(?:[ ,]+(\d{1,2}):(\d{2}))?\b", t)
    if m:
        d, mth, y = map(int, m.group(1,2,3))
        if y < 100: y += 2000
        hh = int(m.group(4)) if m.group(4) else 0
        mm = int(m.group(5)) if m.group(5) else 0
        try: return datetime(y, mth, d, hh, mm)
        except ValueError: return None

    m = re.search(r"\b(\d{1,2})\s+([А-Яа-яA-Za-z\.]+)\s+(\d{2,4})(?:[ ,]+(\d{1,2}):(\d{2}))?\b", t)
    if m:
        d = int(m.group(1))
        mon_txt = m.group(2).strip(".").lower()
        mon_key = mon_txt[:4] if mon_txt.startswith("сент") else mon_txt[:3]
        mon = RU_MONTHS.get(mon_key) or RU_MONTHS.get(mon_txt)
        y = int(m.group(3));  y += 2000 if y < 100 else 0
        hh = int(m.group(4)) if m.group(4) else 0
        mm = int(m.group(5)) if m.group(5) else 0
        if mon:
            try: return datetime(y, mon, d, hh, mm)
            except ValueError: return None
    return None

def parse_post_datetime_from_container(post):
    t = post.find("time")
    if t:
        for attr in ("datetime","title"):
            val = t.get(attr)
            if val:
                try:
                    dt = datetime.fromisoformat(val.replace("Z","+00:00"))
                    if dt.tzinfo is None: dt = dt.replace(tzinfo=ASSUMED_TZ)
                    return dt.astimezone(ASSUMED_TZ)
                except Exception:
                    pass

    ab = post.find(["abbr","span"], attrs={"title": True})
    if ab and ab.get("title"):
        try:
            dt = datetime.fromisoformat(ab["title"].replace("Z","+00:00"))
            if dt.tzinfo is None: dt = dt.replace(tzinfo=ASSUMED_TZ)
            return dt.astimezone(ASSUMED_TZ)
        except Exception:
            pass

    header_nodes = []
    for sel in [".author",".postauthor",".postbody .author",".post .author",".postprofile",".postinfo"]:
        header_nodes.extend(post.select(sel))
    if not header_nodes:
        header_nodes = post.find_all(["p","div","span"], limit=6)

    rx_prefix = re.compile(r"(Добавлено|Отправлено|Опубликовано)\s*[:\-–]\s*(.+)", re.IGNORECASE)
    for node in header_nodes:
        txt = node.get_text(" ", strip=True)
        m = rx_prefix.search(txt)
        if m:
            g = parse_ru_datetime(m.group(2))
            if g: return g.replace(tzinfo=ASSUMED_TZ)

    rx_soob = re.compile(r"Сообщение.*?»\s*([^\|]+)$")
    for node in header_nodes:
        txt = node.get_text(" ", strip=True)
        m = rx_soob.search(txt)
        if m:
            g = parse_ru_datetime(m.group(1))
            if g: return g.replace(tzinfo=ASSUMED_TZ)

    head = post.get_text(" ", strip=True)[:240]
    g = parse_ru_datetime(head)
    if g: return g.replace(tzinfo=ASSUMED_TZ)
    return None

# ---- Topic traversal & images ----
def extract_topic_links_from_listing(html_text, base_url):
    soup = BeautifulSoup(html_text, "html.parser")
    topics = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "viewtopic.php" in href:
            topics.add(strip_unstable_params(urljoin(base_url, href)))
    return sorted(topics)

def find_last_page_url(soup, current_url):
    a_last = soup.find("a", attrs={"rel": "last"}, href=True)
    if a_last:
        return urljoin(current_url, a_last["href"])
    max_start, candidate = -1, None
    for a in soup.find_all("a", attrs={"role":"button"}, href=True):
        m = re.search(r"[?&]start=(\d+)", a["href"])
        if m:
            st = int(m.group(1))
            if st > max_start:
                max_start, candidate = st, a["href"]
    return urljoin(current_url, candidate) if candidate else current_url

def build_permalink(page_url, post_id):
    pu = urlparse(page_url)
    qs = parse_qs(pu.query, keep_blank_values=True)
    for k in list(qs.keys()):
        if k in _UNSTABLE_QUERY_KEYS:
            qs.pop(k, None)
    qs["p"] = [post_id]
    new_q = urlencode({k: v[-1] for k, v in qs.items()})
    return pu._replace(query=new_q, fragment=f"p{post_id}").geturl()

def iter_post_image_urls(node, base_url):
    """Yield image candidates from an inline post body (topic page or permalink container)."""
    body = node.select_one(".content") or node
    # <img ...>
    for img in body.find_all("img") :
        src = img.get("src") or img.get("data-src")
        if not src:
            srcset = img.get("srcset")
            if srcset:
                first = srcset.split(",")[0].strip().split(" ")[0]
                src = first
        if src:
            yield src if src.startswith("http") else urljoin(base_url, src)
    # linked images in content
    for a in body.find_all("a", href=True):
        href = a["href"]
        if re.search(r"\.(?:jpe?g|png|gif|webp)(?:\?|#|$)", href, flags=re.I):
            yield href if href.startswith("http") else urljoin(base_url, href)
    
    att = node.select_one(".attachbox") or node
    # <img ...>
    for img in att.find_all("img") :
        src = img.get("src") or img.get("data-src")
        if not src:
            srcset = img.get("srcset")
            if srcset:
                first = srcset.split(",")[0].strip().split(" ")[0]
                src = first
        if src:
            yield src if src.startswith("http") else urljoin(base_url, src)

def image_size_at_least(session, url, min_w=MIN_IMG_W, min_h=MIN_IMG_H, max_bytes=4*1024*1024):
    try:
        r = session.get(url, stream=True, timeout=25)
        r.raise_for_status()
        parser = ImageFile.Parser()
        read = 0
        for chunk in r.iter_content(10240):
            if not chunk: break
            parser.feed(chunk); read += len(chunk)
            if parser.image:
                w, h = parser.image.size
                return (w >= min_w) and (h >= min_h)
            if read >= max_bytes: break
    except Exception:
        return False
    return False

def find_first_eligible_image(session, post, page_url, min_w=MIN_IMG_W, min_h=MIN_IMG_H):
    for url in iter_post_image_urls(post, page_url):
        if image_size_at_least(session, url, min_w, min_h):
            return url
    return None

def extract_posts_from_topic_page(session, html_text, page_url):
    soup = BeautifulSoup(html_text, "html.parser")
    results = []
    topic_key = canonical_topic_url(page_url)
    for post in soup.select("[id^=p]"):
        post_id = None
        id_attr = post.get("id")
        if id_attr and id_attr.startswith("p") and id_attr[1:].isdigit():
            post_id = id_attr[1:]
        if not post_id:
            a = post.find("a", href=True)
            if a and "#p" in a.get("href",""):
                m = re.search(r"#p(\d+)", a["href"])
                if m: post_id = m.group(1)
        if not post_id:
            continue

        permalink = build_permalink(page_url, post_id)

        title = None
        h = post.find(["h2","h3"])
        if h: title = h.get_text(" ", strip=True)
        if not title:
            body = post.select_one(".content") or post
            snippet = body.get_text(" ", strip=True)
            title = (snippet[:120] + "…") if len(snippet) > 120 else snippet

        eligible_img = find_first_eligible_image(session, post, page_url)
        results.append({"permalink": permalink, "title": title or "Сообщение", "eligible_img": eligible_img, "topic_key": topic_key,})
    return results

def extract_dt_from_permalink(session, permalink):
    full, html_text = fetch(session, permalink)
    soup = BeautifulSoup(html_text, "html.parser")
    m = re.search(r"#p(\d+)$", permalink)
    container = soup.find(id=f"p{m.group(1)}") if m else soup.select_one("[id^=p]")
    if not container:
        return None
    dt = parse_post_datetime_from_container(container)
    return dt.astimezone(ASSUMED_TZ) if dt else None

# ---- Crawl -> confirm -> filter (with better de-dup + debug) ----
def collect_recent_posts(session, within_hours=WITHIN_HOURS):
    cutoff = datetime.now(ASSUMED_TZ) - timedelta(hours=within_hours)

    # Gather topics
    raw_topic_urls = set()

    LISTING_PATHS = [
        "search.php?search_id=newposts",
        "search.php?search_id=active_topics",
        "search.php?search_id=unreadposts",
        "search.php?search_id=unanswered",
    ]
    EXCLUDE_LIST = [39,173,85,25,89,100,242,315,25,90,29,81,127,171,205,229,237,274,
        302,301,345,346,96,101,102,105,106,109,111,158,182,134,86,55,294,116,117,110,
        150,192,218,234,253,299,312,324,343,356,361,370,376,337,371,372,373,374,375,
        364,365,366,367,357,358,359,360,338,339,340,341,342,332,333,334,335,336,
        326,237,328,329,330,316,317,318,319,320,304,305,306,307,308,309,310,
        285,290,288,287,289,291,21,152,136,148,193,225,233,255,293,313,322,331,344,]

    # Forum range that can have new puzzles (as of 1 Nov 2025 last forum numer 381)
    for i in range(7,420):
        # Excluding forums that are not expected to have new puzzles
        if i not in EXCLUDE_LIST:
            LISTING_PATHS += ["viewforum.php?f=" + str(i)]

    for path in LISTING_PATHS:
        try:
            full, html_text = fetch(session, urljoin(BASE, path))
            logging.info("Fetched %s", full)
            raw_topic_urls.update(extract_topic_links_from_listing(html_text, full))
        except Exception as e:
            logging.warning("Listing fetch failed for %s: %s", path or "/", e)

    topic_map = {}
    for u in raw_topic_urls:
        key = canonical_topic_url(u)
        topic_map.setdefault(key, u)

    candidates_by_permalink = {}
    logged = set()

    for key, topic_url in sorted(topic_map.items()):
        try:
            logging.info("topic_url = %s", topic_url)
            t_url, t_html = fetch(session, topic_url)
            t_soup = BeautifulSoup(t_html, "html.parser")
            last_url = find_last_page_url(t_soup, t_url)

            if last_url != t_url:
                _, last_html = fetch(session, last_url)
                page_url, page_soup = last_url, BeautifulSoup(last_html, "html.parser")
            else:
                page_url, page_soup = t_url, t_soup

            posts = extract_posts_from_topic_page(session, str(page_soup), page_url)
            for p in posts:
                if not p.get("eligible_img"):
                    continue
                permalink = p["permalink"]
                if permalink in candidates_by_permalink:
                    continue
                dt = extract_dt_from_permalink(session, permalink)
                if dt and dt >= cutoff:
                    p["dt"] = dt
                    candidates_by_permalink[permalink] = p
                    if permalink not in logged:
                        logging.info("Recent+HasImage: %s | %s", dt.isoformat(), permalink)
                        logged.add(permalink)
        except Exception as e:
            logging.warning("Topic parse failed for %s: %s", topic_url, e)

    candidates = sorted(candidates_by_permalink.values(), key=lambda x: x["dt"], reverse=True)

    return candidates

    # One per topic currently not running. Previously used to have one port on topic change
    #from collections import defaultdict

    #by_topic = defaultdict(list)
    #for p in candidates:
    #    key = p.get("topic_key") or canonical_topic_url(p["permalink"])
    #    by_topic[key].append(p)

    #eligible_one_per_topic = []
    #for key, arr in by_topic.items():
    #    # pick the first new post in this thread
    #    best = min(arr, key=lambda x: x["dt"])
    #    eligible_one_per_topic.append(best)

    # Sort newest-first for sending
    #eligible_one_per_topic.sort(key=lambda x: x["dt"], reverse=True)
    #return eligible_one_per_topic

# ---- Telegram ----
def telegram_send_photo(token, chat_id, photo_url, title, url):
    api = f"https://api.telegram.org/bot{token}/sendPhoto"
    caption = f"🧩 <b>{html.escape(title, quote=False)}</b>\n{url}"
    data = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML", "disable_notification": True, "photo": photo_url}
    r = requests.post(api, data=data, timeout=25)
    if r.status_code == 429:
        retry = r.json().get("parameters", {}).get("retry_after", 3); time.sleep(retry)
        r = requests.post(api, data=data, timeout=25)
    r.raise_for_status()
    return r.json()

# ---- Main ----
def main():
    token = os.getenv("TG_TOKEN")
    chat_id = os.getenv("TG_CHAT_ID", "@forumpuzzler")
    if not token:
        raise SystemExit("Please set TG_TOKEN environment variable.")

    state = load_state()
    prune_state(state)  # expire old entries

    session = mk_session()
    posts = collect_recent_posts(session, within_hours=WITHIN_HOURS)

    if not posts:
        logging.info("No qualifying posts (within %dh and with >=%dx%d image).", WITHIN_HOURS, MIN_IMG_W, MIN_IMG_H)
        save_state(state)
        return

    # Debug: show how many are blocked by state
    blocked = [p for p in posts if already_sent(state, p["permalink"])]
    if DEBUG_LIST and blocked:
        logging.info("DEBUG_LIST=1 -> %d candidate(s) skipped due to state:", len(blocked))
        for b in blocked[:10]:
            logging.info("  blocked: %s  (dt=%s)", b["permalink"], b["dt"].isoformat())

    sent = 0
    for info in posts:
        if sent >= MAX_POSTS_TO_SEND:
            break
        url = info["permalink"]
        if already_sent(state, url):
            continue
        try:
            telegram_send_photo(token, chat_id, info["eligible_img"], info["title"], url)
            mark_sent(state, url)
            sent += 1
            time.sleep(1.0)
        except Exception as e:
            logging.error("Failed to send %s: %s", url, e)

    if sent == 0:
        logging.info("Nothing new to send (top %d already posted).", MAX_POSTS_TO_SEND)
    else:
        logging.info("Done. Posted %d new item(s).", sent)

    save_state(state)

if __name__ == "__main__":
    main()
