#!/usr/bin/env python3
"""
grab_multi.py — crawl MULTIPLE real domain-list sites, merge + dedupe, save once.
Output:  All-Domain (<from>-<to>).txt   (no duplicate domains)

  python grab_multi.py 01/01/2012-01/01/2026                 # all sources below
  python grab_multi.py 01/01/2012-01/01/2026 --only ExpiredDomains,DomCop
  python grab_multi.py 01/01/2012-01/01/2026 --only GoDaddyAuctions   # needs Playwright
  python grab_multi.py 01/01/2012-01/01/2026 -o mylist.txt
"""
import argparse, asyncio, hashlib, re, sys, time
from urllib.parse import urlparse

try:
    import aiohttp
    from bs4 import BeautifulSoup
except ImportError:
    sys.exit("missing deps:  pip install aiohttp beautifulsoup4")

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
           "Accept": "text/html,*/*;q=0.8"}
DOMAIN_RE = re.compile(r"\b(?:(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,24})\b", re.I)

# ============================================================================
#  REAL SOURCES  (edit the `url` to paste your browser-filtered URL)
#  http=True  → plain-HTML table, works with this script
#  http=False → JS/SPA, needs the Playwright variant (script will warn if empty)
#  Apply the date/status filter IN THE BROWSER first, then copy the address bar
#  and paste it here — date-filter URL params are the fiddly part per site.
# ============================================================================
SOURCES = [
    # --- server-rendered: work right away ---
    dict(name="ExpiredDomains", http=True,  pageparam="pg",
         url="https://www.expireddomains.net/com/",
         note="biggest list. shows expiry + drop + age. filter by dates in UI, paste URL"),
    dict(name="DomCop",         http=True,  pageparam="pg",
         url="https://www.domcop.com/expired-domains/",
         note="has reg-date & expiry filters"),
    dict(name="SpamZilla",      http=True,  pageparam="page",
         url="https://www.spamzilla.net/expired-domains.php",
         note="reg-date filter available"),
    dict(name="SnapNames",      http=True,  pageparam="page",
         url="https://www.snapnames.com/expired/",
         note="expired auctions, server-rendered list"),

    # --- JS/SPA: flag=False so script warns instead of failing silently ---
    dict(name="GoDaddyAuctions",http=False, pageparam="page",
         url="https://auctions.godaddy.com/",
         note="JS SPA — use Playwright variant, or scrape its API endpoint"),
    dict(name="DropCatch",      http=False, pageparam="page",
         url="https://www.dropcatch.com/expired-domains",
         note="JS SPA — use Playwright variant"),
    dict(name="NameJet",        http=False, pageparam="page",
         url="https://www.namejet.com/expired",
         note="JS SPA — use Playwright variant"),
]


# ---------- range / url helpers ----------

def parse_range(raw):
    m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})\s*[-–—]\s*(\d{4})-(\d{1,2})-(\d{1,2})", raw.strip())
    if not m:
        m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})\s*[-–—]\s*(\d{1,2})/(\d{1,2})/(\d{4})", raw.strip())
        if not m:
            sys.exit(f"range must be 2012-01-01-2026-01-01 or 01/01/2012-01/01/2026 (got {raw!r})")
        d1, m1, y1, d2, m2, y2 = map(int, m.groups())
        return f"{y1:04d}-{m1:02d}-{d1:02d}", f"{y2:04d}-{m2:02d}-{d2:02d}"
    y1, mo1, d1, y2, mo2, d2 = map(int, m.groups())
    return f"{y1:04d}-{mo1:02d}-{d1:02d}", f"{y2:04d}-{mo2:02d}-{d2:02d}"


def make_page_url(base, page, pageparam):
    if "{page}" in base:
        return base.replace("{page}", str(page))
    m = re.search(r"[?&](page|pg|p|offset|start|page_num)=\d+", base)
    if m:
        return re.sub(r"([?&](?:page|pg|p|offset|start|page_num))=\d+", rf"\1={page}", base)
    if "?" in base:
        return f"{base}&{pageparam}={page}"
    return f"{base}?{pageparam}={page}"


def clean_domain(t):
    t = t.strip().lower().strip(".")
    t = t.removeprefix("http://").removeprefix("https://").split("/")[0].split(":")[0]
    return t if DOMAIN_RE.fullmatch(t) else None


def extract_domains(html):
    soup = BeautifulSoup(html, "html.parser")
    tokens = []
    for td in soup.find_all("td"):
        tokens.extend(DOMAIN_RE.findall(td.get_text(" ", strip=True)))
    if not soup.find("td"):
        for a in soup.find_all("a", href=True):
            tokens.extend(DOMAIN_RE.findall(a.get_text(" ", strip=True)))
    return tokens


class Dedup:
    def __init__(self):
        self.set, self.order, self.src = set(), [], {}
    def add(self, d, s):
        if d in self.set:
            return False
        self.set.add(d); self.order.append(d); self.src[d] = s
        return True
    @property
    def size(self):
        return len(self.set)


async def fetch(session, url, timeout, retries):
    for attempt in range(retries + 1):
        try:
            async with session.get(url, headers=HEADERS,
                                   timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                if r.status in (429, 500, 502, 503, 504) and attempt < retries:
                    await asyncio.sleep(2 * (2 ** attempt)); continue
                if r.status != 200:
                    return None, f"HTTP {r.status}"
                return await r.text(), None
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt < retries:
                await asyncio.sleep(2 * (2 ** attempt)); continue
            return None, "network error"
    return None, "failed"


async def crawl_source(src, args, dedup):
    name, base = src["name"], src["url"]
    st = {"pages": 0, "raw": 0, "new": 0, "js_warn": False}
    cap = args.pages if args.pages > 0 else 5000
    seen_pages, empty_streak, err_streak = set(), 0, 0

    async with aiohttp.ClientSession() as session:
        for page in range(1, cap + 1):
            url = make_page_url(base, page, src["pageparam"])
            html, err = await fetch(session, url, args.timeout, args.retries)

            if err:
                err_streak += 1
                if err_streak >= 3:
                    print(f"  [{name}] page {page}: {err} x3 → stop", file=sys.stderr)
                    break
                await asyncio.sleep(args.pause); continue
            err_streak = 0

            found = [d for d in (clean_domain(t) for t in extract_domains(html)) if d]
            st["raw"] += len(found); st["pages"] = page

            if found:
                sig = hashlib.md5("\n".join(sorted(found)).encode()).hexdigest()
                if sig in seen_pages:                      # identical page → loop guard
                    break
                seen_pages.add(sig)
                for d in found:
                    if dedup.add(d, name):
                        st["new"] += 1
                empty_streak = 0
            else:
                empty_streak += 1
                if empty_streak >= 2:
                    if not src["http"]:
                        st["js_warn"] = True
                    break

            print(f"\r  [{name:<16}] pg {page:<4} pg-domains {len(found):>5}   unique {dedup.size:>8}",
                  end="", flush=True)

            if args.max_domains and dedup.size >= args.max_domains:
                break
            await asyncio.sleep(args.pause)
    print()
    return st


async def main():
    p = argparse.ArgumentParser(description="Multi-source domain grabber (real sites, deduped).")
    p.add_argument("range", metavar="FROM-TO", help="e.g. 01/01/2012-01/01/2026")
    p.add_argument("--only", help="comma-list of source names to use (default: all)")
    p.add_argument("--list", action="store_true", help="print sources and exit")
    p.add_argument("-o", "--out", help="output file (default: All-Domain (<range>).txt)")
    p.add_argument("--pages", type=int, default=0, help="max pages per source (0 = until end, cap 5000)")
    p.add_argument("--max-domains", type=int, default=0, help="stop everything at N unique")
    p.add_argument("--sources", type=int, default=3, help="sources to run concurrently")
    p.add_argument("--pause", type=float, default=1.5, help="seconds between pages (per source)")
    p.add_argument("--timeout", type=float, default=30)
    p.add_argument("--retries", type=int, default=2)
    args = p.parse_args()

    if args.list:
        print(f"{'SOURCE':<16}{'HTTP':>5}  URL")
        for s in SOURCES:
            print(f"{s['name']:<16}{str(s['http']):>5}  {s['url']}")
            print(f"{'':16}     · {s['note']}")
        return

    if args.only:
        want = {x.strip() for x in args.only.split(",")}
        srcs = [s for s in SOURCES if s["name"] in want]
        if not srcs:
            sys.exit(f"no sources match --only {args.only!r} — see --list")
    else:
        srcs = SOURCES

    frm, to = parse_range(args.range)
    safe = re.sub(r"\s+", " ", re.sub(r"[^\w.\-() ]", "-", args.range.strip()))
    out_path = args.out or f"All-Domain ({safe}).txt"

    print(f"range    : {frm} → {to}")
    print(f"sources  : {len(srcs)}   (concurrent={args.sources}, pause={args.pause}s)")
    for s in srcs:
        flag = "" if s["http"] else "  ⚠ JS/SPA"
        print(f"  · {s['name']:<16} {s['url']}{flag}")
    print("-" * 64)

    dedup = Dedup()
    sem = asyncio.Semaphore(args.sources)

    async def run(s):
        async with sem:
            await crawl_source(s, args, dedup)

    t0 = time.monotonic()
    await asyncio.gather(*[run(s) for s in srcs])

    order = dedup.order
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(order) + ("\n" if order else ""))

    print("-" * 64)
    if not order:
        print("⚠ 0 domains written. Sources likely JS-rendered or date-filter URL wrong.")
        print("  Fix: open each site, set the date filter in the UI, copy the address bar,")
        print("  and paste it into the `url=` field for that source. JS sites need Playwright.")
    print(f"unique saved : {dedup.size}")
    print(f"elapsed      : {time.monotonic() - t0:.0f}s")
    print(f"output       : {out_path}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\naborted", file=sys.stderr); sys.exit(130)
