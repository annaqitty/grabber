#!/usr/bin/env python3
"""
grab_pages.py — walk a paginated domain-list, grab every domain on every page.
Writes everything (deduped) to  All-Domain (<from>-<to>).txt

  python grab_pages.py 01/01/2012-01/01/2026 -u "https://example.com/list?reg=..."
  python grab_pages.py 01/01/2012-01/01/2026 -u URL --pages 500
"""
import argparse, asyncio, re, sys
from urllib.parse import urljoin, quote

try:
    import aiohttp
    from bs4 import BeautifulSoup
except ImportError:
    sys.exit("missing deps:  pip install aiohttp beautifulsoup4")

UA = "Mozilla/5.0 (compatible; domain-grab/1.0)"
HEADERS = {"User-Agent": UA, "Accept": "text/html,*/*;q=0.8"}

# a bare domain, for filtering table cells / links
DOMAIN_RE = re.compile(
    r"\b(?:(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,24})\b", re.I)


def parse_range(raw):
    m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})\s*[-–—]\s*(\d{4})-(\d{1,2})-(\d{1,2})",
                     raw.strip())
    if not m:
        # allow slash dates too
        m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})\s*[-–—]\s*(\d{1,2})/(\d{1,2})/(\d{4})",
                         raw.strip())
        if not m:
            sys.exit(f"range must be 2012-01-01-2026-01-01 (got {raw!r})")
        d1, m1, y1, d2, m2, y2 = map(int, m.groups())
        return f"{y1:04d}-{m1:02d}-{d1:02d}", f"{y2:04d}-{m2:02d}-{d2:02d}"
    y1, mo1, d1, y2, mo2, d2 = map(int, m.groups())
    return f"{y1:04d}-{mo1:02d}-{d1:02d}", f"{y2:04d}-{mo2:02d}-{d2:02d}"


def make_page_url(base, page):
    """Insert/replace a ?page=N or &page=N param. Falls back to appending ?page=N."""
    u = base
    m = re.search(r"[?&]page?=\d+", u)
    if m:
        u = re.sub(r"([?&]page?)=\d+", rf"\1={page}", u)
    elif "?" in u:
        u += f"&page={page}"
    else:
        u += f"?page={page}"
    return u


def extract_domains(html):
    """Pull every domain-looking token from table cells and links (order kept, dedup later)."""
    soup = BeautifulSoup(html, "html.parser")
    tokens = []
    # 1) table cells (most list sites put the domain in a <td>)
    for td in soup.find_all("td"):
        tokens.extend(DOMAIN_RE.findall(td.get_text(" ", strip=True)))
    # 2) if the page has no <td>, fall back to link text
    if not soup.find("td"):
        for a in soup.find_all("a", href=True):
            tokens.extend(DOMAIN_RE.findall(a.get_text(" ", strip=True)))
    return tokens


def clean_domain(t):
    t = t.strip().lower().strip(".")
    t = t.removeprefix("http://").removeprefix("https://").split("/")[0].split(":")[0]
    return t if DOMAIN_RE.fullmatch(t) else None


async def fetch(session, url, timeout, retries):
    for attempt in range(retries + 1):
        try:
            async with session.get(url, headers=HEADERS,
                                   timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                if r.status in (429, 500, 502, 503, 504) and attempt < retries:
                    await asyncio.sleep(2 * (2 ** attempt)); continue
                if r.status != 200:
                    return None, r.status
                return await r.text(), None
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt < retries:
                await asyncio.sleep(2 * (2 ** attempt)); continue
            return None, "network error"
    return None, "failed"


async def main():
    p = argparse.ArgumentParser(description="Page-by-page domain list grabber.")
    p.add_argument("range", metavar="FROM-TO", help="e.g. 01/01/2012-01/01/2026 or 2012-01-01-2026-01-01")
    p.add_argument("-u", "--url", required=True, help="base listing URL (page 1)")
    p.add_argument("-o", "--out", help="output file (default: All-Domain (<from>-<to>).txt)")
    p.add_argument("--pages", type=int, default=0, help="max pages to walk (0 = until empty/dup, cap 5000)")
    p.add_argument("--pause", type=float, default=1.5, help="seconds between pages (be polite)")
    p.add_argument("--timeout", type=float, default=30)
    p.add_argument("--retries", type=int, default=2)
    args = p.parse_args()

    frm, to = parse_range(args.range)
    cap = args.pages if args.pages > 0 else 5000
    cap = min(cap, 5000)

    safe = re.sub(r"\s+", " ", re.sub(r"[^\w.\-() ]", "-", args.range.strip()))
    out_path = args.out or f"All-Domain ({safe}).txt"

    print(f"range   : {frm} → {to}")
    print(f"base    : {args.url}")
    print(f"walking up to {cap} pages ...\n")

    seen, order = set(), []
    empty_streak = 0
    t0 = asyncio.get_event_loop().time()

    async with aiohttp.ClientSession() as session:
        for page in range(1, cap + 1):
            url = make_page_url(args.url, page)
            html, err = await fetch(session, url, args.timeout, args.retries)
            if err:
                print(f"  page {page:<5} {err} — skipping", file=sys.stderr)
                await asyncio.sleep(args.pause); continue

            found = [d for d in (clean_domain(t) for t in extract_domains(html)) if d]
            new = [d for d in found if d not in seen]
            for d in new:
                seen.add(d); order.append(d)

            # stop conditions
            if len(found) == 0:
                empty_streak += 1
                if empty_streak >= 2:
                    print(f"  page {page:<5} empty x2 → assuming end of list")
                    break
            else:
                empty_streak = 0
            if len(new) == 0 and len(found) > 0 and page > 1:
                # same content as a previous page → we've looped / hit a duplicate page
                print(f"  page {page:<5} duplicate content → stopping")
                break

            elapsed = asyncio.get_event_loop().time() - t0
            print(f"\r  page {page:<5} this-page {len(found):>5}   total unique {len(order):>7}   {elapsed:.0f}s",
                  end="", flush=True)
            await asyncio.sleep(args.pause)

    print(f"\n\n{len(order)} unique domains written → {out_path}")
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(order) + ("\n" if order else ""))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\naborted", file=sys.stderr); sys.exit(130)
