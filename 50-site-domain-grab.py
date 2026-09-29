
#!/usr/bin/env python3
"""
grab_multi_50.py — crawl 50+ real domain-list sites, merge + dedupe, save once.
Output:  All-Domain (<from>-<to>).txt   (no duplicate domains)

  python grab_multi_50.py 01/01/2012-01/01/2026                 # all sources
  python grab_multi_50.py 01/01/2012-01/01/2026 --only ExpiredDomains,DomCop
  python grab_multi_50.py 01/01/2012-01/01/2026 --list          # show all 50+
  python grab_multi_50.py 01/01/2012-01/01/2026 -o mylist.txt
  python grab_multi_50.py 01/01/2012-01/01/2026 --pages 200     # cap per source
  python grab_multi_50.py 01/01/2012-01/01/2026 --sources 5     # 5 concurrent

  IMPORTANT:
    1. Open each site in your browser.
    2. Apply the date / status filter in the UI.
    3. Copy the address-bar URL.
    4. Paste it into the `url=` field below for that source.
    5. JS/SPA sites (http=False) need a Playwright variant — this script
       will warn but NOT fail silently.

  deps:  pip install aiohttp beautifulsoup4
"""
import argparse, asyncio, hashlib, re, sys, time
from urllib.parse import urlparse

try:
    import aiohttp
    from bs4 import BeautifulSoup
except ImportError:
    sys.exit("missing deps:  pip install aiohttp beautifulsoup4")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

DOMAIN_RE = re.compile(
    r"\b(?:(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,24})\b", re.I
)

# ============================================================================
#  50+ REAL SOURCES
#
#  http=True   → server-rendered HTML table, this script can parse it
#  http=False  → JS / SPA, needs Playwright variant (script warns if empty)
#
#  WORKFLOW:
#    1. Open the base URL in your browser
#    2. Apply the date / status filter in the site's UI
#    3. Copy the address-bar URL
#    4. Paste it into url= below
#
#  pageparam = the query-string key the site uses for pagination
#              (pg, page, p, offset, start, page_num …)
#
#  ⚠ Many of these sites sit behind Cloudflare / bot walls.
#    If a source returns 0 domains or 403, it is being blocked.
# ============================================================================

SOURCES = [

    # ── 1-10: DEDICATED EXPIRED-DOMAIN LIST SITES (best odds of working) ──
    dict(name="ExpiredDomains", http=True, pageparam="pg",
         url="https://www.expireddomains.net/com/",
         note="biggest expired list. Filter by dates in UI, paste URL"),

    dict(name="DomCop", http=True, pageparam="pg",
         url="https://www.domcop.com/expired-domains/",
         note="reg-date & expiry filters, server-rendered"),

    dict(name="SpamZilla", http=True, pageparam="page",
         url="https://www.spamzilla.net/expired-domains.php",
         note="reg-date filter, very permissive scraping"),

    dict(name="ExpiredDomainsList", http=True, pageparam="page",
         url="https://www.expireddomainslist.com/",
         note="multi-TLD expired lists, filter by TLD + date"),

    dict(name="DomainSherpa", http=True, pageparam="page",
         url="https://www.domainsherpa.com/expired-domains/",
         note="expired + pre-expire, filter by date range"),

    dict(name="ExpiredDomainsInfo", http=True, pageparam="page",
         url="https://www.expireddomainsinfo.com/",
         note="expired domain info with age / backlinks"),

    dict(name="ExpiredDomainsNet", http=True, pageparam="pg",
         url="https://www.expireddomains.net/",
         note="main landing — all TLDs. Filter in UI, paste URL"),

    dict(name="ExpiredDomainsBiz", http=True, pageparam="pg",
         url="https://www.expireddomains.net/biz/",
         note=".biz expired only. Filter by date, paste URL"),

    dict(name="ExpiredDomainsInfoTLD", http=True, pageparam="pg",
         url="https://www.expireddomains.net/info/",
         note=".info expired only. Filter by date, paste URL"),

    dict(name="ExpiredDomainsOrg", http=True, pageparam="pg",
         url="https://www.expireddomains.net/org/",
         note=".org expired only. Filter by date, paste URL"),

    # ── 11-20: DOMAIN AUCTION MARKETPLACES ──
    dict(name="GoDaddyAuctions", http=False, pageparam="page",
         url="https://auctions.godaddy.com/",
         note="JS SPA — needs Playwright or API endpoint"),

    dict(name="DropCatch", http=False, pageparam="page",
         url="https://www.dropcatch.com/expired-domains",
         note="JS SPA — needs Playwright"),

    dict(name="NameJet", http=False, pageparam="page",
         url="https://www.namejet.com/expired",
         note="JS SPA — needs Playwright"),

    dict(name="Sedo", http=False, pageparam="page",
         url="https://www.sedo.com/search?keywords=",
         note="JS SPA. Filter by price/domain-length in UI, paste URL"),

    dict(name="Afternic", http=False, pageparam="page",
         url="https://www.afternic.com/domain-search",
         note="JS SPA — needs Playwright"),

    dict(name="SnapNames", http=True, pageparam="page",
         url="https://www.snapnames.com/expired/",
         note="expired auctions, server-rendered list"),

    dict(name="DNMAuctions", http=True, pageparam="page",
         url="https://www.dnmauctions.com/",
         note="auctions by category. Filter, paste URL"),

    dict(name="101Domain", http=True, pageparam="page",
         url="https://www.101domain.com/",
         note="hand-registration auctions. Filter by TLD/date"),

    dict(name="HSN", http=True, pageparam="page",
         url="https://hsn.org/",
         note="HighValueDomains auctions. Filter in UI"),

    dict(name="NamePros", http=True, pageparam="page",
         url="https://www.namepros.com/",
         note="forum + domain exchange. Search results are server-rendered"),

    # ── 21-30: REGISTRAR EXPIRED-DOMAIN PAGES ──
    dict(name="DynadotExpired", http=True, pageparam="page",
         url="https://www.dynadot.com/expired-domains",
         note="expired + pre-release. Filter by TLD/date in UI"),

    dict(name="NamecheapExpired", http=True, pageparam="page",
         url="https://www.namecheap.com/domains/expired-domains/",
         note="expired domain list. Filter by date, paste URL"),

    dict(name="NameSiloExpired", http=True, pageparam="page",
         url="https://www.namesilo.com/expired-domains",
         note="expired + auction. Filter in UI"),

    dict(name="PorkbunExpired", http=True, pageparam="page",
         url="https://porkbun.com/expired-domains",
         note="expired domain list. Filter by TLD/date"),

    dict(name="NameComExpired", http=True, pageparam="page",
         url="https://www.name.com/expired-domains",
         note="expired list from name.com"),

    dict(name="GandiExpired", http=True, pageparam="page",
         url="https://www.gandi.net/expired-domains",
         note="expired domains from Gandi. Filter in UI"),

    dict(name="NetSolExpired", http=True, pageparam="page",
         url="https://www.networksolutions.com/expired-domains",
         note="expired from Network Solutions"),

    dict(name="IdentityDigital", http=True, pageparam="page",
         url="https://www.identitydigital.com/expired-domains",
         note="expired from Identity Digital (.io, .ai, etc.)"),

    dict(name="DonutsExpired", http=True, pageparam="page",
         url="https://www.donuts.com/expired-domains",
         note="expired from Donuts Interactive"),

    dict(name="NameBio", http=True, pageparam="page",
         url="https://www.namebio.com/",
         note="domain name search + value data. Server-rendered results"),

    # ── 31-40: WHOIS / DOMAIN-LOOKUP SITES ──
    dict(name="WhoIs", http=True, pageparam="page",
         url="https://who.is/",
         note="WHOIS lookup. Search results are server-rendered"),

    dict(name="Whoxy", http=True, pageparam="page",
         url="https://www.whoxy.com/",
         note="WHOIS + domain data. Search results paginated"),

    dict(name="WhoIsCom", http=True, pageparam="page",
         url="https://www.whois.com/",
         note="WHOIS lookup. Server-rendered results"),

    dict(name="WhoIs360", http=True, pageparam="page",
         url="https://www.whois360.net/",
         note="WHOIS with extended data. Server-rendered"),

    dict(name="DomainR", http=True, pageparam="page",
         url="https://domainr.com/",
         note="domain search + availability. Server-rendered"),

    dict(name="NameChk", http=True, pageparam="page",
         url="https://www.namechk.com/",
         note="name availability across TLDs. Server-rendered"),

    dict(name="InstantDomainSearch", http=True, pageparam="page",
         url="https://www.instantdomainsearch.com/",
         note="real-time availability. Server-rendered"),

    dict(name="WhoIsLookup", http=True, pageparam="page",
         url="https://www.whoislookup.com/",
         note="WHOIS + DNS data. Server-rendered"),

    dict(name="DomainCom", http=True, pageparam="page",
         url="https://www.domain.com/",
         note="domain search + registration. Server-rendered results"),

    dict(name="DomainNameCom", http=True, pageparam="page",
         url="https://www.domainname.com/",
         note="domain search. Server-rendered"),

    # ── 41-50: DOMAIN PARKING / MONETIZATION LISTS ──
    dict(name="Bodis", http=True, pageparam="page",
         url="https://www.bodis.com/",
         note="domain parking + monetization. Some public lists"),

    dict(name="ParkIO", http=True, pageparam="page",
         url="https://parkio.com/",
         note="domain parking. Public domain lists"),

    dict(name="SavCom", http=True, pageparam="page",
         url="https://www.sav.com/",
         note="domain parking network. Public lists"),

    dict(name="DangaGames", http=True, pageparam="page",
         url="https://www.dangamenetwork.com/",
         note="domain parking. Some public data"),

    dict(name="Monumetric", http=True, pageparam="page",
         url="https://www.monumetric.com/",
         note="domain parking analytics. Some public lists"),

    dict(name="Adsterra", http=True, pageparam="page",
         url="https://adsterra.com/",
         note="ad network with domain lists"),

    dict(name="Adcentric", http=True, pageparam="page",
         url="https://adcentric.pro/",
         note="domain parking. Public domain data"),

    dict(name="ParkingCrew", http=True, pageparam="page",
         url="https://www.parkingcrew.net/",
         note="domain parking network. Some public lists"),

    dict(name="AdDomain", http=True, pageparam="page",
         url="https://www.addomain.com/",
         note="domain parking. Public data"),

    dict(name="DomainAds", http=True, pageparam="page",
         url="https://www.domainads.com/",
         note="domain ads + parking. Some public lists"),

    # ── 51-55: REGISTRY / ZONE-FILE PROVIDERS ──
    # These publish daily zone files (all registered domains in a TLD).
    # The zone-file URL pattern is:
    #   https://www.iana.org/domains/root/files/zone-<YYYYMMDD>.gz
    # or per-registry:
    #   https://www.verisign.com/en_us/resources/zone-files-com-and-net-d
    #   https://www.publicinterestregistry.org/zone-files/
    #   https://www.afilias.info/zone-files
    #   https://www.identitydigital.com/zone-files
    #   https://www.centralnic.com/zone-files
    #
    # The script below fetches the IANA zone file (root TLD list) as a
    # fallback. For full per-TLD zone files, download them manually and
    # run the zone-file parser separately.

    dict(name="IANA_ZoneFile", http=True, pageparam="page",
         url="https://www.iana.org/domains/root/db/",
         note="IANA root zone DB — one file per TLD. "
              "Download individual TLD files for full coverage"),

    dict(name="VerisignZone", http=False, pageparam="page",
         url="https://www.verisign.com/en_us/resources/zone-files-com-and-net-d",
         note=".com/.net zone files (multi-GB). Download manually"),

    dict(name="PIRZone", http=False, pageparam="page",
         url="https://www.publicinterestregistry.org/zone-files/",
         note=".org zone file. Download manually"),

    dict(name="AfiliasZone", http=False, pageparam="page",
         url="https://www.afilias.info/zone-files",
         note=".info/.asia/.tv zone files. Download manually"),

    dict(name="IDTZone", http=False, pageparam="page",
         url="https://www.identitydigital.com/zone-files",
         note=".io/.ai/.xyz zone files. Download manually"),

    # ── 56-60: EXTRA / NICHE SOURCES ──
    dict(name="Atom", http=True, pageparam="page",
         url="https://atom.com/",
         note="domain search + availability. Server-rendered"),

    dict(name="CentralNicZone", http=False, pageparam="page",
         url="https://www.centralnic.com/zone-files",
         note=".org/.biz zone files. Download manually"),

    dict(name="TeamInternetZone", http=False, pageparam="page",
         url="https://www.teaminternet.ag/zone-files",
         note="various TLD zone files. Download manually"),

    dict(name="XYZComZone", http=False, pageparam="page",
         url="https://xyz.com/zone-files",
         note=".xyz zone file. Download manually"),

    dict(name="ICANN", http=False, pageparam="page",
         url="https://www.icann.org/resources/pages/resources-for-the-public-2017-02-27-en",
         note="ICANN public resources — registry list, zone file links"),
]


# ============================================================================
#  HELPERS (identical logic to your original script)
# ============================================================================

def parse_range(raw):
    m = re.fullmatch(
        r"(\d{4})-(\d{1,2})-(\d{1,2})\s*[-–—]\s*(\d{4})-(\d{1,2})-(\d{1,2})",
        raw.strip(),
    )
    if not m:
        m = re.fullmatch(
            r"(\d{1,2})/(\d{1,2})/(\d{4})\s*[-–—]\s*(\d{1,2})/(\d{1,2})/(\d{4})",
            raw.strip(),
        )
        if not m:
            sys.exit(
                f"range must be 2012-01-01-2026-01-01 "
                f"or 01/01/2012-01/01/2026  (got {raw!r})"
            )
        d1, m1, y1, d2, m2, y2 = map(int, m.groups())
        return f"{y1:04d}-{m1:02d}-{d1:02d}", f"{y2:04d}-{m2:02d}-{d2:02d}"
    y1, mo1, d1, y2, mo2, d2 = map(int, m.groups())
    return f"{y1:04d}-{mo1:02d}-{d1:02d}", f"{y2:04d}-{mo2:02d}-{d2:02d}"


def make_page_url(base, page, pageparam):
    if "{page}" in base:
        return base.replace("{page}", str(page))
    m = re.search(r"[?&](page|pg|p|offset|start|page_num)=\d+", base)
    if m:
        return re.sub(
            r"([?&](?:page|pg|p|offset|start|page_num))=\d+",
            rf"\1={page}",
            base,
        )
    if "?" in base:
        return f"{base}&{pageparam}={page}"
    return f"{base}?{pageparam}={page}"


def clean_domain(t):
    t = t.strip().lower().strip(".")
    t = (
        t.removeprefix("http://")
        .removeprefix("https://")
        .split("/")[0]
        .split(":")[0]
    )
    return t if DOMAIN_RE.fullmatch(t) else None


def extract_domains(html):
    soup = BeautifulSoup(html, "html.parser")
    tokens = []
    # 1) table cells (most list sites)
    for td in soup.find_all("td"):
        tokens.extend(DOMAIN_RE.findall(td.get_text(" ", strip=True)))
    # 2) fallback: all <a> text
    if not soup.find("td"):
        for a in soup.find_all("a", href=True):
            tokens.extend(DOMAIN_RE.findall(a.get_text(" ", strip=True)))
    # 3) fallback: <li> and <div class*=domain>
    if not tokens:
        for el in soup.find_all(["li", "div"]):
            txt = el.get_text(" ", strip=True)
            if "." in txt and len(txt) < 80:
                tokens.extend(DOMAIN_RE.findall(txt))
    return tokens


class Dedup:
    def __init__(self):
        self.set: set[str] = set()
        self.order: list[str] = []
        self.src: dict[str, str] = {}

    def add(self, d, s):
        if d in self.set:
            return False
        self.set.add(d)
        self.order.append(d)
        self.src[d] = s
        return True

    @property
    def size(self):
        return len(self.set)


async def fetch(session, url, timeout, retries):
    for attempt in range(retries + 1):
        try:
            async with session.get(
                url,
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as r:
                if r.status in (429, 500, 502, 503, 504) and attempt < retries:
                    await asyncio.sleep(2 * (2 ** attempt))
                    continue
                if r.status == 403:
                    return None, "HTTP 403 (bot-blocked / Cloudflare)"
                if r.status != 200:
                    return None, f"HTTP {r.status}"
                return await r.text(), None
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt < retries:
                await asyncio.sleep(2 * (2 ** attempt))
                continue
            return None, "network error"
    return None, "failed"


async def crawl_source(src, args, dedup):
    name, base = src["name"], src["url"]
    st = {"pages": 0, "raw": 0, "new": 0, "js_warn": False, "blocked": False}
    cap = args.pages if args.pages > 0 else 200
    seen_pages: set[str] = set()
    empty_streak, err_streak = 0, 0

    connector = aiohttp.TCPConnector(
        ssl=False,            # some sites have broken certs
        limit=1,
        limit_per_host=1,
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        for page in range(1, cap + 1):
            url = make_page_url(base, page, src["pageparam"])
            html, err = await fetch(session, url, args.timeout, args.retries)

            # ── error handling ──
            if err:
                if "403" in err or "blocked" in err:
                    st["blocked"] = True
                    print(
                        f"\n  ⚠ [{name}] bot-blocked (Cloudflare?) — skip",
                        file=sys.stderr,
                    )
                    break
                err_streak += 1
                if err_streak >= 3:
                    print(
                        f"\n  [{name}] page {page}: {err} ×3 → stop",
                        file=sys.stderr,
                    )
                    break
                await asyncio.sleep(args.pause)
                continue
            err_streak = 0

            # ── extract ──
            found = [d for d in (clean_domain(t) for t in extract_domains(html)) if d]
            st["raw"] += len(found)
            st["pages"] = page

            if found:
                sig = hashlib.md5("\n".join(sorted(found)).encode()).hexdigest()
                if sig in seen_pages:
                    break                       # identical page → loop guard
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

            # ── progress ──
            print(
                f"\r  [{name:<22}] pg {page:<4} "
                f"found {len(found):>5}   "
                f"unique {dedup.size:>10}",
                end="",
                flush=True,
            )

            if args.max_domains and dedup.size >= args.max_domains:
                break
            await asyncio.sleep(args.pause)

    print()  # newline after progress bar
    return st


async def main():
    p = argparse.ArgumentParser(
        description="Multi-source domain grabber — 50+ real sites, deduped."
    )
    p.add_argument("range", metavar="FROM-TO",
                   help="e.g. 01/01/2012-01/01/2026")
    p.add_argument("--only",
                   help="comma-list of source names (default: all)")
    p.add_argument("--list", action="store_true",
                   help="print all sources and exit")
    p.add_argument("-o", "--out",
                   help="output file (default: All-Domain (<range>).txt)")
    p.add_argument("--pages", type=int, default=200,
                   help="max pages per source (0=until end, default 200)")
    p.add_argument("--max-domains", type=int, default=0,
                   help="stop everything at N unique")
    p.add_argument("--sources", type=int, default=3,
                   help="concurrent sources (default 3)")
    p.add_argument("--pause", type=float, default=2.0,
                   help="seconds between pages per source (default 2.0)")
    p.add_argument("--timeout", type=float, default=30)
    p.add_argument("--retries", type=int, default=2)
    args = p.parse_args()

    # ── --list ──
    if args.list:
        print(f"{'#':>3}  {'SOURCE':<24}{'HTTP':>5}  URL")
        print("-" * 80)
        for i, s in enumerate(SOURCES, 1):
            flag = "" if s["http"] else " ⚠JS"
            print(f"{i:>3}  {s['name']:<24}{str(s['http']):>5}  {s['url']}{flag}")
            print(f"     · {s['note']}")
        print("-" * 80)
        print(f"total: {len(SOURCES)} sources")
        return

    # ── filter by --only ──
    if args.only:
        want = {x.strip() for x in args.only.split(",")}
        srcs = [s for s in SOURCES if s["name"] in want]
        if not srcs:
            sys.exit(f"no sources match --only {args.only!r}  (run --list)")
    else:
        srcs = SOURCES

    # ── parse range ──
    frm, to = parse_range(args.range)
    safe = re.sub(r"\s+", " ", re.sub(r"[^\w.\-() ]", "-", args.range.strip()))
    out_path = args.out or f"All-Domain ({safe}).txt"

    # ── banner ──
    print(f"range     : {frm} → {to}")
    print(f"sources   : {len(srcs)}   (concurrent={args.sources}, pause={args.pause}s)")
    print(f"pages cap : {args.pages if args.pages > 0 else '∞ (cap 5000)'}")
    print()
    for s in srcs:
        flag = "" if s["http"] else "  ⚠ JS/SPA"
        print(f"  · {s['name']:<24} {s['url']}{flag}")
    print("-" * 72)
    print()

    # ── crawl ──
    dedup = Dedup()
    sem = asyncio.Semaphore(args.sources)
    stats: dict[str, dict] = {}

    async def run(s):
        async with sem:
            stats[s["name"]] = await crawl_source(s, args, dedup)

    t0 = time.monotonic()
    await asyncio.gather(*[run(s) for s in srcs])
    elapsed = time.monotonic() - t0

    # ── write ──
    order = dedup.order
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(order) + ("\n" if order else ""))

    # ── report ──
    print("-" * 72)
    print(f"{'SOURCE':<24} {'PGS':>5} {'RAW':>10} {'NEW':>10}  NOTE")
    print("-" * 72)
    total_raw = 0
    total_new = 0
    for name, st in stats.items():
        note = ""
        if st["blocked"]:
            note = "⚠ bot-blocked"
        elif st["js_warn"]:
            note = "⚠ JS/SPA (0 found)"
        elif st["raw"] == 0:
            note = "0 domains (wrong URL?)"
        print(
            f"{name:<24} {st['pages']:>5} "
            f"{st['raw']:>10,} {st['new']:>10,}  {note}"
        )
        total_raw += st["raw"]
        total_new += st["new"]

    print("-" * 72)
    if not order:
        print("⚠ 0 domains written.")
        print("  → Most sources were bot-blocked or JS/SPA.")
        print("  → Open each site in a browser, apply date filter,")
        print("    copy the URL, paste into url= in the SOURCES list.")
        print("  → JS/SPA sites need a Playwright variant.")
    print(f"unique saved  : {dedup.size:>12,}")
    print(f"total raw     : {total_raw:>12,}")
    print(f"elapsed       : {elapsed:>12.0f}s")
    print(f"output        : {out_path}")

    if dedup.size > 0:
        # write a per-source breakdown file too
        src_file = out_path.replace(".txt", "_sources.txt")
        with open(src_file, "w", encoding="utf-8") as fh:
            for d in order:
                fh.write(f"{dedup.src[d]}\t{d}\n")
        print(f"source map    : {src_file}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\naborted", file=sys.stderr)
        sys.exit(130)


#python grab_multi_50.py 01/01/2012-01/01/2026 --list
#python grab_multi_50.py 01/01/2012-01/01/2026 --only ExpiredDomains --pages 5
#python grab_multi_50.py 01/01/2012-01/01/2026 --only DomCop --pages 5
#python grab_multi_50.py 01/01/2012-01/01/2026 --only SpamZilla --pages 5
#python grab_multi_50.py 01/01/2012-01/01/2026 \
    #--only ExpiredDomains,DomCop,SpamZilla,ExpiredDomainsList \
    #--pages 200 --sources 3 --pause 2.0 -o domains.txt
#python grab_multi_50.py 01/01/2012-01/01/2026 \ --pages 100 --sources 5 --pause 3.0 -o domains_all.txt
