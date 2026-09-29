
#!/usr/bin/env python3
"""
grab.py — filter a domain list by RDAP registration date,
write every domain to  All-Domain (<start>-<end>).txt

  python grab.py 01/01/2012-01/01/2026 -f candidates.txt
  cat candidates.txt | python grab.py 01/01/2012-01/01/2026
  python grab.py 2015-03-01-2015-03-31 -d example.com -d other.com
"""
import argparse, asyncio, json, os, re, sys, time
from datetime import datetime, date
from urllib.parse import quote

try:
    import aiohttp
except ImportError:
    sys.exit("missing dependency:  pip install aiohttp")

RDAP_URL = "https://rdap.org/domain/{}"
HEADERS = {
    "User-Agent": "domain-grab/1.0 (personal lookup)",
    "Accept": "application/rdap+json, application/json;q=0.9, */*;q=0.5",
}
DOMAIN_RE = re.compile(r"^(?=.{4,253}$)([a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,24}$", re.I)


# ---------- dates ----------

def _parse_date(s, dmy):
    m = re.fullmatch(r"(\d{1,4})([/-])(\d{1,2})\2(\d{1,2})", s.strip())
    if not m:
        raise ValueError(f"unparseable date {s!r}")
    a, _sep, b, c = m.groups()
    if len(a) == 4:                       # 2012-01-01 / 2012/01/01
        y, mth, d = int(a), int(b), int(c)
    else:
        x, z = int(a), int(c)
        if dmy:
            d, mth, amb = x, z, False
        elif x > 12:
            d, mth, amb = x, z, False     # 25/03/2015 → only valid as D/M/Y
        elif z > 12:
            mth, d, amb = x, z, False     # 03/25/2015 → only valid as M/D/Y
        else:
            mth, d, amb = x, z, True      # ambiguous → M/D by default
        y = int(b)
    return date(y, mth, d), amb           # ValueError if month/day out of range


def parse_range(raw, dmy):
    m = re.fullmatch(r"(.+?)\s*[-–—]\s*(.+)", raw.strip())
    if not m:
        sys.exit(f"range must be START-END, e.g. 01/01/2012-01/01/2026  (got {raw!r})")
    try:
        a, amb_a = _parse_date(m.group(1), dmy)
        b, amb_b = _parse_date(m.group(2), dmy)
    except ValueError as e:
        sys.exit(f"bad range {raw!r}: {e}")
    if not dmy and (amb_a or amb_b):
        print("note: ambiguous slash dates read as MM/DD — pass --dmy for DD/MM", file=sys.stderr)
    return (a, b) if a <= b else (b, a)


def iso_date(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except ValueError:
        return None


# ---------- input ----------

def parse_args():
    p = argparse.ArgumentParser(description="Filter a domain list by RDAP registration date.")
    p.add_argument("range", metavar="START-END", help="e.g. 01/01/2012-01/01/2026 or 2012-01-01-2026-01-01")
    p.add_argument("-f", "--file", help="one domain per line ('-' or absent → stdin if piped)")
    p.add_argument("-d", "--domain", action="append", default=[], help="single domain, repeatable")
    p.add_argument("-o", "--out", help="output file (default: All-Domain (<range>).txt)")
    p.add_argument("--with-dates", action="store_true", help="append '\\tYYYY-MM-DD' to each line")
    p.add_argument("--dmy", action="store_true", help="read ambiguous slash dates as DD/MM/YYYY")
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--timeout", type=float, default=20)
    p.add_argument("--retries", type=int, default=2)
    p.add_argument("--cache", default=".rdap_cache.json")
    return p.parse_args()


def read_domains(args):
    lines = []
    if args.file and args.file != "-":
        with open(args.file, encoding="utf-8", errors="ignore") as fh:
            lines += fh.read().splitlines()
    if (not args.file or args.file == "-") and not sys.stdin.isatty():
        lines += sys.stdin.read().splitlines()
    lines += args.domain
    out, seen = [], set()
    for line in lines:
        d = line.strip().lower().strip(".")
        d = d.removeprefix("https://").removeprefix("http://").split("/")[0].split(":")[0]
        if DOMAIN_RE.fullmatch(d) and d not in seen:
            seen.add(d)
            out.append(d)
    return out


# ---------- RDAP ----------

def _rdap_events(data):
    out = {}
    def absorb(lst):
        for ev in lst or []:
            act = (ev.get("eventAction") or "").strip().lower()
            if act and not out.get(act):
                out[act] = ev.get("eventDate")
    absorb(data.get("events"))
    for ent in data.get("entities", []):
        absorb(ent.get("events"))
    return out


def _rdap_registrar(data):
    for ent in data.get("entities", []):
        if "registrar" in (ent.get("roles") or []):
            card = ent.get("vcardArray")
            if card:
                for field in card[1]:
                    if field[0] == "fn":
                        return field[3]
    return ""


async def lookup(session, sem, domain, timeout, retries):
    url = RDAP_URL.format(quote(domain, safe=""))
    async with sem:
        for attempt in range(retries + 1):
            try:
                async with session.get(url, headers=HEADERS, allow_redirects=True,
                                       timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                    if r.status in (429, 500, 502, 503, 504) and attempt < retries:
                        await asyncio.sleep(1.5 * (2 ** attempt)); continue
                    if r.status == 404:
                        return {"created": None, "note": "not found / no RDAP"}
                    if r.status != 200:
                        return {"created": None, "note": f"HTTP {r.status}"}
                    data = await r.json(content_type=None)
                    break
            except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError):
                if attempt < retries:
                    await asyncio.sleep(1.5 * (2 ** attempt)); continue
                return {"created": None, "note": "network error"}
        ev = _rdap_events(data)
        return {"created": ev.get("registration"),
                "registrar": _rdap_registrar(data),
                "note": "ok"}


def load_cache(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def save_cache(path, cache):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(cache, fh)
    os.replace(tmp, path)


# ---------- main ----------

async def main():
    args = parse_args()
    start, end = parse_range(args.range, args.dmy)
    domains = read_domains(args)
    if not domains:
        sys.exit("no domains given — use -f file, -d domain, or pipe via stdin")

    def hit(created_iso):
        d = iso_date(created_iso)
        return d is not None and start <= d <= end

    total = len(domains)
    print(f"range   : {start} → {end}   ({(end - start).days} days)")
    print(f"domains : {total}")

    cache = load_cache(args.cache)
    todo = [d for d in domains if d not in cache]
    print(f"cached  : {total - len(todo)}   to fetch: {len(todo)}")

    sem = asyncio.Semaphore(args.workers)
    t0 = time.monotonic()
    async with aiohttp.ClientSession() as session:
        tasks = {d: asyncio.create_task(lookup(session, sem, d, args.timeout, args.retries)) for d in todo}
        for i, d in enumerate(todo, 1):
            cache[d] = await tasks[d]
            if i % 150 == 0 or i == len(todo):
                n_in = sum(1 for x in todo[:i] if hit(cache[x].get("created")))
                rate = i / max(time.monotonic() - t0, 1e-9)
                eta = (len(todo) - i) / rate if rate else 0
                print(f"\r  {i:>6}/{len(todo)} fetched   in-range so far: {n_in}   {rate:.1f}/s   eta {eta:.0f}s",
                      end="", flush=True)
    if todo:
        print()
    save_cache(args.cache, cache)

    rows = []
    for d in domains:
        r = cache[d]
        rows.append({"domain": d,
                     "created": iso_date(r.get("created")),
                     "in_range": hit(r.get("created"))})
    rows.sort(key=lambda r: (r["created"] is None, r["created"], r["domain"]))

    if args.out:
        out_path = args.out
    else:
        safe = re.sub(r"\s+", " ", re.sub(r"[^\w.\-() ]", "-", args.range.strip()))
        out_path = f"All-Domain ({safe}).txt"

    with open(out_path, "w", encoding="utf-8") as fh:
        for r in rows:
            if args.with_dates:
                fh.write(f"{r['domain']}\t{r['created'].isoformat() if r['created'] else '-'}\n")
            else:
                fh.write(r["domain"] + "\n")

    n_dated = sum(1 for r in rows if r["created"])
    n_in = sum(1 for r in rows if r["in_range"])
    print(f"dated     : {n_dated}/{total}   (no date / redacted / missing: {total - n_dated})")
    print(f"in range  : {n_in}")
    print(f"written   : {out_path}   ({len(rows)} domains)")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\naborted — cache kept for already-fetched domains", file=sys.stderr)
        sys.exit(130)


#python grab.py 01/01/2012-01/01/2026 -f candidates.txt
# → All-Domain (01-01-2012-01-01-2026).txt   (every domain, one per line)

#python grab.py 01/01/2012-01/01/2026 -f candidates.txt --with-dates
# → oldsite.net\t2012-06-14   (tab + reg-date, unknown dates as -)

#python grab.py 01/01/2012-01/01/2026 -f candidates.txt -o mylist.txt
