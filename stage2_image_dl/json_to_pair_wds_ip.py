#!/usr/bin/env python3
import argparse
import json
import os
import random
import socket
import time
import mimetypes
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import dns.resolver
import requests
import webdataset as wds
from requests.exceptions import SSLError

# =========================
# Global DNS/IP cache config
# =========================

# Global host->IP CSV (read-only during shard jobs)
GLOBAL_IP_FILE = Path("./host_ip_map.csv")  # adjust to absolute path if you prefer
GLOBAL_IP_MAP = {}

if GLOBAL_IP_FILE.exists():
    with open(GLOBAL_IP_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 2:
                host, ip = parts[0], parts[1]
                GLOBAL_IP_MAP[host] = ip
    print(f"[INFO] Loaded {len(GLOBAL_IP_MAP)} entries from global host_ip_map.csv")
else:
    print(f"[INFO] Global host_ip_map.csv not found; continuing without it.")

# Campus DNS from /etc/resolv.conf
CAMPUS_DNS = ["10.242.67.11", "140.247.139.253", "10.31.20.13"]

# External DNS (kept small to avoid timeouts on blocked servers)
EXTERNAL_DNS = [
    "8.8.8.8", "8.8.4.4",  # Google
    "1.1.1.1", "1.0.0.1",  # Cloudflare
]

# Per-process, per-shard IP cache state
HOST_IP = {}          # dict: host -> ip (global+shard)
IP_RECORD_FILE = None # path to current shard's .ip file (or None)


# =========================
# DNS helpers
# =========================

def resolve_with_system(host: str):
    """Use the node's default resolver (glibc + /etc/resolv.conf)."""
    try:
        infos = socket.getaddrinfo(host, None)
        # Prefer IPv4 if available
        for family, _, _, _, sockaddr in infos:
            if family == socket.AF_INET:
                return sockaddr[0]
        return infos[0][4][0]
    except Exception:
        return None


def resolve_with_dnspython_single(host: str, nameserver: str, timeout: float = 0.2):
    """Use dnspython against a single nameserver IP."""
    resolver = dns.resolver.Resolver(configure=False)
    resolver.nameservers = [nameserver]
    resolver.lifetime = timeout
    resolver.timeout = timeout
    try:
        answer = resolver.resolve(host, "A")
        for r in answer:
            return r.address
    except Exception:
        return None

def record_host_ip(host: str, ip: str):
    """
    Record a newly learned host->IP mapping in the in-memory HOST_IP
    and append to the current shard's .ip file (IP_RECORD_FILE).
    """
    global HOST_IP, IP_RECORD_FILE
    if host in HOST_IP:
        return
    HOST_IP[host] = ip
    if IP_RECORD_FILE is None:
        return
    try:
        with open(IP_RECORD_FILE, "a") as f:
            f.write(f"{host},{ip}\n")
    except Exception as e:
        print(f"[WARN] Could not append to {IP_RECORD_FILE}: {e}")


def resolve_hostname_balanced(host: str):
    """
    Resolve host using:
      - HOST_IP cache first
      - then balanced DNS: system + campus + external in random order
    Returns IPv4 string or None.
    """
    # 0) Cache hit
    if host in HOST_IP:
        return HOST_IP[host]

    # 1) Balanced DNS
    options = ["system"] + CAMPUS_DNS + EXTERNAL_DNS
    random.shuffle(options)

    for opt in options:
        if opt == "system":
            ip = resolve_with_system(host)
        else:
            ip = resolve_with_dnspython_single(host, opt)

        if ip:
            record_host_ip(host, ip)
            return ip

    return None


def rewrite_url_with_ip_table_only(url: str):
    """
    If host is in HOST_IP, rewrite URL to use the cached IP and set Host header.
    Otherwise return (original_url, {}).
    """
    parsed = urlparse(url)
    host = parsed.hostname
    if not host:
        return url, {}

    ip = HOST_IP.get(host)
    if not ip:
        return url, {}

    netloc = f"{ip}:{parsed.port}" if parsed.port else ip
    new_url = urlunparse((
        parsed.scheme,
        netloc,
        parsed.path or "",
        parsed.params or "",
        parsed.query or "",
        parsed.fragment or "",
    ))
    headers = {"Host": host}
    return new_url, headers


def rewrite_url_with_dns_balanced(url: str):
    """
    Use balanced DNS (system + campus + external) to resolve the host to an IP.
    If resolution succeeds, return (url_with_ip, Host header).
    If it fails, return (original_url, {}), letting requests use default DNS.
    """
    parsed = urlparse(url)
    host = parsed.hostname
    if not host:
        return url, {}

    ip = resolve_hostname_balanced(host)
    if not ip:
        return url, {}

    netloc = f"{ip}:{parsed.port}" if parsed.port else ip
    new_url = urlunparse((
        parsed.scheme,
        netloc,
        parsed.path or "",
        parsed.params or "",
        parsed.query or "",
        parsed.fragment or "",
    ))
    headers = {"Host": host}
    return new_url, headers


def rewrite_url_ip_first_then_dns(url: str, use_ip_table: bool = True):
    """
    If use_ip_table is True and host is in HOST_IP, use that.
    Otherwise, use balanced DNS (system + campus + external).
    """
    if use_ip_table:
        req_url, headers = rewrite_url_with_ip_table_only(url)
        if req_url != url or headers:
            return req_url, headers  # IP table hit

    # Fallback: balanced DNS
    return rewrite_url_with_dns_balanced(url)


# =========================
# Download + WebDataset logic
# =========================

def guess_ext_from_url_or_ctype(url, content_type):
    """
    Return extension string WITHOUT leading dot (e.g. 'png', 'gif', 'jpg').
    Prefer Content-Type, fall back to URL path, then default to 'jpg'.
    """
    # 1. From Content-Type header
    if content_type:
        ct = content_type.split(";", 1)[0].strip().lower()  # e.g. image/png
        ext = mimetypes.guess_extension(ct)
        if ext:
            return ext.lstrip(".")

    # 2. From URL path
    parsed = urlparse(url)
    path = parsed.path or ""
    mime, _ = mimetypes.guess_type(path)
    if mime:
        ext = mimetypes.guess_extension(mime)
        if ext:
            return ext.lstrip(".")

    # 3. Fallback
    return "jpg"


def download_image(
    url,
    timeout=1,
    max_size=10 * 1024 * 1024,
    max_retries=2,
    base_backoff=0.1,
):
    """
    Download an image with retries and backoff.

    Order:
      1. Try HOST_IP table (if entry exists).
      2. If not, or if HTTPS IP-based access fails (SSLError), use
         balanced DNS (system + campus + external, randomized).
      3. If DNS-based resolution fails, we fall back to the original URL
         and let requests use system DNS.

    Returns: (bytes_or_None, num_attempts, content_type_or_None)
    """
    attempt = 0
    use_ip_table = True  # can be turned off after HTTPS failures
    last_ctype = None

    while attempt < max_retries:
        attempt += 1
        try:
            # IP-table first (if enabled), then balanced DNS
            req_url, extra_headers = rewrite_url_ip_first_then_dns(
                url, use_ip_table=use_ip_table
            )

            with requests.get(req_url, timeout=timeout, stream=True, headers=extra_headers) as r:
                status = r.status_code

                if status in (429, 503):
                    retry_after = r.headers.get("Retry-After")
                    if retry_after is not None:
                        try:
                            sleep_s = float(retry_after)
                        except ValueError:
                            sleep_s = base_backoff * attempt
                    else:
                        sleep_s = base_backoff * attempt
                    time.sleep(sleep_s + random.uniform(0, 0.5))
                    continue

                r.raise_for_status()
                ctype = r.headers.get("Content-Type", "")
                last_ctype = ctype

                if "image" not in ctype.lower():
                    return None, attempt, ctype

                chunks = []
                total = 0
                for chunk in r.iter_content(8192):
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > max_size:
                        return None, attempt, ctype
                    chunks.append(chunk)
                return b"".join(chunks), attempt, ctype

        except SSLError:
            # Likely HTTPS cert/SNI mismatch when using IP directly.
            # Disable IP-table path for this URL and retry using DNS only.
            use_ip_table = False
            if attempt >= max_retries:
                return None, attempt, last_ctype
            sleep_s = base_backoff * (2 ** (attempt - 1))
            time.sleep(sleep_s + random.uniform(0, 0.5))

        except requests.RequestException:
            if attempt >= max_retries:
                return None, attempt, last_ctype
            sleep_s = base_backoff * (2 ** (attempt - 1))
            time.sleep(sleep_s + random.uniform(0, 0.5))

    return None, attempt, last_ctype


def _load_json_field(field):
    """Normalize sample['json'] into a Python dict."""
    if isinstance(field, dict):
        return field
    if isinstance(field, (bytes, bytearray)):
        return json.loads(field.decode("utf-8"))
    if isinstance(field, str):
        return json.loads(field)
    return json.loads(field)


def process_one_json_shard(
    input_shard: Path,
    output_pattern: str,
    max_samples_per_shard=None,
    max_images_per_doc=None,
):
    global HOST_IP, IP_RECORD_FILE

    shard_name = input_shard.stem

    print(f"[INFO] Processing shard: {input_shard}")
    print(f"       Output pattern:  {output_pattern}")

    dataset = wds.WebDataset(str(input_shard), shardshuffle=False).decode()

    # Determine a sidecar "done" file path (per input shard)
    if "%" not in output_pattern:
        done_file = output_pattern + ".done"
    else:
        # tie the done-file to the input shard instead
        done_file = str(input_shard) + ".done"

    done_keys = set()
    if os.path.exists(done_file):
        with open(done_file, "r") as f:
            for line in f:
                key = line.strip().split("\t", 1)[0]
                if key:
                    done_keys.add(key)
        print(f"[INFO] Loaded {len(done_keys)} completed keys from {done_file}")
    else:
        print(f"[INFO] No existing done file, starting fresh.")

    # Shard-specific IP cache file, e.g. CC-MAIN-2013-48-000000.tar.ip
    ip_file = str(input_shard) + ".ip"
    IP_RECORD_FILE = ip_file  # set global pointer for this shard

    # Build HOST_IP for this shard: start with global, then overlay shard-specific map
    HOST_IP = dict(GLOBAL_IP_MAP)

    if os.path.exists(ip_file):
        with open(ip_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 2:
                    host, ip = parts[0], parts[1]
                    HOST_IP[host] = ip
        print(f"[INFO] Loaded {len(HOST_IP)} host->IP entries (global + shard) for {input_shard}")
    else:
        print(f"[INFO] No existing IP map file for this shard; starting from global only.")

    # open done file for appending
    done_f = open(done_file, "a", buffering=1)  # line-buffered

    # Stats
    doc_idx = 0
    written_pairs = 0
    total_slots_with_url = 0
    attempted_downloads = 0
    successful_downloads = 0
    failed_downloads = 0
    total_attempts = 0

    use_shardwriter = max_samples_per_shard and "%" in output_pattern

    if use_shardwriter:
        writer_cls = wds.ShardWriter
        writer_kwargs = {"maxcount": max_samples_per_shard}
    else:
        writer_cls = wds.TarWriter
        writer_kwargs = {}
        if max_samples_per_shard and "%" not in output_pattern:
            print("[WARN] max_samples_per_shard set but output_shard has no '%'; "
                  "writing a single tar file.")

    pair_idx = 0
    with writer_cls(output_pattern, **writer_kwargs) as sink:
        for sample in dataset:
            j = _load_json_field(sample["json"])

            images = j.get("images") or []
            texts = j.get("texts") or []

            if not isinstance(images, list) or len(images) == 0:
                doc_idx += 1
                continue

            num_slots = len(images)

            full_txt_array = texts
            full_url_array = images
            num_yielded_for_doc = 0

            for i, url in enumerate(images):
                if url is None:
                    continue

                key = f"{doc_idx:09d}_{i:04d}"

                # skip if already completed
                if key in done_keys:
                    continue

                total_slots_with_url += 1

                if max_images_per_doc is not None and num_yielded_for_doc >= max_images_per_doc:
                    break

                attempted_downloads += 1
                img_bytes, attempts, content_type = download_image(
                    url,
                    timeout=1,
                    max_size=10 * 1024 * 1024,
                    max_retries=2,
                    base_backoff=0.1,
                )
                total_attempts += attempts
                if img_bytes is None:
                    failed_downloads += 1
                    continue

                pair_idx += 1
                if pair_idx % 100 == 0:
                    print(f"[PROGRESS] {pair_idx} pairs written so far")

                successful_downloads += 1

                # Decide extension based on URL / Content-Type
                ext = guess_ext_from_url_or_ctype(url, content_type)  # e.g. 'png', 'gif', 'jpg'
                image_filename = f"{key}.{ext}"

                meta = {
                    "shard": shard_name,
                    "doc_idx": int(doc_idx),
                    "sub_index": int(i),
                    "num_slots": int(num_slots),
                    "url": url,
                    "image_filename": image_filename,
                }

                txt_payload = {
                    "texts": full_txt_array,
                    "urls": full_url_array,
                }

                out = {
                    "__key__": key,
                    ext: img_bytes,  # field name matches ext ('png', 'gif', 'jpg', ...)
                    "txt": json.dumps(txt_payload).encode("utf-8"),
                    "json": json.dumps(meta).encode("utf-8"),
                }

                sink.write(out)
                written_pairs += 1
                num_yielded_for_doc += 1

                # record completion to done file and in-memory set
                done_f.write(f"{key}\t{url}\n")
                done_keys.add(key)

            doc_idx += 1

    done_f.close()
    print(f"[INFO] Finished shard {input_shard}: "
          f"written_pairs={written_pairs}, "
          f"attempted_downloads={attempted_downloads}, "
          f"successful_downloads={successful_downloads}, "
          f"failed_downloads={failed_downloads}, "
          f"total_attempts={total_attempts}")


# =========================
# CLI
# =========================

def parse_args():
    ap = argparse.ArgumentParser(
        description="Convert ONE JSON-only WebDataset shard into pair-level shards."
    )
    ap.add_argument(
        "--input-shard", required=True,
        help="Path to a single input JSON-only shard .tar",
    )
    ap.add_argument(
        "--output-shard", required=True,
        help=(
            "Output .tar path or pattern. "
            "If it contains '%%' and --max-samples-per-shard > 0, "
            "ShardWriter is used (multiple output shards). "
            "Otherwise a single tar file is written."
        ),
    )
    ap.add_argument(
        "--max-samples-per-shard", type=int, default=0,
        help="Max samples per output shard (only used if output-shard contains '%%').",
    )
    ap.add_argument(
        "--max-images-per-doc", type=int, default=None,
        help="Optional cap on images per document (None = no limit).",
    )
    return ap.parse_args()


def main():
    args = parse_args()
    input_shard = Path(args.input_shard)
    output_pattern = args.output_shard

    # If we're doing a single fixed tar (no '%'), allow skip-if-exists
    if "%" not in output_pattern:
        out_path = Path(output_pattern)
        if out_path.exists():
            print(f"[INFO] Output shard {out_path} already exists, skipping.")
            return
        out_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        # pattern: ensure directory exists (dirname of the pattern)
        Path(output_pattern).parent.mkdir(parents=True, exist_ok=True)

    maxcount = args.max_samples_per_shard if args.max_samples_per_shard > 0 else None

    process_one_json_shard(
        input_shard=input_shard,
        output_pattern=output_pattern,
        max_samples_per_shard=maxcount,
        max_images_per_doc=args.max_images_per_doc,
    )


if __name__ == "__main__":
    main()
