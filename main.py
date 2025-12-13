#!/usr/bin/env python3
"""Telegram chat exporter (Termux-friendly).

This script is intentionally built for **legitimate** archiving/moderation use:
- It only scans chats you're already a member of.
- It can filter chats by keywords (e.g., chat title contains a keyword).
- It exports messages in chronological order (oldest -> newest).
- It can dedupe identical message texts.
- It can optionally redact likely payment-card-like numbers (Luhn-validated) so
  you don't accidentally store sensitive data.

Requirements:
  pip install -r requirements.txt

Telegram API credentials:
  - Create an API ID/HASH at https://my.telegram.org
  - Provide them via env vars TG_API_ID and TG_API_HASH (recommended)

Example (Termux):
  pkg update -y
  pkg install -y python
  pip install -r requirements.txt

  export TG_API_ID="12345"
  export TG_API_HASH="your_api_hash"
  python main.py --keywords "card,drop,scrap" --out export.txt

Notes:
  - The first run will ask for your phone number and login code.
  - A *.session file will be created in the current directory.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from telethon import TelegramClient, errors
from telethon.tl.types import Message, MessageService


DIGIT_CANDIDATE_RE = re.compile(r"(?<!\d)(?:\d[ -]?){12,23}(?!\d)")


def _utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _luhn_ok(num: str) -> bool:
    """Return True if the number passes Luhn checksum."""
    digits = [int(c) for c in num if c.isdigit()]
    if len(digits) < 12:
        return False
    total = 0
    parity = len(digits) % 2
    for i, d in enumerate(digits):
        if i % 2 == parity:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def redact_sensitive_numbers(text: str) -> str:
    """Redact likely sensitive long numbers (e.g., payment-card-like).

    - Finds digit sequences (optionally separated by spaces/dashes).
    - If the digits count looks plausible and passes Luhn, replaces with [REDACTED].

    This is a safety feature to avoid storing sensitive data by accident.
    """

    def _replace(match: re.Match[str]) -> str:
        raw = match.group(0)
        digits = "".join(c for c in raw if c.isdigit())
        # Typical PAN lengths are 13-19. We keep a slightly wider band.
        if 13 <= len(digits) <= 19 and _luhn_ok(digits):
            return "[REDACTED]"
        return raw

    return DIGIT_CANDIDATE_RE.sub(_replace, text)


@dataclass(frozen=True)
class ExportStats:
    chats_seen: int = 0
    chats_selected: int = 0
    messages_written: int = 0
    messages_skipped: int = 0
    duplicates_skipped: int = 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export Telegram messages from your existing chats.")

    p.add_argument(
        "--keywords",
        default="",
        help="Comma-separated keywords. Chats whose title/username matches any keyword are exported.",
    )
    p.add_argument(
        "--out",
        default="export.txt",
        help="Output text file path (TSV lines).",
    )
    p.add_argument(
        "--session",
        default="termux",
        help="Telethon session name/path (creates <name>.session).",
    )

    p.add_argument("--api-id", default=os.getenv("TG_API_ID", ""), help="Telegram API ID (or TG_API_ID env var).")
    p.add_argument(
        "--api-hash", default=os.getenv("TG_API_HASH", ""), help="Telegram API hash (or TG_API_HASH env var)."
    )

    p.add_argument(
        "--no-channels",
        dest="include_channels",
        action="store_false",
        help="Do not include channels.",
    )
    p.add_argument(
        "--no-groups",
        dest="include_groups",
        action="store_false",
        help="Do not include groups.",
    )
    p.add_argument(
        "--include-private",
        action="store_true",
        default=False,
        help="Also include private chats/DMs (default: false).",
    )
    p.set_defaults(include_channels=True, include_groups=True)

    p.add_argument(
        "--max-chats",
        type=int,
        default=0,
        help="Max number of matched chats to export (0 = no limit).",
    )
    p.add_argument(
        "--max-messages-per-chat",
        type=int,
        default=0,
        help="Max messages per chat (0 = no limit).",
    )

    p.add_argument(
        "--no-dedupe",
        dest="dedupe",
        action="store_false",
        help="Disable deduping identical message texts.",
    )
    p.add_argument(
        "--no-redact-sensitive",
        dest="redact_sensitive",
        action="store_false",
        help="Disable redaction of Luhn-valid long numbers.",
    )
    p.set_defaults(dedupe=True, redact_sensitive=True)

    p.add_argument(
        "--quiet",
        action="store_true",
        default=False,
        help="Less console output.",
    )

    return p.parse_args(argv)


def _kw_list(s: str) -> list[str]:
    kws = [k.strip().lower() for k in s.split(",") if k.strip()]
    # Deduplicate while preserving order
    seen: set[str] = set()
    out: list[str] = []
    for k in kws:
        if k not in seen:
            out.append(k)
            seen.add(k)
    return out


def _chat_matches(dialog, keywords: list[str]) -> bool:
    title = (dialog.name or "").lower()
    username = ""
    try:
        username = (getattr(dialog.entity, "username", None) or "").lower()
    except Exception:
        username = ""

    if not keywords:
        return True

    hay = f"{title} {username}".strip()
    return any(k in hay for k in keywords)


def _is_exportable_dialog(dialog, *, include_channels: bool, include_groups: bool, include_private: bool) -> bool:
    if dialog.is_channel:
        # Channels and some supergroups appear as channel entities.
        if dialog.is_group:
            return include_groups
        return include_channels
    if dialog.is_group:
        return include_groups
    return include_private


def _message_text(msg: Message) -> str:
    # Prefer .message for text; captions are also in .message.
    return msg.message or ""


async def export_messages(args: argparse.Namespace) -> ExportStats:
    if not args.api_id or not args.api_hash:
        raise SystemExit(
            "Missing Telegram API credentials. Set TG_API_ID and TG_API_HASH env vars or pass --api-id/--api-hash."
        )

    try:
        api_id = int(args.api_id)
    except ValueError as e:
        raise SystemExit("TG_API_ID / --api-id must be an integer.") from e

    keywords = _kw_list(args.keywords)

    stats = ExportStats()
    dedupe_hashes: set[str] = set()

    client = TelegramClient(args.session, api_id, args.api_hash)

    await client.start()  # interactive login if needed

    # Write header for TSV output.
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("utc_time\tchat\tchat_id\tmsg_id\ttext\n")

        selected_count = 0
        async for dialog in client.iter_dialogs():
            stats = ExportStats(
                chats_seen=stats.chats_seen + 1,
                chats_selected=stats.chats_selected,
                messages_written=stats.messages_written,
                messages_skipped=stats.messages_skipped,
                duplicates_skipped=stats.duplicates_skipped,
            )

            if not _is_exportable_dialog(
                dialog,
                include_channels=args.include_channels,
                include_groups=args.include_groups,
                include_private=args.include_private,
            ):
                continue

            if not _chat_matches(dialog, keywords):
                continue

            selected_count += 1
            stats = ExportStats(
                chats_seen=stats.chats_seen,
                chats_selected=stats.chats_selected + 1,
                messages_written=stats.messages_written,
                messages_skipped=stats.messages_skipped,
                duplicates_skipped=stats.duplicates_skipped,
            )

            if not args.quiet:
                print(f"[chat {selected_count}] {dialog.name} (id={dialog.id})")

            msg_count = 0
            try:
                async for msg in client.iter_messages(dialog.entity, reverse=True):
                    if isinstance(msg, MessageService):
                        stats = ExportStats(
                            chats_seen=stats.chats_seen,
                            chats_selected=stats.chats_selected,
                            messages_written=stats.messages_written,
                            messages_skipped=stats.messages_skipped + 1,
                            duplicates_skipped=stats.duplicates_skipped,
                        )
                        continue

                    text = _normalize_ws(_message_text(msg))
                    if not text:
                        stats = ExportStats(
                            chats_seen=stats.chats_seen,
                            chats_selected=stats.chats_selected,
                            messages_written=stats.messages_written,
                            messages_skipped=stats.messages_skipped + 1,
                            duplicates_skipped=stats.duplicates_skipped,
                        )
                        continue

                    if args.redact_sensitive:
                        text = redact_sensitive_numbers(text)

                    if args.dedupe:
                        h = hashlib.sha256(text.encode("utf-8")).hexdigest()
                        if h in dedupe_hashes:
                            stats = ExportStats(
                                chats_seen=stats.chats_seen,
                                chats_selected=stats.chats_selected,
                                messages_written=stats.messages_written,
                                messages_skipped=stats.messages_skipped,
                                duplicates_skipped=stats.duplicates_skipped + 1,
                            )
                            continue
                        dedupe_hashes.add(h)

                    dt = msg.date
                    if dt is None:
                        dt = datetime.now(timezone.utc)

                    line = f"{_utc_iso(dt)}\t{(dialog.name or '').replace('\t', ' ')}\t{dialog.id}\t{msg.id}\t{text.replace('\t', ' ')}\n"
                    f.write(line)

                    msg_count += 1
                    stats = ExportStats(
                        chats_seen=stats.chats_seen,
                        chats_selected=stats.chats_selected,
                        messages_written=stats.messages_written + 1,
                        messages_skipped=stats.messages_skipped,
                        duplicates_skipped=stats.duplicates_skipped,
                    )

                    if args.max_messages_per_chat and msg_count >= args.max_messages_per_chat:
                        break

            except errors.FloodWaitError as e:
                # Respect Telegram rate limits.
                wait_s = int(getattr(e, "seconds", 0) or 0)
                if not args.quiet:
                    print(f"Rate limited. Sleeping for {wait_s}s...")
                await asyncio.sleep(max(wait_s, 1))

            if args.max_chats and selected_count >= args.max_chats:
                break

    await client.disconnect()
    return stats


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)

    try:
        stats = asyncio.run(export_messages(args))
    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 130

    if not args.quiet:
        print(
            "Done. "
            f"chats_seen={stats.chats_seen}, "
            f"chats_selected={stats.chats_selected}, "
            f"messages_written={stats.messages_written}, "
            f"skipped={stats.messages_skipped}, "
            f"dupes_skipped={stats.duplicates_skipped}."
        )
        print(f"Output: {args.out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
