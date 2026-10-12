#!/usr/bin/env python3
"""Daily public-source research. Requires OPENAI_API_KEY repository secret."""
import datetime as dt
import json
import os
import pathlib
import sys
import urllib.error
import urllib.request
from zoneinfo import ZoneInfo

ROOT = pathlib.Path(__file__).resolve().parents[1]
today = dt.datetime.now(ZoneInfo("Asia/Kolkata")).date()
relative = pathlib.Path("daily") / str(today.year) / f"{today.month:02d}" / f"Interview_Preparation_{today.isoformat()}.md"
destination = ROOT / relative
if destination.exists():
    print(f"Already published: {relative}")
    sys.exit(0)

key = os.getenv("OPENAI_API_KEY")
if not key:
    sys.exit("OPENAI_API_KEY secret is missing. No report was published.")

existing = sorted((ROOT / "daily").glob("**/Interview_Preparation_*.md")) if (ROOT / "daily").exists() else []
recent_names = [p.name for p in existing[-30:]]
instructions = (
    "Research PUBLICLY accessible posts from LinkedIn containing genuine software engineering "
    "interview questions, and credible official sources for recent engineering developments. "
    "Prioritize Java, Spring Boot, Kafka, concurrency, AWS, databases, system design, DSA, "
    "AI engineering and security. Produce a detailed standalone Markdown report titled "
    f"'Interview Preparation — {today.isoformat()}'. Include only questions you can support "
    "with an original publicly accessible URL. For each interview question give the question, "
    "source URL, full correct explanation, Java solution where appropriate, complexity, edge "
    "cases, and follow-up questions. For new engineering topics give the source, publication "
    "date when known, what's new, and practical implications. Distinguish verified facts "
    "from unverified claims and never invent URLs. Do not claim access to private feeds or "
    "saved posts. If few posts can be verified, say so instead of fabricating questions. "
    "Prefer substantive accuracy over quantity. Avoid previously covered questions where possible. "
    f"Recent report filenames: {', '.join(recent_names)}. "
    "Return ONLY Markdown content, no code fence."
)
payload = json.dumps({
    "model": "gpt-5.1",
    "tools": [{"type": "web_search"}],
    "input": instructions,
    "max_output_tokens": 12000
}).encode()
request = urllib.request.Request(
    "https://api.openai.com/v1/responses",
    data=payload,
    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
    method="POST"
)
try:
    with urllib.request.urlopen(request, timeout=300) as response:
        result = json.load(response)
except urllib.error.HTTPError as exc:
    sys.exit(f"Research API failed: HTTP {exc.code}: {exc.read(1500).decode(errors='replace')}")
parts = []
for item in result.get("output", []):
    if item.get("type") == "message":
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                parts.append(content.get("text", ""))
report = "\n".join(parts).strip()
if len(report) < 800:
    sys.exit("Research response was empty or too short; refusing to publish.")
destination.parent.mkdir(parents=True, exist_ok=True)
destination.write_text(report + "\n", encoding="utf-8")
index = ROOT / "INDEX.md"
lines = ["# Interview Preparation Index", "", "## Published reports", ""]
for path in sorted([*existing, destination], reverse=True):
    lines.append(f"- [{path.stem}]({path.relative_to(ROOT).as_posix()})")
index.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(f"Created {relative}")
