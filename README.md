# Software Engineering Interview Hub

Daily source-linked interview preparation and software engineering updates.

## Published reports
- [October 12, 2026](daily/2026/10/Interview_Preparation_2026-10-12.md)
- [October 11, 2026](daily/2026/10/Interview_Preparation_2026-10-11.md)

## Topics
Java · Spring Boot · Kafka · Concurrency · AWS · Databases · System Design · DSA · AI Engineering · Security

See [INDEX.md](INDEX.md) for the full report index.

## System design interview practice

New: [System Design Interview Playbook](system-design/README.md) with:
- [HLD answer framework](system-design/interview-framework/hld-interview-playbook.md) and [URL shortener example](system-design/hld/url-shortener.md).
- [LLD answer framework](system-design/interview-framework/lld-interview-playbook.md) and [Java parking lot example](system-design/lld/parking-lot.md).
- Reusable HLD and LLD templates.

The daily report generator is now instructed to include **one HLD and one LLD walkthrough**. This applies to future runs, not already published historical reports.

## Automatic daily publishing
A [GitHub Actions workflow](.github/workflows/daily-interview.yml) is scheduled for **08:00 IST** daily and can also be run manually. To enable it, add an `OPENAI_API_KEY` GitHub Actions repository secret in **Settings → Secrets and variables → Actions**. API usage is billed separately. The workflow has not yet been tested successfully.

It researches publicly accessible material; it does **not** access your private LinkedIn feed or saved posts. Sources and solutions should be reviewed for accuracy.
