# Software Engineering Interview Hub

Daily source-linked interview preparation and software engineering updates.

## Topics
Java · Spring Boot · Kafka · Concurrency · AWS · Databases · System Design · DSA · AI Engineering · Security

## Publishing
A [scheduled GitHub Actions workflow](.github/workflows/daily-interview.yml) is configured to attempt a daily public-source research report at **08:00 IST**. It uses the OpenAI API and **will not work until you add an `OPENAI_API_KEY` repository Actions secret**. API usage may incur separate charges. It does not access your private LinkedIn feed or saved posts.

1. Open repository **Settings → Secrets and variables → Actions → New repository secret**.
2. Add secret name `OPENAI_API_KEY` with your own API key (never commit the key).
3. Open **Actions → Daily interview research → Run workflow** to test.
4. Confirm a file appears under `daily/YYYY/MM/`.

The workflow uses web research, but public LinkedIn access and interview-question coverage are not guaranteed. Review reports for factual accuracy.

## Historical reports
The previously generated October 11 and October 12 Markdown files are **not yet uploaded** to this repository. See [INDEX.md](INDEX.md) for actual publication status.
