# Progress

## Where things stand (2026-11-09)

- Parser, LLM enrichment, BigQuery schema and writer, the Pub/Sub push subscriber and the daily aggregator are written.
- 52 tests pass with no GCP credentials or LLM keys: 19 parser, 5 enrichment, 5 writer, 6 service start-up and 17 aggregator tests.
- Both services start from the repo root and from their Docker images. Before 2026-10-06 neither could start, because the code sat at the repo root while every import used `tributary.*`.
- Not yet deployed to a real Google Cloud project. The LLM and BigQuery calls have only run against stubs.
- No Cloud Scheduler job or Pub/Sub definitions in `infra/` yet.
- No dossier yet (`docs/dossier/`).

## Known issues

Each of these was reproduced against the code on 2026-10-06.

1. **Amount normalisation guesses units from size.** `parse()` treats any amount under 1000 as dollars and anything else as cents, and reads `amount` and `amount_cents` the same way. Measured: `{"amount": 1500.0}` gives 1500 cents, `{"amount_cents": 500}` gives 50000, `{"amount": 19.99}` gives 1998 (float truncation), and `{"amount": 0}` gives `None`.
2. **Non-object JSON crashes the parser.** `parse(b"[1,2]")`, `parse(b'"hello"')` and `parse(b"null")` raise `AttributeError`, so the subscriber returns 500 and Pub/Sub keeps redelivering the message. It should become an `UNKNOWN` event with a parse error, like invalid JSON does.
3. **Push authentication does not match the documented subscription.** The README creates the push subscription with `--push-auth-service-account`, which makes Pub/Sub send a Google-signed OIDC token. The subscriber compares the bearer token with the fixed `PUBSUB_VERIFICATION_TOKEN` secret, so every push would get a 401 once that secret is set. Either verify the OIDC token (audience and service account) or drop the flag and use a static token.
4. **Phone false positives.** The phone pattern matches any ten-digit number, so a Unix timestamp such as `1728000000` is reported as a phone number.
5. **Markdown fence stripping.** `enrich()` uses `lstrip("```json")`, which strips a set of characters rather than a prefix. It works for JSON objects because they start with `{`, but `str.removeprefix` would say what it means.
6. **Lint not configured.** `ruff` is in the requirements but has no config and does not run in CI. Run as `uvx ruff check tributary tests` (ruff 0.17.4 on 2026-11-09) it reports 52 findings, mostly import style and `Optional[...]` annotations. With no project config, the count depends on the ruff version and any user-level ruff config on the machine.

## Fixed

- **Aggregator SQL injection and duplicate rows** (2026-11-09). `POST /run` used to format `target_date` from the request body straight into both SQL statements, and re-running a day ran a plain `INSERT` again, doubling that day's summary rows. `target_date` must now be a real date written as `YYYY-MM-DD` (anything else, or a body that is not a JSON object, gets a 400 before any query runs) and reaches BigQuery only as a DATE query parameter. The summary write is one `MERGE` that updates the day's existing groups, inserts new ones and deletes groups that no longer appear, so a retry or backfill leaves one copy of the day. Grouping now uses the same `COALESCE(..., 'unknown')` values the rows are written with, so a NULL and an `'unknown'` category cannot become two rows with the same key. Checked with `tests/test_aggregator.py` (stub client) and, outside the repo, by parsing both statements with sqlglot's BigQuery dialect and running the summary SQL twice in DuckDB. It has not been run against real BigQuery.

## Next steps

- Fix known issues 1 and 2, each with a regression test.
- Decide how push authentication should work (issue 3).
- Add the dossier.
