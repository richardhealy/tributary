# Progress

## Where things stand (2026-10-06)

- Parser, LLM enrichment, BigQuery schema and writer, the Pub/Sub push subscriber and the daily aggregator are written.
- 35 tests pass with no GCP credentials or LLM keys: 19 parser, 5 enrichment, 5 writer and 6 service start-up tests.
- Both services start from the repo root and from their Docker images. Before 2026-10-06 neither could start, because the code sat at the repo root while every import used `tributary.*`.
- Not yet deployed to a real Google Cloud project. The LLM and BigQuery calls have only run against stubs.
- No Cloud Scheduler job or Pub/Sub definitions in `infra/` yet.
- No dossier yet (`docs/dossier/`).

## Known issues

Each of these was reproduced against the code on 2026-10-06.

1. **Aggregator SQL injection and duplicate rows.** `POST /run` formats `target_date` from the request body straight into both SQL statements (`tributary/scheduler/aggregator.py`). It should be checked as an ISO date and passed as a BigQuery query parameter. Re-running a day (a Cloud Scheduler retry or a manual backfill) runs the `INSERT` again and doubles that day's summary rows; it needs a delete-then-insert or a `MERGE`.
2. **Amount normalisation guesses units from size.** `parse()` treats any amount under 1000 as dollars and anything else as cents, and reads `amount` and `amount_cents` the same way. Measured: `{"amount": 1500.0}` gives 1500 cents, `{"amount_cents": 500}` gives 50000, `{"amount": 19.99}` gives 1998 (float truncation), and `{"amount": 0}` gives `None`.
3. **Non-object JSON crashes the parser.** `parse(b"[1,2]")`, `parse(b'"hello"')` and `parse(b"null")` raise `AttributeError`, so the subscriber returns 500 and Pub/Sub keeps redelivering the message. It should become an `UNKNOWN` event with a parse error, like invalid JSON does.
4. **Push authentication does not match the documented subscription.** The README creates the push subscription with `--push-auth-service-account`, which makes Pub/Sub send a Google-signed OIDC token. The subscriber compares the bearer token with the fixed `PUBSUB_VERIFICATION_TOKEN` secret, so every push would get a 401 once that secret is set. Either verify the OIDC token (audience and service account) or drop the flag and use a static token.
5. **Phone false positives.** The phone pattern matches any ten-digit number, so a Unix timestamp such as `1728000000` is reported as a phone number.
6. **Markdown fence stripping.** `enrich()` uses `lstrip("```json")`, which strips a set of characters rather than a prefix. It works for JSON objects because they start with `{`, but `str.removeprefix` would say what it means.
7. **Lint not configured.** `ruff` is in the requirements but has no config and does not run in CI. With ruff 0.16's defaults it reports 47 findings (mostly import style and `Optional[...]` annotations).

## Next steps

- Fix known issues 1 to 3, each with a regression test.
- Decide how push authentication should work (issue 4).
- Add the dossier.
