#!/usr/bin/env bash
set -euo pipefail
VISIBILITY="${VISIBILITY:-public}"
ORG="${ORG:-}"; owner=""; [ -n "$ORG" ] && owner="${ORG}/"
gh repo create "${owner}tributary" --${VISIBILITY} --source=. --remote=origin --push \
  --description "Real-time event intelligence: Pub/Sub → Cloud Run → BigQuery with LLM enrichment and HIPAA-readiness patterns"
echo "Done: https://github.com/${owner}tributary"
