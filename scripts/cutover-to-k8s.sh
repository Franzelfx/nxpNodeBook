#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# File:        scripts/cutover-to-k8s.sh
# Author:      Fabian Franz
# Company:     NexPatch AI
# Created:     2026-09-13
#
# Description:
#   Moves nxpNodeBook from the compose stack on the host into the prod
#   namespace, on the SAME pgdata directory, with the shortest gap the
#   sequence allows. Everything before "cutover" is idempotent and already
#   done once; the cutover itself is the part that needs a human on the
#   keyboard, because a helm upgrade against prod is a production deploy.
#
#   Run from the warehouse host as the user that owns the checkout:
#     ./scripts/cutover-to-k8s.sh            # do it
#     ./scripts/cutover-to-k8s.sh --check    # preconditions + helm diff only
#
#   What it does, in order:
#     1. preconditions: image in the registry, secret present, chart diff
#        shows exactly the six new prod objects and nothing else
#     2. `docker compose down`  — collectors close their venue sessions
#        (stream_runs.reason = shutdown), the db releases pgdata
#     3. `make deploy ENV=prod SERVICES=`  — SERVICES must be EMPTY, or the
#        Makefile sets a timestamp tag on nxp-ingest/confighead-ui that does
#        not exist (k8s-deploy-traps #1)
#     4. wait for book-db and nxp-book-api, then verify /health.ok from a pod
#        and that the new sessions continue the same database
#     5. staging ExternalName + monitoring manifests (kubectl apply)
#
#   Rollback: `docker compose up -d` in this directory brings the compose
#   stack back on the same pgdata after `kubectl -n prod scale deploy
#   book-db nxp-book-api --replicas=0`. Never run both at once — two
#   postmasters on one data directory.
#
# CONFIDENTIAL – Proprietary. Unauthorized copying or distribution is prohibited.
# © 2026 NexPatch AI. All rights reserved.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WH_K8S="$(cd "${HERE}/../nxpWarehouse/k8s" && pwd)"
TAG="${IMAGE_TAG:-prod-20260913-initial}"
CHECK_ONLY=false
[[ "${1:-}" == "--check" ]] && CHECK_ONLY=true

say() { printf '\n\033[1m>> %s\033[0m\n' "$*"; }
die() { printf '\n\033[31mABORT: %s\033[0m\n' "$*" >&2; exit 1; }

# ── 1. preconditions ────────────────────────────────────────────────────────
say "image localhost:5000/nxp-book-api:${TAG} in the registry?"
curl -fsS http://localhost:5000/v2/nxp-book-api/tags/list | grep -q "\"${TAG}\"" \
  || die "tag ${TAG} missing — build + push first (README §7, then docker tag/push)"

say "secret nxp-book-env in prod?"
kubectl -n prod get secret nxp-book-env >/dev/null || die "create it: kubectl -n prod create secret generic nxp-book-env --from-env-file=${HERE}/.env"

say "chart tag matches?"
grep -q "tag: ${TAG}" "${WH_K8S}/helm/nexpatch/values-prod.yaml" || die "values-prod.yaml does not pin ${TAG}"

say "helm diff against prod — must be exactly the six nxpNodeBook objects"
DIFF="$(cd "${WH_K8S}" && helm diff upgrade nexpatch helm/nexpatch -n prod \
        -f helm/nexpatch/values.yaml -f helm/nexpatch/values-prod.yaml 2>&1 | grep -E '^prod, ' || true)"
echo "${DIFF}"
EXPECTED=6
[[ "$(echo "${DIFF}" | grep -c 'has been added')" -eq "${EXPECTED}" ]] || die "expected ${EXPECTED} added objects"
[[ "$(echo "${DIFF}" | grep -c 'has changed')" -eq 0 ]] || die "chart drift on an existing object — resolve before deploying (k8s-deploy-traps #2)"

say "pgdata on the array, owned by postgres (uid 70)?"
stat -c '%u %n' /mnt/warehouse/nxp-book/pgdata | grep -q '^70 ' || die "unexpected owner on pgdata"

if ${CHECK_ONLY}; then say "check only — all preconditions hold"; exit 0; fi

# ── 2. stop the compose stack ───────────────────────────────────────────────
say "$(date -u +%FT%TZ) stopping the compose stack (gap begins)"
( cd "${HERE}" && docker compose down )
docker ps --format '{{.Names}}' | grep -q '^nxp-book-' && die "compose containers still running"

# ── 3. helm upgrade prod ────────────────────────────────────────────────────
say "$(date -u +%FT%TZ) helm upgrade prod (SERVICES= deliberately empty)"
( cd "${WH_K8S}" && make deploy ENV=prod SERVICES= )

# ── 4. wait + verify ────────────────────────────────────────────────────────
say "waiting for book-db"
kubectl -n prod rollout status deploy/book-db --timeout=180s
say "waiting for nxp-book-api (readiness = contract ok, needs the first grid ≈ 5 min)"
kubectl -n prod rollout status deploy/nxp-book-api --timeout=600s
say "$(date -u +%FT%TZ) gap ends"

say "health from inside the cluster"
kubectl -n prod exec deploy/nxp-ingest -- curl -fsS -m 10 http://nxp-book-api:9130/health \
  | python3 -c "import json,sys; h=json.load(sys.stdin); print('ok', h['ok'], 'capture_fresh', h['capture_fresh']); [print(' ', s['label'], 'connected', s['connected'], 'synced', s['synced'], 'lag', s['event_lag_seconds']) for s in h['capture_streams']]"

say "same database, sessions continue after the shutdown rows"
kubectl -n prod exec deploy/book-db -- psql -U nxp_book -d book -Atc \
  "select id, venue, connected_at, disconnected_at, reason from stream_runs order by id desc limit 6;"

# ── 5. staging + monitoring ─────────────────────────────────────────────────
say "staging ExternalName (helm-adoptable), monitoring, admin ingress"
kubectl apply -f "${HERE}/scripts/staging-externalname-nxp-book-api.yaml"
kubectl apply -f "${WH_K8S}/monitoring/node-health-exporter.yaml"
kubectl apply -f "${WH_K8S}/monitoring/probes-internal-uis.yaml"
kubectl apply -f "${WH_K8S}/monitoring/postgres-exporters.yaml"
kubectl apply -f "${WH_K8S}/admin-services/ingress-internal-uis.yaml"

say "done — nxp-book-api:9130 is a prod Service; staging resolves it via ExternalName"
