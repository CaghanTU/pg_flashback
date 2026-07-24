#!/usr/bin/env bash
# Mechanically verify qualification provenance honesty.
#
# A Git commit cannot self-contain its own final hash. Evidence that claims
# "tested_commit == this evidence commit" while the commit also changes code is
# rejected. Distinguishes:
#   source_commit / source_tree  — what was actually tested
#   evidence_summary_commit      — optional later docs-only commit
#   qualification_artifact_sha256 / extension_binary_sha256 / package_sha256
#
# Usage:
#   ./scripts/verify_qualification_provenance.sh [evidence.json]
# Exit 0 when provenance is consistent; non-zero on violations.

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EVIDENCE="${1:-}"

if [[ -z "$EVIDENCE" ]]; then
    echo "usage: $0 <qualification-evidence.json>" >&2
    exit 2
fi

die() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "OK: $*"; }

[[ -f "$EVIDENCE" ]] || die "evidence file missing: $EVIDENCE"
command -v jq >/dev/null || die "jq is required"

SOURCE_COMMIT="$(jq -r '.provenance.source_commit // .source_commit // .tested_commit // empty' "$EVIDENCE")"
[[ -n "$SOURCE_COMMIT" ]] || die "evidence lacks provenance.source_commit / tested_commit"

git -C "$ROOT" cat-file -e "${SOURCE_COMMIT}^{commit}" 2>/dev/null \
    || die "source_commit $SOURCE_COMMIT is not a commit in this repository"

SOURCE_TREE="$(git -C "$ROOT" rev-parse "${SOURCE_COMMIT}^{tree}")"
CLAIMED_TREE="$(jq -r '.provenance.source_tree // .source_tree // empty' "$EVIDENCE")"
if [[ -n "$CLAIMED_TREE" && "$CLAIMED_TREE" != "$SOURCE_TREE" ]]; then
    die "source_tree mismatch: evidence=$CLAIMED_TREE git=$SOURCE_TREE"
fi
ok "source_commit $SOURCE_COMMIT tree $SOURCE_TREE"

# If evidence claims final_head / evidence_commit, ensure that commit either
# equals source_commit or differs only in documentation/evidence paths.
EVIDENCE_COMMIT="$(jq -r '.provenance.evidence_summary_commit // .evidence_summary_commit // .final_head // empty' "$EVIDENCE")"
if [[ -n "$EVIDENCE_COMMIT" && "$EVIDENCE_COMMIT" != "$SOURCE_COMMIT" ]]; then
    git -C "$ROOT" cat-file -e "${EVIDENCE_COMMIT}^{commit}" 2>/dev/null \
        || die "evidence_summary_commit $EVIDENCE_COMMIT is not a commit"
    mapfile -t CHANGED < <(git -C "$ROOT" diff --name-only "$SOURCE_COMMIT" "$EVIDENCE_COMMIT")
    for path in "${CHANGED[@]}"; do
        case "$path" in
            docs/*|CHANGELOG.md|README.md|*.md)
                ;;
            *)
                die "evidence_summary_commit changes executable path: $path (source=$SOURCE_COMMIT evidence=$EVIDENCE_COMMIT)"
                ;;
        esac
    done
    ok "evidence_summary_commit $EVIDENCE_COMMIT differs only in docs/evidence from source"
elif [[ -n "$EVIDENCE_COMMIT" ]]; then
    ok "evidence_summary_commit equals source_commit"
fi

# Reject self-hash churn claims: tested_commit must not equal a commit whose
# only purpose was to rewrite tested_commit inside the same file.
if [[ -n "$EVIDENCE_COMMIT" ]]; then
    if git -C "$ROOT" show --name-only --pretty=format: "$EVIDENCE_COMMIT" \
        | grep -q 'qualification/.*evidence'; then
        BODY="$(git -C "$ROOT" show -s --format=%B "$EVIDENCE_COMMIT")"
        if grep -qiE 'stamp(ed)? (tested_commit|HEAD)|embed(s|ded)? (its )?own (commit )?hash|self-?hash churn|tested_commit.*=.*HEAD' <<<"$BODY"; then
            die "evidence commit message suggests self-hash stamping; use source_commit + separate artifact instead"
        fi
    fi
fi

# Optional binary digests: if present, must look like sha256 hex.
for field in extension_binary_sha256 cli_binary_sha256 package_sha256 qualification_artifact_sha256; do
    val="$(jq -r --arg f "$field" '
        .provenance[$f] // .[$f] // empty
    ' "$EVIDENCE")"
    if [[ -n "$val" && "$val" != "null" ]]; then
        [[ "$val" =~ ^[0-9a-f]{64}$ ]] || die "$field is not a sha256 hex digest: $val"
        ok "$field format"
    fi
done

# Honesty: never claim clean-host / 24h soak passed without explicit true/passed.
CLEAN_HOST="$(jq -r '.clean_host_smoke // .provenance.clean_host_smoke // "not_run"' "$EVIDENCE")"
SOAK="$(jq -r '.exact_rc_24h_soak // .provenance.exact_rc_24h_soak // "not_run"' "$EVIDENCE")"
# Accept plain statuses or annotated "not_run (...)" / "blocked (...)" forms.
normalize_gate_status() {
    local raw=$1
    case "$raw" in
        passed|true) printf '%s' passed ;;
        not_run|NOT_RUN|not_run\ *|NOT_RUN\ *) printf '%s' not_run ;;
        skipped|skipped\ *) printf '%s' skipped ;;
        blocked|blocked\ *) printf '%s' blocked ;;
        *) printf '%s' "$raw" ;;
    esac
}
CLEAN_HOST_NORM="$(normalize_gate_status "$CLEAN_HOST")"
SOAK_NORM="$(normalize_gate_status "$SOAK")"
case "$CLEAN_HOST_NORM" in
    passed|not_run|skipped|blocked) ;;
    *) die "clean_host_smoke has unrecognized status: $CLEAN_HOST" ;;
esac
case "$SOAK_NORM" in
    passed|not_run|skipped|blocked) ;;
    *) die "exact_rc_24h_soak has unrecognized status: $SOAK" ;;
esac
ok "clean_host=$CLEAN_HOST soak=$SOAK"

echo "PASS: provenance checks for $EVIDENCE"
