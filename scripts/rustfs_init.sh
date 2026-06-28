#!/usr/bin/env bash
set -euo pipefail

RUSTFS_ENDPOINT="${RUSTFS_ENDPOINT:-http://rustfs:9000}"
RUSTFS_BUCKET="${RUSTFS_BUCKET:-market-data}"
RUSTFS_PREFIX="${RUSTFS_PREFIX:-live/}"
RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfsadmin}"
INGEST_WEBHOOK_URL="${INGEST_WEBHOOK_URL:-http://quest-router:9010/ingest/events}"
AWS_REGION="${AWS_REGION:-us-east-1}"

export AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY"
export AWS_DEFAULT_REGION="$AWS_REGION"

echo "Waiting for RustFS at ${RUSTFS_ENDPOINT}..."
for _ in $(seq 1 60); do
  if aws --endpoint-url "$RUSTFS_ENDPOINT" s3 ls >/dev/null 2>&1; then
    echo "RustFS is ready"
    break
  fi
  sleep 2
done

if ! aws --endpoint-url "$RUSTFS_ENDPOINT" s3 ls >/dev/null 2>&1; then
  echo "RustFS did not become ready in time" >&2
  exit 1
fi

if aws --endpoint-url "$RUSTFS_ENDPOINT" s3 ls "s3://${RUSTFS_BUCKET}" >/dev/null 2>&1; then
  echo "Bucket ${RUSTFS_BUCKET} already exists"
else
  echo "Creating bucket ${RUSTFS_BUCKET}"
  aws --endpoint-url "$RUSTFS_ENDPOINT" s3 mb "s3://${RUSTFS_BUCKET}"
fi

NOTIFICATION_FILE="$(mktemp)"
cat >"$NOTIFICATION_FILE" <<EOF
{
  "WebhookConfiguration": [
    {
      "Id": "quest-router-ingest",
      "Webhook": {
        "Url": "${INGEST_WEBHOOK_URL}"
      },
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [
            {"Name": "prefix", "Value": "${RUSTFS_PREFIX}"},
            {"Name": "suffix", "Value": ".feather"}
          ]
        }
      }
    }
  ]
}
EOF

echo "Configuring bucket notifications (webhook -> ${INGEST_WEBHOOK_URL})"
if aws --endpoint-url "$RUSTFS_ENDPOINT" s3api put-bucket-notification-configuration \
  --bucket "$RUSTFS_BUCKET" \
  --notification-configuration "file://${NOTIFICATION_FILE}"; then
  echo "Bucket notification configured"
else
  echo "WARN: put-bucket-notification-configuration failed; reconcile poller will still ingest objects" >&2
fi

rm -f "$NOTIFICATION_FILE"
echo "RustFS init complete"
