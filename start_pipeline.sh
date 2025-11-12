set -e

cd "$(dirname "$0")"

echo "Starting all Docker services..."
docker-compose up -d

echo "Waiting for NiFi to start..."
until curl -k --silent https://localhost:8443/nifi >/dev/null 2>&1; do
  printf '.'
  sleep 5
done
echo -e "\n✅ NiFi webserver reachable."

echo "Authenticating to NiFi..."
NIFI_USER="admin"
NIFI_PASS="adminadminadmin"

# Get auth token
TOKEN=$(curl -k -s -X POST \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=${NIFI_USER}&password=${NIFI_PASS}" \
  https://localhost:8443/nifi-api/access/token)

if [ -z "$TOKEN" ]; then
  echo "❌ Failed to get NiFi auth token. Check credentials or NiFi status."
  exit 1
fi
echo "Authentication successful."

echo "Retrieving NiFi Root Process Group ID..."
PG_ID=$(curl -k -s -H "Authorization: Bearer $TOKEN" https://localhost:8443/nifi-api/process-groups/root | jq -r '.id')

if [ -z "$PG_ID" ] || [ "$PG_ID" == "null" ]; then
  echo "❌ Could not retrieve process group ID. Make sure NiFi is fully started."
  exit 1
fi
echo "✅ Found Root Process Group ID: $PG_ID"

echo "▶️ Starting NiFi flow..."
curl -k -X PUT \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  "https://localhost:8443/nifi-api/flow/process-groups/$PG_ID" \
  -d "{\"id\": \"$PG_ID\", \"state\": \"RUNNING\"}" >/dev/null

echo "NiFi flow started successfully."

echo "Waiting for Spark Master..."
until curl -s http://localhost:8070 >/dev/null 2>&1; do
  printf '.'
  sleep 5
done
echo "Spark Master is up."

echo "🚀 Submitting Spark Structured Streaming job..."
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  /opt/spark-apps/streaming_app.py

echo "🎉 Pipeline running successfully."