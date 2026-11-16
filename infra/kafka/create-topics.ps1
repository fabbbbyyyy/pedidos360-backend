$ErrorActionPreference = 'Stop'

docker compose -f "$PSScriptRoot\docker-compose.yml" exec kafka `
  /opt/kafka/bin/kafka-topics.sh `
  --create `
  --if-not-exists `
  --topic orders.events `
  --bootstrap-server localhost:9092 `
  --partitions 3 `
  --replication-factor 1 `
  --config retention.ms=604800000

docker compose -f "$PSScriptRoot\docker-compose.yml" exec kafka `
  /opt/kafka/bin/kafka-topics.sh `
  --create `
  --if-not-exists `
  --topic orders.events.dlt `
  --bootstrap-server localhost:9092 `
  --partitions 1 `
  --replication-factor 1 `
  --config retention.ms=2592000000

docker compose -f "$PSScriptRoot\docker-compose.yml" exec kafka `
  /opt/kafka/bin/kafka-topics.sh `
  --list `
  --bootstrap-server localhost:9092
