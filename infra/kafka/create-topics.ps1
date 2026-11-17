$ErrorActionPreference = 'Stop'
$bootstrapServers = if ($env:KAFKA_BOOTSTRAP_SERVERS) {
  $env:KAFKA_BOOTSTRAP_SERVERS
} else {
  'localhost:9092'
}

docker compose -f "$PSScriptRoot\docker-compose.yml" exec kafka `
  /opt/kafka/bin/kafka-topics.sh `
  --create `
  --if-not-exists `
  --topic orders.events `
  --bootstrap-server $bootstrapServers `
  --partitions 3 `
  --replication-factor 3 `
  --config retention.ms=604800000 `
  --config min.insync.replicas=2

docker compose -f "$PSScriptRoot\docker-compose.yml" exec kafka `
  /opt/kafka/bin/kafka-topics.sh `
  --create `
  --if-not-exists `
  --topic orders.events.dlt `
  --bootstrap-server $bootstrapServers `
  --partitions 1 `
  --replication-factor 3 `
  --config retention.ms=2592000000 `
  --config min.insync.replicas=2

docker compose -f "$PSScriptRoot\docker-compose.yml" exec kafka `
  /opt/kafka/bin/kafka-topics.sh `
  --list `
  --bootstrap-server $bootstrapServers
