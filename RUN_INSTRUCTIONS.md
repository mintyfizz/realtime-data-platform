# Run Instructions

These steps are machine-independent and assume only that you cloned this repository.

## Prerequisites

- Docker Desktop (or Docker Engine + Docker Compose)
- Python 3.10+
- Java 17 (required for local Spark execution)

## Initial setup

From the repository root:

```bash
cp .env.example .env
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements/local.txt
```

## Start the platform

```bash
docker compose up -d
docker compose ps
```

UI endpoints:

- Airflow: http://localhost:8080
- Control Center: http://localhost:9021
- Spark UI: http://localhost:9090

## Run end-to-end flow

Terminal A (streaming consumer):

```bash
source .venv/bin/activate
python streaming/kafka_to_cassandra.py
```

Terminal B (producer):

```bash
source .venv/bin/activate
python producers/kafka_producer.py
```

Verify rows in Cassandra:

```bash
docker compose exec cassandra_db cqlsh -e "SELECT COUNT(*) FROM spark_streams.created_users;"
```

## Airflow producer path (optional)

```bash
docker compose exec scheduler airflow dags list
docker compose exec scheduler airflow dags unpause user_automation
docker compose exec scheduler airflow dags trigger user_automation
```

## Safe reset

```bash
pkill -f "kafka_to_cassandra.py" || true
pkill -f "SparkSubmit.*KafkaSparkDataStreaming" || true
pkill -f "pyspark-shell" || true
find /tmp -maxdepth 1 -name 'spark_checkpoint*' -exec rm -rf {} + 2>/dev/null || true
docker compose down
docker compose up -d
```

## Troubleshooting

- `zsh: command not found: --master`
  - Run `spark-submit --master ...` correctly, or use `python streaming/kafka_to_cassandra.py`.
- `unknown exec flag -i`
  - Use `docker compose exec ...`, not shell `exec`.
- `CONCURRENT_STREAM_LOG_UPDATE`
  - Multiple stream jobs are sharing one checkpoint path. Stop duplicates and clear checkpoints.
- Kafka offset/data-loss startup errors
  - The stream already sets `.option("failOnDataLoss", "false")`.
