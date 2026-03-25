# Real-Time Data Platform

End-to-end data pipeline using Kafka, Airflow, Spark Structured Streaming, and Cassandra.

## What it does

- Produces user events to Kafka topic `user_data` (via local producer or Airflow DAG)
- Streams those events with Spark
- Writes transformed records into Cassandra table `spark_streams.created_users`

## Project structure

- `docker-compose.yml` — infrastructure stack (Kafka, Airflow, Spark, Cassandra, PostgreSQL)
- `dags/kafka_stream.py` — Airflow DAG `user_automation`
- `producers/kafka_producer.py` — local Kafka producer
- `streaming/kafka_to_cassandra.py` — Spark streaming job
- `requirements/airflow.txt` — Python deps installed in Airflow containers
- `requirements/local.txt` — Python deps for local scripts
- `scripts/entrypoint.sh` — Airflow bootstrap script
- `RUN_INSTRUCTIONS.md` — detailed setup/run/troubleshooting

## Quick start

1. Copy env template:

```bash
cp .env.example .env
```

2. Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

3. Install local dependencies:

```bash
pip install -r requirements/local.txt
```

4. Start infrastructure:

```bash
docker compose up -d
```

5. In one terminal, run the stream:

```bash
source .venv/bin/activate
python streaming/kafka_to_cassandra.py
```

6. In another terminal, produce events:

```bash
source .venv/bin/activate
python producers/kafka_producer.py
```

7. Verify data in Cassandra:

```bash
docker compose exec cassandra_db cqlsh -e "SELECT COUNT(*) FROM spark_streams.created_users;"
```

## Notes

- Host scripts use Kafka at `localhost:9092`
- Airflow containers use Kafka at `broker:29092`
- Spark requires Java 17 on the host when running `streaming/kafka_to_cassandra.py` locally

## Contributing

Contributions are welcome. Please read `CONTRIBUTING.md` before opening a pull request.

## License

This project is licensed under the MIT License. See `LICENSE` for details.
