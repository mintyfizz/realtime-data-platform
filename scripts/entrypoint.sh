#!/usr/bin/env bash
set -euo pipefail

AIRFLOW_HOME="${AIRFLOW_HOME:-/opt/airflow}"
REQ_FILE="${AIRFLOW_HOME}/requirements.txt"

if [ -f "${REQ_FILE}" ]; then
	"$(command -v pip)" install --user -r "${REQ_FILE}"
fi

airflow db init

ADMIN_USERNAME="${AIRFLOW_ADMIN_USERNAME:-admin}"
ADMIN_PASSWORD="${AIRFLOW_ADMIN_PASSWORD:-admin}"
ADMIN_FIRSTNAME="${AIRFLOW_ADMIN_FIRSTNAME:-admin}"
ADMIN_LASTNAME="${AIRFLOW_ADMIN_LASTNAME:-admin}"
ADMIN_EMAIL="${AIRFLOW_ADMIN_EMAIL:-admin@example.com}"

if ! airflow users list | awk 'NR>2 {print $1}' | grep -qx "${ADMIN_USERNAME}"; then
	airflow users create \
		--username "${ADMIN_USERNAME}" \
		--firstname "${ADMIN_FIRSTNAME}" \
		--lastname "${ADMIN_LASTNAME}" \
		--role Admin \
		--email "${ADMIN_EMAIL}" \
		--password "${ADMIN_PASSWORD}"
fi

if [ "$#" -eq 0 ]; then
	exec airflow webserver
fi

exec "$@"
