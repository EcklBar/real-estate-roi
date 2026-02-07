#!/bin/bash
set -e

echo "Initializing PostGIS extension..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS postgis;
    CREATE EXTENSION IF NOT EXISTS postgis_topology;
    SELECT PostGIS_Version();
EOSQL
echo "PostGIS initialized successfully."
