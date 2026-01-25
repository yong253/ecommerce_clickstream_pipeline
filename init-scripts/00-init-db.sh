#!/bin/bash
set -e

# PostgreSQL 다중 데이터베이스 초기화 함수
create_multiple_postgresql_databases() {
    local database_list=$(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' ')

    for db in $database_list; do
        echo "Checking database: $db"
        # 데이터베이스가 이미 존재하는지 확인
        if psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres -lqt | cut -d \| -f 1 | grep -qw "$db"; then
            echo "Database $db already exists"
        else
            echo "Creating database: $db"
            psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-EOSQL
                CREATE DATABASE $db;
                GRANT ALL PRIVILEGES ON DATABASE $db TO $POSTGRES_USER;
EOSQL
        fi
    done
}

# POSTGRES_MULTIPLE_DATABASES 환경 변수가 설정되어 있다면 다중 데이터베이스 생성
if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    create_multiple_postgresql_databases
fi
