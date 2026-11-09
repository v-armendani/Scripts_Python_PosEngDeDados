#!/bin/bash
# Executado pelo entrypoint do Postgres apenas na primeira inicialização
# (volume vazio). Cria o usuário de replicação e a publicação usada pelo CDC.
#
# Slots de replicação NÃO são criados aqui: um slot sem consumidor retém WAL
# indefinidamente. Cada consumidor cria o seu (Debezium automaticamente; o
# script do cenário 2 de forma idempotente).
set -euo pipefail

: "${CDC_USER:?CDC_USER não definido}"
: "${CDC_PASSWORD:?CDC_PASSWORD não definido}"
: "${CDC_PUBLICATION:?CDC_PUBLICATION não definido}"

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v cdc_user="$CDC_USER" \
  -v cdc_password="$CDC_PASSWORD" \
  -v cdc_publication="$CDC_PUBLICATION" \
  -v db_name="$POSTGRES_DB" <<'EOSQL'
-- Usuário dedicado ao CDC: só replicação e leitura, sem superuser
CREATE ROLE :"cdc_user" WITH LOGIN REPLICATION PASSWORD :'cdc_password';
GRANT CONNECT ON DATABASE :"db_name" TO :"cdc_user";
GRANT USAGE ON SCHEMA public TO :"cdc_user";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :"cdc_user";
-- Tabelas criadas depois pelo usuário admin também ficam legíveis (snapshot)
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO :"cdc_user";

-- Publicação para o decoder pgoutput; FOR ALL TABLES inclui tabelas futuras
CREATE PUBLICATION :"cdc_publication" FOR ALL TABLES;
EOSQL

echo "CDC setup concluído: role=${CDC_USER} publication=${CDC_PUBLICATION}"
