#!/usr/bin/env bash
# Tags: no-fasttest
# Uses S3 and IcebergLocal.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -euo pipefail

dir=$(mktemp -d "${CLICKHOUSE_TMP}/parquet_field_ids.XXXXXX")
key="${CLICKHOUSE_TEST_UNIQUE_NAME}/$(basename "$dir")/data.parquet"
lake="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}_$(basename "$dir")"
trap 'rm -rf "$dir" "$lake"' EXIT

# The object name, projection order and IDs are independent choices.
$CLICKHOUSE_CLIENT --query "
    INSERT INTO FUNCTION s3(s3_conn, filename='$key', format=Parquet,
        structure='tags Array(Tuple(label String, counts Map(String, Nullable(Int64)))), id Int64')
    SELECT [tuple('x', map('in', 5))], 7
    SETTINGS output_format_parquet_string_as_string = 1, output_format_parquet_field_ids = {
        'id':'4', 'tags.element.counts.key':'60', 'tags':'19', 'tags.element.label':'8',
        'tags.element.counts.value':'17', 'tags.element':'89', 'tags.element.counts':'22'
    }"
$CLICKHOUSE_CLIENT --query "
    SELECT data FROM s3(s3_conn, filename='$key', format=RawBLOB, structure='data String') FORMAT RawBLOB" > "$dir/s3.parquet"

$CLICKHOUSE_CLIENT --query "SELECT [1::Int64] AS tags FORMAT Parquet" > "$dir/default.parquet"

# Use the table function so the query's format settings reach the Iceberg writer.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE field_ids_lake (id Int64, label String) ENGINE = IcebergLocal('$lake');
    INSERT INTO FUNCTION icebergLocal('$lake') SELECT 7, 'x'
        SETTINGS allow_insert_into_iceberg = 1, output_format_parquet_field_ids = {'id':'99', 'label':'98'};
    SELECT data FROM file('$lake/data/*.parquet', RawBLOB, 'data String') FORMAT RawBLOB;
    DROP TABLE field_ids_lake;" > "$dir/catalog.parquet"

python3 - "$dir" <<'PY'
from pathlib import Path
import sys
import pyarrow.parquet as pq

directory = Path(sys.argv[1])
parquet = pq.ParquetFile(directory / "s3.parquet")
schema = parquet.schema_arrow

def field_id(field):
    return (field.metadata or {}).get(b"PARQUET:field_id")

assert schema.names == ["tags", "id"]
tags = schema.field("tags")
element = tags.type.value_field
counts = element.type.field("counts")
fields = [tags, element, element.type.field("label"), counts, counts.type.key_field, counts.type.item_field, schema.field("id")]
print("S3 field IDs:", *(int(field_id(field)) for field in fields))
assert parquet.read().to_pylist() == [{"tags": [{"label": "x", "counts": [("in", 5)]}], "id": 7}]

tags = pq.read_schema(directory / "default.parquet").field("tags")
assert field_id(tags) is None and field_id(tags.type.value_field) is None

schema = pq.read_schema(directory / "catalog.parquet")
print("Iceberg field IDs:", *(int(field_id(schema.field(name))) for name in ("id", "label")))
PY

reject_mapping()
{
    local fields=$1 ids=$2
    if $CLICKHOUSE_LOCAL --query "SELECT $fields SETTINGS output_format_parquet_field_ids = $ids FORMAT Parquet" > /dev/null 2> "$dir/error"; then
        echo "Unexpectedly accepted field IDs $ids" >&2
        exit 1
    fi
    grep -q '(BAD_ARGUMENTS)' "$dir/error" || { cat "$dir/error" >&2; exit 1; }
}

reject_mapping '[1] AS a' "{'a':'1'}"
reject_mapping '1 AS a' "{'a':'1','b':'2'}"
reject_mapping '1 AS a, 2 AS b' "{'a':'1','b':'1'}"
reject_mapping '1 AS a' "{'a':'1','a':'2'}"
reject_mapping '1 AS a' "{'a':'0'}"
reject_mapping '1 AS a' "{'a':'2147483648'}"
reject_mapping '1 AS a' "{'a':'5x'}"
reject_mapping "CAST(tuple(1), 'Tuple(b UInt8)') AS a, 2 AS \`a.b\`" "{'a':'1','a.b':'2'}"
