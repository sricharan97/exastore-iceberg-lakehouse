"""
AWS Glue Job: CDC Bronze Layer
Reads CDC JSON from S3, creates Iceberg tables, applies MERGE INTO for upserts/deletes.
"""

import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'database_name', 'data_lake_bucket',
    'cdc_data_path', 'target_year', 'aws_region'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

DATABASE = args['database_name']
BUCKET = args['data_lake_bucket']
CDC_PATH = args['cdc_data_path']
TARGET_YEAR = args.get('target_year', '2025')
CATALOG = "glue_catalog"

# ── Table definitions ────────────────────────────────────────────────────────
# Schema, primary key, and partitioning for each CDC table.
#
# CDC records have this structure:
#   { "before": {...}, "after": {...}, "op": "c|u|d", "ts_ms": ..., "source": {"lsn": ...} }
#
# Creates/updates: business data is in "after"
# Deletes: business data is in "before"

TABLES = {
    "orders": {
        "pk": "order_id",
        "partition": "days(created_at)",
        "columns": """
            order_id BIGINT,
            user_id BIGINT,
            order_date TIMESTAMP,
            status STRING,
            total_amount DECIMAL(10,2),
            payment_method STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            is_deleted BOOLEAN,
            op STRING,
            ts_ms BIGINT,
            source_lsn BIGINT,
            _processed_at TIMESTAMP
        """,
        "extract": """
            CAST(COALESCE(after.order_id, before.order_id) AS BIGINT) AS order_id,
            CAST(after.user_id AS BIGINT) AS user_id,
            CAST(after.order_date AS TIMESTAMP) AS order_date,
            CAST(after.status AS STRING) AS status,
            CAST(after.total_amount AS DECIMAL(10,2)) AS total_amount,
            CAST(after.payment_method AS STRING) AS payment_method,
            CAST(after.created_at AS TIMESTAMP) AS created_at,
            CAST(after.updated_at AS TIMESTAMP) AS updated_at,
            CAST(after.is_deleted AS BOOLEAN) AS is_deleted,
            op,
            CAST(ts_ms AS BIGINT) AS ts_ms,
            CAST(source.lsn AS BIGINT) AS source_lsn,
            current_timestamp() AS _processed_at
        """,
    },

    "users": {
        "pk": "id",
        "partition": "months(created_at)",
        "columns": """
            id BIGINT,
            first_name STRING,
            last_name STRING,
            email STRING,
            age INT,
            gender STRING,
            state STRING,
            city STRING,
            country STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            is_deleted BOOLEAN,
            op STRING,
            ts_ms BIGINT,
            source_lsn BIGINT,
            _processed_at TIMESTAMP
        """,
        "extract": """
            CAST(COALESCE(after.id, before.id) AS BIGINT) AS id,
            CAST(after.first_name AS STRING) AS first_name,
            CAST(after.last_name AS STRING) AS last_name,
            CAST(after.email AS STRING) AS email,
            CAST(after.age AS INT) AS age,
            CAST(after.gender AS STRING) AS gender,
            CAST(after.state AS STRING) AS state,
            CAST(after.city AS STRING) AS city,
            CAST(after.country AS STRING) AS country,
            CAST(after.created_at AS TIMESTAMP) AS created_at,
            CAST(after.updated_at AS TIMESTAMP) AS updated_at,
            CAST(after.is_deleted AS BOOLEAN) AS is_deleted,
            op,
            CAST(ts_ms AS BIGINT) AS ts_ms,
            CAST(source.lsn AS BIGINT) AS source_lsn,
            current_timestamp() AS _processed_at
        """,
    },

    "products": {
        "pk": "id",
        "partition": "months(created_at)",
        "columns": """
            id BIGINT,
            name STRING,
            description STRING,
            category STRING,
            price DECIMAL(10,2),
            sku STRING,
            stock_quantity INT,
            is_active BOOLEAN,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            is_deleted BOOLEAN,
            op STRING,
            ts_ms BIGINT,
            source_lsn BIGINT,
            _processed_at TIMESTAMP
        """,
        "extract": """
            CAST(COALESCE(after.id, before.id) AS BIGINT) AS id,
            CAST(after.name AS STRING) AS name,
            CAST(after.description AS STRING) AS description,
            CAST(after.category AS STRING) AS category,
            CAST(after.price AS DECIMAL(10,2)) AS price,
            CAST(after.sku AS STRING) AS sku,
            CAST(after.stock_quantity AS INT) AS stock_quantity,
            CAST(after.is_active AS BOOLEAN) AS is_active,
            CAST(after.created_at AS TIMESTAMP) AS created_at,
            CAST(after.updated_at AS TIMESTAMP) AS updated_at,
            CAST(after.is_deleted AS BOOLEAN) AS is_deleted,
            op,
            CAST(ts_ms AS BIGINT) AS ts_ms,
            CAST(source.lsn AS BIGINT) AS source_lsn,
            current_timestamp() AS _processed_at
        """,
    }
}


def create_table(table_name, cfg):
    """Create an Iceberg table if it doesn't exist."""
    fqn = f"{CATALOG}.{DATABASE}.{table_name}"
    location = f"s3://{BUCKET}/warehouse/{DATABASE}/{table_name}"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            {cfg['columns']}
        )
        USING iceberg
        PARTITIONED BY ({cfg['partition']})
        LOCATION '{location}'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)

    logger.info(f"Table ready: {fqn}")


def process_table(table_name, cfg):
    """Read CDC JSON, deduplicate, and MERGE INTO the Iceberg table."""
    pk = cfg["pk"]
    fqn = f"{CATALOG}.{DATABASE}.{table_name}"
    s3_path = f"{CDC_PATH}/year={TARGET_YEAR}/table_name={table_name}/"

    raw = spark.read.option("multiline", "true").json(s3_path)
    raw_count = raw.count()
    logger.info(f"{table_name}: {raw_count} raw CDC records")

    if raw_count == 0:
        return True

    raw.createOrReplaceTempView(f"raw_{table_name}")

    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW cdc_{table_name} AS
        SELECT {cfg['extract']}
        FROM raw_{table_name}
        WHERE op IN ('c', 'u', 'd')
    """)

    # Keep latest record per primary key (source_lsn, then ts_ms)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW staged_{table_name} AS
        SELECT * FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY {pk}
                       ORDER BY source_lsn DESC, ts_ms DESC
                   ) AS _rn
            FROM cdc_{table_name}
            WHERE {pk} IS NOT NULL
        ) ranked
        WHERE _rn = 1
    """)

    staged_count = spark.sql(f"SELECT COUNT(*) AS c FROM staged_{table_name}").collect()[0]['c']
    logger.info(f"{table_name}: {staged_count} records after dedup")

    ops = spark.sql(f"SELECT op, COUNT(*) AS cnt FROM staged_{table_name} GROUP BY op").collect()
    for row in ops:
        op_label = {'c': 'INSERT', 'u': 'UPDATE', 'd': 'DELETE'}.get(row['op'], row['op'])
        logger.info(f"  {op_label}: {row['cnt']}")

    # SELECT * EXCEPT is not supported on Glue 5.0 — drop _rn via an explicit column list
    cols = [c for c in spark.sql(f"SELECT * FROM staged_{table_name}").columns if c != '_rn']
    col_list = ', '.join(cols)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW source_{table_name} AS
        SELECT {col_list} FROM staged_{table_name}
    """)

    spark.sql(f"""
        MERGE INTO {fqn} t
        USING source_{table_name} s
        ON t.{pk} = s.{pk}
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED AND s.op != 'd' THEN INSERT *
    """)

    final_count = spark.sql(f"SELECT COUNT(*) AS c FROM {fqn}").collect()[0]['c']
    logger.info(f"{table_name}: MERGE complete — {final_count} rows in table")
    return True


def main():
    logger.info(f"CDC Bronze Layer — DB: {DATABASE}, Year: {TARGET_YEAR}")

    results = {}
    for table_name, cfg in TABLES.items():
        try:
            create_table(table_name, cfg)
            results[table_name] = process_table(table_name, cfg)
        except Exception as e:
            logger.error(f"{table_name} failed: {e}")
            results[table_name] = False

    succeeded = sum(1 for v in results.values() if v)
    logger.info(f"Summary: {succeeded}/{len(TABLES)} tables processed")
    job.commit()


if __name__ == "__main__":
    main()
