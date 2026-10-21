"""
AWS Glue Job: Events Bronze Layer
Reads clickstream events JSON from S3, validates data quality,
writes to Iceberg table via SparkSQL.
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
    'events_s3_path', 'batch_size', 'aws_region'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

DATABASE = args['database_name']
BUCKET = args['data_lake_bucket']
EVENTS_PATH = args['events_s3_path']
CATALOG = "glue_catalog"

FQN = f"{CATALOG}.{DATABASE}.events"
LOCATION = f"s3://{BUCKET}/warehouse/{DATABASE}/events"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {FQN} (
        event_id STRING,
        event_type STRING,
        user_id BIGINT,
        product_id BIGINT,
        event_timestamp TIMESTAMP,
        session_id STRING,
        page_url STRING,
        source STRING,
        quantity BIGINT,
        cart_value DOUBLE,
        click_position STRING,
        click_target STRING,
        currency STRING,
        order_id BIGINT,
        order_value DOUBLE,
        payment_method STRING,
        referrer STRING,
        processing_time TIMESTAMP,
        year INT,
        month INT,
        day INT
    )
    USING iceberg
    PARTITIONED BY (year, month)
    LOCATION '{LOCATION}'
    TBLPROPERTIES (
        'format-version' = '2',
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'snappy'
    )
""")

logger.info(f"Table ready: {FQN}")

# ── Read events JSON ─────────────────────────────────────────────────────────

raw = spark.read.option("multiline", "true").json(EVENTS_PATH)
raw_count = raw.count()
logger.info(f"Read {raw_count} raw event records")

if raw_count == 0:
    logger.warning("No events found, exiting")
    job.commit()
    sys.exit(0)

raw.createOrReplaceTempView("raw_events")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW parsed_events AS
    SELECT
        CAST(event_id AS STRING) AS event_id,
        CAST(event_type AS STRING) AS event_type,
        CAST(user_id AS BIGINT) AS user_id,
        CAST(product_id AS BIGINT) AS product_id,
        TO_TIMESTAMP(event_timestamp) AS event_timestamp,
        CAST(session_id AS STRING) AS session_id,
        CAST(page_url AS STRING) AS page_url,
        CAST(source AS STRING) AS source,
        CAST(quantity AS BIGINT) AS quantity,
        CAST(cart_value AS DOUBLE) AS cart_value,
        CAST(click_position AS STRING) AS click_position,
        CAST(click_target AS STRING) AS click_target,
        CAST(currency AS STRING) AS currency,
        CAST(order_id AS BIGINT) AS order_id,
        CAST(order_value AS DOUBLE) AS order_value,
        CAST(payment_method AS STRING) AS payment_method,
        CAST(referrer AS STRING) AS referrer,
        current_timestamp() AS processing_time,
        CAST(YEAR(TO_TIMESTAMP(event_timestamp)) AS INT) AS year,
        CAST(MONTH(TO_TIMESTAMP(event_timestamp)) AS INT) AS month,
        CAST(DAY(TO_TIMESTAMP(event_timestamp)) AS INT) AS day
    FROM raw_events
    WHERE event_id IS NOT NULL
      AND event_timestamp IS NOT NULL
""")

# ── Data quality checks ──────────────────────────────────────────────────────
# Log invalid event types, future timestamps, and null user_id (non-failing).

dq = spark.sql("""
    SELECT
        COUNT(*) AS total_records,
        SUM(CASE WHEN event_type IS NULL
                  OR event_type NOT IN ('page_view', 'add_to_cart', 'purchase', 'click')
                 THEN 1 ELSE 0 END) AS invalid_event_type,
        SUM(CASE WHEN event_timestamp > current_timestamp() THEN 1 ELSE 0 END) AS future_timestamps,
        SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS null_user_id
    FROM parsed_events
""").collect()[0]

logger.info(f"Data quality — total: {dq['total_records']}, "
            f"invalid event_type: {dq['invalid_event_type'] or 0}, "
            f"future timestamps: {dq['future_timestamps'] or 0}, "
            f"null user_id: {dq['null_user_id'] or 0}")

# ── Filter and insert ────────────────────────────────────────────────────────

spark.sql("""
    CREATE OR REPLACE TEMP VIEW valid_events AS
    SELECT * FROM parsed_events
    WHERE event_type IN ('page_view', 'add_to_cart', 'purchase', 'click')
      AND event_timestamp <= current_timestamp()
      AND user_id IS NOT NULL
      AND user_id >= 1
""")

valid_count = spark.sql("SELECT COUNT(*) AS c FROM valid_events").collect()[0]['c']
logger.info(f"{valid_count} valid events to insert")

spark.sql(f"INSERT INTO {FQN} SELECT * FROM valid_events")

final_count = spark.sql(f"SELECT COUNT(*) AS c FROM {FQN}").collect()[0]['c']
logger.info(f"Events table now has {final_count} total rows")

breakdown = spark.sql(f"""
    SELECT event_type, COUNT(*) AS cnt FROM {FQN} GROUP BY event_type ORDER BY cnt DESC
""").collect()
for row in breakdown:
    logger.info(f"  {row['event_type']}: {row['cnt']}")

logger.info("Events bronze layer complete")
job.commit()
