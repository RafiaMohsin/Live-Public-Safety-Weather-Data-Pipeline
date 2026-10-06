"""Delta helpers: table creation from explicit schemas, MERGE INTO upserts, quarantine writes."""
import uuid

from pyspark.sql import functions as F

from .config import QUARANTINE_TABLE
from .schemas import QUARANTINE_KEYS, QUARANTINE_SCHEMA


def create_table(spark, name, schema):
    """Create an empty Delta table with an explicit schema (no-op if it already exists)."""
    spark.createDataFrame([], schema).write.format("delta").mode("ignore").saveAsTable(name)


def conform(df, schema):
    """Select + cast columns so the DataFrame matches the target schema exactly (fails on typos)."""
    return df.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


def merge_into(spark, target, source_df, keys, hash_col=None):
    """Idempotent upsert. Returns (rows_inserted, rows_updated).

    - matched + (hash differs) -> UPDATE   (only when hash_col is given)
    - not matched              -> INSERT
    Re-running with identical data therefore inserts and updates nothing.
    """
    view = "_src_" + uuid.uuid4().hex[:8]
    source_df.createOrReplaceTempView(view)
    on = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    matched = (f"WHEN MATCHED AND NOT (t.`{hash_col}` <=> s.`{hash_col}`) THEN UPDATE SET *"
               if hash_col else "")
    sql = (f"MERGE INTO {target} AS t USING {view} AS s ON {on} "
           f"{matched} WHEN NOT MATCHED THEN INSERT *")
    try:
        res = spark.sql(sql).collect()[0].asDict()
    finally:
        spark.catalog.dropTempView(view)
    return int(res.get("num_inserted_rows") or 0), int(res.get("num_updated_rows") or 0)


def write_quarantine(spark, cfg, bad_df, layer, source, batch_id):
    """Upsert rejected records (columns: record_key, reason, raw_payload) into quarantine_records."""
    q = bad_df.select(
        F.lit(layer).alias("layer"), F.lit(source).alias("source"), "record_key", "reason",
        F.col("raw_payload").substr(1, 100000).alias("raw_payload"),
        F.lit(batch_id).alias("batch_id"), F.current_timestamp().alias("load_timestamp"),
    ).dropDuplicates(QUARANTINE_KEYS)
    q = q.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in QUARANTINE_SCHEMA.fields])
    inserted, _ = merge_into(spark, cfg.t(QUARANTINE_TABLE), q, QUARANTINE_KEYS)
    return inserted
