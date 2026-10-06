"""Audit / execution logging. One row per file (raw->bronze) or per table run (bronze->silver)."""
from datetime import datetime, timezone

from .config import LOG_TABLE
from .schemas import LOG_SCHEMA


def _now():
    return datetime.now(timezone.utc)


class Audit:
    """Context manager: times a unit of work, records the outcome in pipeline_execution_logs.

    A failure inside the `with` block is logged as 'Failure' and swallowed, so one bad file does
    not stop the rest of the batch (fault tolerance). Check `.status` afterwards if needed.
    """

    def __init__(self, spark, cfg, run_id, batch_id, layer, source, load_type, parameter):
        self.spark, self.cfg = spark, cfg
        self.run_id, self.batch_id = run_id, batch_id
        self.layer, self.source, self.load_type, self.parameter = layer, source, load_type, parameter
        self.rows_inserted = self.rows_updated = self.rows_quarantined = 0
        self.message, self.status = "", None

    def __enter__(self):
        self.start_time = _now()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.end_time = _now()
        self.status = "Failure" if exc_type else "Success"
        msg = self.message
        if exc_type:
            msg = f"{msg} | ERROR {exc_type.__name__}: {exc}"
        row = (self.run_id, self.batch_id, self.layer, self.source, self.load_type, self.parameter,
               self.start_time, self.end_time, self.status, int(self.rows_inserted),
               int(self.rows_updated), int(self.rows_quarantined), msg[:2000])
        try:
            (self.spark.createDataFrame([row], LOG_SCHEMA).write.format("delta")
             .mode("append").saveAsTable(self.cfg.t(LOG_TABLE)))
        except Exception as e:  # logging must never hide the original problem
            print("WARNING: could not write audit log:", e)
        print(f"[{self.status}] {self.layer} | {self.source} | {self.parameter} | "
              f"inserted={self.rows_inserted} updated={self.rows_updated} "
              f"quarantined={self.rows_quarantined} {self.message}")
        return exc_type is None or issubclass(exc_type, Exception)

    def summary(self):
        return {"layer": self.layer, "source": self.source, "parameter": self.parameter,
                "status": self.status, "inserted": self.rows_inserted,
                "updated": self.rows_updated, "quarantined": self.rows_quarantined}
