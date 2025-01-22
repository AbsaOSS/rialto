from datetime import timedelta

from rialto.runner.date_manager import DateManager
from rialto.runner.record import Record

record = Record(
    "job",
    "target",
    DateManager.str_to_date("2024-01-01"),
    timedelta(days=0, hours=1, minutes=2, seconds=3),
    1,
    "status",
    "reason",
)


def test_record_to_spark(spark):
    row = record.to_spark_row()
    assert row.job == "job"
    assert row.target == "target"
    assert row.date == DateManager.str_to_date("2024-01-01")
    assert row.time == "1:02:03"
    assert row.records == 1
    assert row.status == "status"
    assert row.reason == "reason"
    assert row.exception is None
