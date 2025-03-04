from datetime import datetime, timedelta

from reporting.record import Record

from rialto.runner.date_manager import DateManager

record = Record(
    "job",
    "target",
    DateManager.str_to_date("2024-01-01"),
    timedelta(days=0, hours=1, minutes=2, seconds=3),
    1,
    "status",
    "reason",
    None,
    datetime(2024, 1, 1, 1, 2, 3),
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
    assert row.run_timestamp == datetime(2024, 1, 1, 1, 2, 3)
