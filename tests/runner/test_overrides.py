from rialto.runner import Runner


def test_overrides_simple(spark):
    runner = Runner(
        spark,
        config_path="tests/runner/transformations/config.yaml",
        run_date="2023-03-31",
        overrides={"runner.mail.to": ["x@b.c", "y@b.c", "z@b.c"]},
    )
    assert runner.config.runner.mail.to == ["x@b.c", "y@b.c", "z@b.c"]


def test_overrides_array_index(spark):
    runner = Runner(
        spark,
        config_path="tests/runner/transformations/config.yaml",
        run_date="2023-03-31",
        overrides={"runner.mail.to[1]": "a@b.c"},
    )
    assert runner.config.runner.mail.to == ["developer@testing.org", "a@b.c"]


def test_overrides_array_append(spark):
    runner = Runner(
        spark,
        config_path="tests/runner/transformations/config.yaml",
        run_date="2023-03-31",
        overrides={"runner.mail.to[-1]": "test"},
    )
    assert runner.config.runner.mail.to == ["developer@testing.org", "developer2@testing.org", "test"]


def test_overrides_array_lookup(spark):
    runner = Runner(
        spark,
        config_path="tests/runner/transformations/config.yaml",
        run_date="2023-03-31",
        overrides={"pipelines[name=SimpleGroup].target.target_schema": "new_schema"},
    )
    assert runner.config.pipelines[0].target.target_schema == "new_schema"
