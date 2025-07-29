import tests.jobs.dependency_checks_job.main_datasources as ds
from rialto.jobs import job, register_dependency_module

register_dependency_module(ds)


@job
def ok_dependency_job(c):
    return c + 1


@job
def circular_dependency_job(circle_third):
    return circle_third + 1


@job
def missing_dependency_job(a, x):
    return x + a


@job
def self_dependency_job(self_dependency):
    return self_dependency + 1


@job
def default_dependency_job(run_date, spark, config, table_reader):
    assert run_date is not None
    assert spark is not None
    assert config is not None
    assert table_reader is not None


@job
def dependency_with_config_job(dependency_with_config):
    assert dependency_with_config == 54  # 1 + 11 + 42
