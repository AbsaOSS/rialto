from rialto.jobs.test_utils import disable_job_decorators, resolver_resolves
from tests.jobs.test_job import test_job


def test_resolve_after_disable(spark):
    with disable_job_decorators(test_job):
        assert test_job.job_with_datasource("test") == "test"
    assert resolver_resolves(spark, test_job.job_with_datasource)
