from rialto.jobs import job
from tests.jobs.resolver_dep_checks_job.datasources import *


@job
def ok_dependency_job(c):
    return c + 1


@job
def circular_dependency_job(d):
    return d + 1


@job
def missing_dependency_job(a, x):
    return x + a


@job
def default_dependency_job(run_date, spark, config, table_reader, feature_loader):
    return 1
