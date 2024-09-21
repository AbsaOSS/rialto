import tests.jobs.dependency_checks_job.datasources_a as a
import tests.jobs.dependency_checks_job.datasources_b as b
from rialto.jobs import job, register_dependency_module

# module "A" has i(), j(), k()
# module "B" has i(j), and dependency on module C

register_dependency_module(b)
register_dependency_module(a)


@job
def duplicate_dependency_job(i):
    # i is in both A and B
    pass
