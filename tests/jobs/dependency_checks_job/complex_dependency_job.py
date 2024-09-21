import tests.jobs.dependency_checks_job.datasources_a as a
import tests.jobs.dependency_checks_job.datasources_b as b
from rialto.jobs import job, register_dependency_callable, register_dependency_module

# module "A" has i(), j(), k()
# module "B" has i(j), and dependency on module C
# module "C" has j(), k()

register_dependency_module(b)
register_dependency_callable(a.j)


@job
def complex_dependency_job(i, j):
    # If we import module B, and A.j, we should not see any conflicts, because:
    # A.i won't get imported, thus won't clash with B.i
    # B has no j it only sees C.j as registered dependency

    assert i == "B.i-C.j"
    assert j == "A.j"


@job
def unimported_dependency_job(k):
    # k is in both A and C, but it's not imported here, thus won't get resolved
    pass
