import tests.jobs.dependency_checks_job.datasources_c as c
from rialto.jobs import datasource, register_dependency_module

register_dependency_module(c)


@datasource
def i(j):
    return f"B.i-{j}"
