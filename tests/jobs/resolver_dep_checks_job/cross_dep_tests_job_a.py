import tests.jobs.resolver_dep_checks_job.datasources as ds
from rialto.jobs import job, register_module

register_module(ds)


@job
def ok_dep_job(datasource_pkg, datasource_base):
    pass
