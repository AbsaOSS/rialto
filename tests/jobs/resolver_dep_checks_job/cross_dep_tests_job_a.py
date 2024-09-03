from rialto.jobs import job
from tests.jobs.resolver_dep_checks_job.datasources import *
from tests.jobs.resolver_dep_checks_job.dep_package.pkg_datasources import *


@job
def ok_dep_job(datasource_pkg, datasource_base):
    pass
