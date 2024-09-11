import tests.jobs.resolver_dep_checks_job.dep_package.pkg_datasources as pkg_ds
from rialto.jobs import datasource, register_module

register_module(pkg_ds)


@datasource
def a():
    return 1


@datasource
def b(a):
    return a + 1


@datasource
def c(a, b):
    return a + b


@datasource
def d(a, circle_1):
    return circle_1 + a


@datasource
def circle_1(circle_2):
    return circle_2 + 1


@datasource
def circle_2(circle_1):
    return circle_1 + 1


@datasource
def datasource_base():
    return "dataset_base_return"
