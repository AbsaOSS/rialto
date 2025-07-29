import tests.jobs.dependency_checks_job.datasources_config as cfg
from rialto.jobs import datasource, register_dependency_module

register_dependency_module(cfg)


@datasource
def a():
    return 1


@datasource
def b(a):
    return a + 10


@datasource
def c(a, b):
    # 1 + 11 = 12
    return a + b


@datasource
def circle_first(circle_second):
    return circle_second + 1


@datasource
def circle_second(circle_third):
    return circle_third + 1


@datasource
def circle_third(circle_first):
    return circle_first + 1


@datasource
def self_dependency(a, b, c, self_dependency):
    return a


@datasource
def dependency_with_config(a, b, my_config):
    return a + b + my_config
