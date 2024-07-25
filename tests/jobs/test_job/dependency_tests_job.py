from rialto.jobs.decorators import job, datasource


@datasource
def a():
    return 1


@datasource
def b(a):
    return a + 1


@datasource
def c(a, b):
    return a + b


@job
def ok_dependency_job(c):
    return c + 1


@datasource
def d(a, circle_1):
    return circle_1 + a


@datasource
def circle_1(circle_2):
    return circle_2 + 1


@datasource
def circle_2(circle_1):
    return circle_1 + 1


@job
def circular_dependency_job(d):
    return d + 1


@job
def missing_dependency_job(a, x):
    return x + a


@job
def default_dependency_job(run_date, spark, config, dependencies, table_reader, feature_loader):
    return 1
