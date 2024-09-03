from rialto.jobs import datasource


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
