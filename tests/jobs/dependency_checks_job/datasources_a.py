from rialto.jobs import datasource


@datasource
def i():
    return "A.i"


@datasource
def j():
    return "A.j"


@datasource
def k():
    return "A.k"
