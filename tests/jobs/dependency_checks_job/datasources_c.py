from rialto.jobs import datasource


@datasource
def j():
    return "C.j"


@datasource
def k():
    return "C.k"
