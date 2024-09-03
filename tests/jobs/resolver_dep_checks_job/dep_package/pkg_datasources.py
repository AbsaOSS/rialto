from rialto.jobs import datasource


@datasource
def datasource_pkg():
    return "datasource_pkg_return"
