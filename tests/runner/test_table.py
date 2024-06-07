from rialto.runner.table import Table


def test_table_basic_init():
    t = Table(catalog="cat", schema="sch", table="tab", schema_path=None, table_path=None, class_name=None)

    assert t.get_table_path() == "cat.sch.tab"
    assert t.get_schema_path() == "cat.sch"


def test_table_classname_init():
    t = Table(catalog=None, schema=None, table=None, schema_path="cat.sch", table_path=None, class_name="ClaSs")

    assert t.get_table_path() == "cat.sch.cla_ss"
    assert t.get_schema_path() == "cat.sch"
    assert t.catalog == "cat"
    assert t.schema == "sch"
    assert t.table == "cla_ss"


def test_table_path_init():
    t = Table(catalog=None, schema=None, table=None, schema_path=None, table_path="cat.sch.tab", class_name=None)

    assert t.get_table_path() == "cat.sch.tab"
    assert t.get_schema_path() == "cat.sch"
    assert t.catalog == "cat"
    assert t.schema == "sch"
    assert t.table == "tab"
