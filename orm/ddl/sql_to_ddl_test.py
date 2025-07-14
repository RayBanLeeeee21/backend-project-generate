# -*- codding: utf-8 -*-
import utils
from orm.ddl.ddl_to_sql import ddl_to_sql
from orm.ddl.sql_to_ddl import sql_to_ddl

if __name__ == '__main__':

    ddl_input = utils.read_as_json('tables/test_table.json', encoding='utf-8')

    sql_str = ddl_to_sql(ddl_input)
    ddl_data = sql_to_ddl(sql_str)

    utils.write_json('tables/test_table-2.json', ddl_data, indent=2)

    assert ddl_data == ddl_input, "The converted DDL does not match the original DDL."