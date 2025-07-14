# -*- codding: utf-8 -*-
import utils
from orm.ddl.ddl_to_sql import ddl_to_sql
from orm.ddl.sql_to_ddl import sql_to_ddl

if __name__ == '__main__':

    sql_str = utils.read_as_str('tables/test_table.sql')
    ddl_data_1 = sql_to_ddl(sql_str)
    utils.write_json('tables/test_table.json', ddl_data_1)

    sql_str_2 = ddl_to_sql(ddl_data_1)
    ddl_data_2 = sql_to_ddl(sql_str_2)

    assert ddl_data_1 == ddl_data_2, "The converted DDL does not match the original DDL."