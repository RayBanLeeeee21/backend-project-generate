# -*- codding: utf-8 -*-
import utils
from orm.ddl.sql_to_ddl import sql_to_ddl

if __name__ == '__main__':

    sql_str = utils.read_as_str('tables/test_table.sql')
    ddl_data = sql_to_ddl(sql_str)

    utils.write_json('tables/test_table-2.json', ddl_data, indent=2)