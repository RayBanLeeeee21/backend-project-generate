# -*- codding: utf-8 -*-
import utils
from orm.ddl.ddl_to_sql import ddl_to_sql
from orm.ddl.sql_to_ddl import sql_to_ddl

def split_ddls_to_list(sql_str):
    """
    将包含多个DDL的SQL字符串拆分为单个DDL语句的列表
    """
    import re
    # 按CREATE TABLE分割，保留CREATE TABLE关键字
    ddl_list = re.split(r'(?=CREATE TABLE)', sql_str, flags=re.IGNORECASE)
    # 去除空白和无效项
    ddl_list = [ddl.strip() for ddl in ddl_list if ddl.strip()]
    return ddl_list

if __name__ == '__main__':

    ddls_sql_str = utils.read_as_str('tables/ddls.sql')
    ddl_list = split_ddls_to_list(ddls_sql_str)

    for sql in  ddl_list:

        ddl_data_1 = sql_to_ddl(sql)
        table = ddl_data_1.get('table_name', '')

        if table == 'c_song_promotion_cost':
            continue

        utils.write_json('tables/%s-1.json' % table, ddl_data_1, indent=2)

        if not table:
            raise ValueError("DDL data does not contain a valid table name.")

        sql_str_2 = ddl_to_sql(ddl_data_1)
        utils.write_str('tables/%s-1.sql' % table, sql)
        utils.write_str('tables/%s-2.sql' % table, sql_str_2)

        ddl_data_2 = sql_to_ddl(sql_str_2)
        utils.write_json('tables/%s-2.json' % table, ddl_data_2, indent=2)

        assert ddl_data_1 == ddl_data_2, "The converted DDL does not match the original DDL."


