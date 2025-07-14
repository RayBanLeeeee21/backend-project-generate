# -*- codding: utf-8 -*-

import utils
from orm.ddl.ddl_to_sql import ddl_to_sql

if __name__ == '__main__':
    input = utils.read_as_json('tables/test_table.json', encoding='utf-8')
    res = ddl_to_sql(input)
    utils.write_str('tables/test_table.sql', res)