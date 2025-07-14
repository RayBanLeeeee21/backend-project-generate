# -*- codding: utf-8 -*-

import utils
from orm.ddl.table_meta_convert import generate_ddl_from_json

if __name__ == '__main__':
    input = utils.read_as_json('tables/test_table.json', encoding='utf-8')
    res = generate_ddl_from_json(input)
    utils.write_str('tables/test_table.sql', res)