from abc import ABC, abstractmethod
from typing import Dict


class TypeHandler(ABC):
    """类型处理器基类"""

    @abstractmethod
    def convert(self, field: dict) -> str:
        pass


class IntTypeHandler(TypeHandler):
    """int类型处理器"""

    def convert(self, field: dict) -> str:
        length = field.get('length')
        if length is None:
            raise ValueError(f'解析字段 {field["name"]} 时未指定 length')
        return f'int({length})'


class BigIntTypeHandler(TypeHandler):
    """整数类型处理器"""

    def convert(self, field: dict) -> str:
        length = field.get('length')
        if length is None:
            raise ValueError(f'解析字段 {field["name"]} 时未指定 length')
        return f'bigint({length})'


class SmallIntTypeHandler(TypeHandler):
    """整数类型处理器"""

    def convert(self, field: dict) -> str:
        length = field.get('length')
        if length is None:
            raise ValueError(f'解析字段 {field["name"]} 时未指定 length')
        return f'smallint({length})'


class TinyIntTypeHandler(TypeHandler):
    """整数类型处理器"""

    def convert(self, field: dict) -> str:
        length = field.get('length')
        if length is None:
            raise ValueError(f'解析字段 {field["name"]} 时未指定 length')
        return f'tinyint({length})'


class StringTypeHandler(TypeHandler):
    """字符串类型处理器"""

    def convert(self, field: dict) -> str:
        max_length = field.get('max_length')
        if max_length is None:
            raise ValueError(f'解析字段 {field["name"]} 时未指定 max_length')
        return f'varchar({max_length})'


class DateTypeHandler(TypeHandler):
    """日期类型处理器"""

    def convert(self, field: dict) -> str:
        # 修复：区分 date 和 datetime
        if field.get('type') == 'datetime':
            return 'datetime'
        return 'date'


class DecimalTypeHandler(TypeHandler):
    """小数类型处理器"""

    def convert(self, field: dict) -> str:
        precision = field.get('precision', 10)
        scale = field.get('scale', 2)
        return f'decimal({precision},{scale})'


class EnumTypeHandler(TypeHandler):
    """枚举类型处理器"""

    def convert(self, field: dict) -> str:
        enum_values = field.get('enum_values', {})
        if not enum_values:
            raise ValueError("枚举类型字段必须指定枚举值")

        # 处理字典格式的enum_values
        if isinstance(enum_values, dict):
            enum_count = len(enum_values)
        else:
            enum_count = len(enum_values)

        if enum_count <= 255:
            return "tinyint"
        elif enum_count <= 65535:
            return "smallint"
        elif enum_count <= 16777215:
            return "mediumint"
        else:
            return "int"


class TextTypeHandler(TypeHandler):
    """TEXT类型处理器"""

    def convert(self, field: dict) -> str:
        return 'text'

class MediumTextTypeHandler(TypeHandler):
    """mediumtext类型处理器"""

    def convert(self, field: dict) -> str:
        return 'mediumtext'

class JsonTypeHandler(TypeHandler):
    """默认类型处理器"""

    def convert(self, field: dict) -> str:
        return 'json'


class TypeRegistry:
    """类型处理器注册中心"""

    def __init__(self):
        self._handlers: Dict[str, TypeHandler] = {
            'int': IntTypeHandler(),
            'bigint': BigIntTypeHandler(),
            'smallint': SmallIntTypeHandler(),
            'tinyint': TinyIntTypeHandler(),
            'string': StringTypeHandler(),
            'date': DateTypeHandler(),
            'datetime': DateTypeHandler(),
            'decimal': DecimalTypeHandler(),
            'json': JsonTypeHandler(),
            'text': TextTypeHandler(),
            'mediumtext': MediumTextTypeHandler(),
            'enum': EnumTypeHandler(),
        }
        self._default_handler = TextTypeHandler()

    def get_handler(self, type_name: str) -> TypeHandler:
        return self._handlers.get(type_name.lower(), self._default_handler)


_type_registry = TypeRegistry()


# 全局类型注册中心实例
def _format_field(field: dict, quote_field_names: bool = True) -> str:
    """格式化单个字段定义"""
    field_name = field['name']
    if quote_field_names:
        field_name = f"`{field_name}`"
    field_type = _convert_type(field)
    # 输出 unsigned
    if field.get('unsigned'):
        field_type += ' unsigned'
    comment = field.get('comment', '')
    comment = comment.replace("'", "\\'")  # 转义单引号

    field_def = f"{field_name} {field_type}"

    # 生成 NOT NULL
    if field.get('not_null', False) or field.get('required', False):
        field_def += " NOT NULL"

    # 生成 AUTO_INCREMENT
    if field.get('auto_increment', False):
        field_def += " AUTO_INCREMENT"

    # 生成 DEFAULT
    if 'default' in field and field['default'] is not None:
        default_val = field['default']
        if isinstance(default_val, str) and not default_val.isdigit():
            default_val = f"'{default_val}'"
        field_def += f" DEFAULT {default_val}"

    # 生成 COMMENT
    if comment:
        field_def += f" COMMENT '{comment}'"

    return field_def


def _convert_type(field: dict) -> str:
    """将JSON类型转换为SQL类型"""
    field_type = field['type']
    handler = _type_registry.get_handler(field_type)
    return handler.convert(field)


def ddl_to_sql(table_config: dict, quote_field_names: bool = True) -> str:
    """
    根据表结构JSON数据生成DDL语句

    Args:
        table_config: 已读取的JSON配置数据
        quote_field_names: 是否对字段名加反引号

    Returns:
        生成的DDL语句
    """
    table_name = table_config['table_name']
    table_comment = table_config.get('table_comment', '')
    ddl_lines = [f"create table {table_name} ("]
    # 主键字段
    id_field_list = table_config.get('id_fields', [])
    key_field_list = table_config.get('key_fields', [])
    value_field_list = table_config.get('value_fields', [])
    status_field_list = table_config.get('status_fields', [])

    # 检查是否重复
    all_field_list = id_field_list + key_field_list + value_field_list + status_field_list
    all_field_map = {}
    for field in all_field_list:
        field_name = field.get('name')

        # 检查字段名是否符合规范
        if not field_name or not isinstance(field_name, str) or not field_name.isidentifier():
            raise ValueError(f"字段名 '{field_name}' 在表 '{table_name}' 中不符合规范")

        if field_name in all_field_map:
            raise ValueError(f"字段 '{field_name}' 在表 '{table_name}' 中重复定义")
        all_field_map[field_name] = field

    # 添加字段到DDL
    formatted_fields = [_format_field(field, quote_field_names) for field in all_field_list]
    ddl_lines.extend([f"    {field}," for field in formatted_fields])

    # 添加主键约束
    pk_info = table_config.get('primary_key')
    if pk_info and pk_info.get('fields'):
        pk_fields = ', '.join([f"`{f}`" if quote_field_names else f"{f}" for f in pk_info['fields']])
        pk_line = f"    PRIMARY KEY ({pk_fields})"
        if pk_info.get('index_type'):
            pk_line += f" USING {pk_info['index_type'].upper()}"
        pk_line += ","
        ddl_lines.append(pk_line)

    # 添加唯一键约束
    if table_config.get('unique_keys'):
        for uk in table_config['unique_keys']:
            uk_name = uk['name']
            uk_fields = ', '.join([f"`{f}`" if quote_field_names else f"{f}" for f in uk['fields']])
            ddl_lines.append(f"    UNIQUE KEY {uk_name} ({uk_fields}),")

    # 添加索引定义
    for index in table_config.get('indexes', []):
        index_name = index['name']
        if quote_field_names:
            index_name = f"`{index_name}`"
        index_fields = ', '.join([f"`{f}`" if quote_field_names else f"{f}" for f in index['fields']])
        index_comment = index.get('comment', '')
        index_def = f"    KEY {index_name} ({index_fields})"
        if index_comment:
            index_comment = index_comment.replace("'", "\\'")  # 转义单引号
            index_def += f" COMMENT '{index_comment}'"
        index_def += ","
        ddl_lines.append(index_def)

    # 移除最后一个逗号
    if ddl_lines[-1].endswith(','):
        ddl_lines[-1] = ddl_lines[-1][:-1]

    ddl_lines.append(");")

    # 添加表注释
    if table_comment:
        ddl_lines[-1] = ddl_lines[-1][:-1] + f" ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='{table_comment}';" if ddl_lines[-1].endswith(';') else \
        ddl_lines[-1] + f" ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='{table_comment}';"

    return '\n'.join(ddl_lines)
