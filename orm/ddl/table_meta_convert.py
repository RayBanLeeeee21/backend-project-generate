from abc import ABC, abstractmethod
from typing import Dict


class TypeHandler(ABC):
    """类型处理器基类"""

    @abstractmethod
    def convert(self, field: dict) -> str:
        pass


class IntTypeHandler(TypeHandler):
    """整数类型处理器"""

    def convert(self, field: dict) -> str:
        return 'INT'


class StringTypeHandler(TypeHandler):
    """字符串类型处理器"""

    def convert(self, field: dict) -> str:
        max_length = field.get('max_length', 255)
        return f'VARCHAR({max_length})'


class DateTypeHandler(TypeHandler):
    """日期类型处理器"""

    def convert(self, field: dict) -> str:
        return 'DATE'


class DecimalTypeHandler(TypeHandler):
    """小数类型处理器"""

    def convert(self, field: dict) -> str:
        precision = field.get('precision', 10)
        scale = field.get('scale', 2)
        return f'DECIMAL({precision},{scale})'


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
            return "TINYINT"
        elif enum_count <= 65535:
            return "SMALLINT"
        elif enum_count <= 16777215:
            return "MEDIUMINT"
        else:
            return "INT"


class DefaultTypeHandler(TypeHandler):
    """默认类型处理器"""

    def convert(self, field: dict) -> str:
        return 'TEXT'


class TypeRegistry:
    """类型处理器注册中心"""

    def __init__(self):
        self._handlers: Dict[str, TypeHandler] = {
            'int': IntTypeHandler(),
            'string': StringTypeHandler(),
            'date': DateTypeHandler(),
            'decimal': DecimalTypeHandler(),
            'enum': EnumTypeHandler()  # 添加这行
        }
        self._default_handler = DefaultTypeHandler()

    def get_handler(self, type_name: str) -> TypeHandler:
        return self._handlers.get(type_name.lower(), self._default_handler)


_type_registry = TypeRegistry()


# 全局类型注册中心实例
def _format_field(field: dict) -> str:
    """格式化单个字段定义"""
    field_name = field['name']
    field_type = _convert_type(field)
    comment = field.get('comment', '')
    comment = comment.replace("'", "\\'")  # 转义单引号

    field_def = f"{field_name} {field_type}"

    if comment:
        field_def += f" COMMENT '{comment}'"

    return field_def


def _convert_type(field: dict) -> str:
    """将JSON类型转换为SQL类型"""
    field_type = field['type']
    handler = _type_registry.get_handler(field_type)
    return handler.convert(field)


def generate_ddl_from_json(table_config: dict) -> str:
    """
    根据表结构JSON数据生成DDL语句

    Args:
        table_config: 已读取的JSON配置数据

    Returns:
        生成的DDL语句
    """
    table_name = table_config['table_name']
    ddl_lines = [f"CREATE TABLE {table_name} ("]

    # 处理主要字段
    key_field_list = table_config.get('key_fields', [])
    key_field_map = {field['name']: field for field in key_field_list}
    value_field_list = table_config.get('value_fields', [])
    value_field_map = {field['name']: field for field in value_field_list}
    status_field_list = table_config.get('status_fields', [])
    status_field_map = {field['name']: field for field in status_field_list}

    # 格式化字段定义 - 修复这里
    all_field_list = key_field_list + value_field_list + status_field_list

    # 检查是否重复
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
    formatted_fields = [_format_field(field) for field in all_field_list]  # 使用_format_field
    ddl_lines.extend([f"    {field}," for field in formatted_fields])

    # 添加唯一键约束
    if table_config.get('unique_keys'):
        unique_key_names = ', '.join(table_config['unique_keys'])
        if unique_key_names:
            # 确保唯一键字段在表中存在
            for key in unique_key_names.split(','):
                # 检查一下key是否合法，不能有空格，必须是合法的标识符
                key = key.strip()
                if not key or not key.isidentifier():
                    raise ValueError(f"唯一键字段 '{key}' 在表 '{table_name}' 中不符合规范")

                # 检查一下是否在字段列表中
                if key not in key_field_map:
                    raise ValueError(f"唯一键字段 '{key.strip()}' 在表 '{table_name}' 中未定义")
            # 添加唯一键约束
        ddl_lines.append(f"    UNIQUE KEY uk_{table_name} ({unique_key_names}),")

    # 添加索引定义
    for index in table_config.get('indexes', []):
        index_name = index['name']
        index_fields = ', '.join(index['fields'])

        # 验证索引字段是否存在
        for field_name in index['fields']:
            if field_name not in all_field_map:
                raise ValueError(f"索引字段 '{field_name}' 在表 '{table_name}' 中未定义")

        ddl_lines.append(f"    INDEX {index_name} ({index_fields}),")

    # 移除最后一个逗号
    if ddl_lines[-1].endswith(','):
        ddl_lines[-1] = ddl_lines[-1][:-1]

    ddl_lines.append(");")

    return '\n'.join(ddl_lines)
