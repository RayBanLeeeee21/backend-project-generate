# -*- codding: utf-8 -*-
import re
from typing import List, Dict, Any, Optional


class SQLParser:
    """SQL DDL解析器"""

    def __init__(self):
        self.type_mappings = {
            'INT': 'int',
            'BIGINT': 'bigint',
            'TINYINT': 'enum',
            'SMALLINT': 'enum',
            'MEDIUMINT': 'enum',
            'DATE': 'date',
            'DATETIME': 'datetime',
            'DECIMAL': 'decimal',
            'VARCHAR': 'string',
            'TEXT': 'string',
            'CHAR': 'string',
            'FLOAT': 'float',
            'DOUBLE': 'float'
        }

    def parse_create_table(self, sql: str) -> Dict[str, Any]:
        """解析CREATE TABLE语句"""
        # 提取表名（支持反引号）
        table_name_match = re.search(r'CREATE\s+TABLE\s+`?(\w+)`?', sql, re.IGNORECASE)
        if not table_name_match:
            raise ValueError("无法解析表名")
        table_name = table_name_match.group(1)

        # 提取表定义部分
        table_def_match = re.search(r'\((.*)\)\s*(ENGINE|TYPE)?=.*?COMMENT\s*=\s*[\'"](.+?)[\'"]', sql, re.DOTALL | re.IGNORECASE)
        if table_def_match:
            table_def = table_def_match.group(1)
            table_comment = table_def_match.group(3)
        else:
            # 没有表注释时的兼容
            table_def_match = re.search(r'\((.*)\)', sql, re.DOTALL)
            if not table_def_match:
                raise ValueError("无法解析表定义")
            table_def = table_def_match.group(1)
            table_comment = ""

        # 解析字段和约束
        fields, unique_keys, indexes = self._parse_table_definition(table_def)

        # 分类字段
        key_fields, value_fields, status_fields = self._classify_fields(fields, unique_keys)

        return {
            "table_name": table_name,
            "table_comment": table_comment,
            "key_fields": key_fields,
            "value_fields": value_fields,
            "status_fields": status_fields,
            "unique_keys": unique_keys,
            "indexes": indexes
        }

    def _parse_table_definition(self, table_def: str) -> tuple:
        """解析表定义部分"""
        lines = [line.strip().rstrip(',') for line in table_def.split('\n') if line.strip()]

        fields = []
        unique_keys = []
        indexes = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            if line.upper().startswith('UNIQUE KEY'):
                unique_keys.extend(self._parse_unique_key(line))
            elif line.upper().startswith('INDEX'):
                indexes.append(self._parse_index(line))
            elif not any(line.upper().startswith(keyword) for keyword in ['PRIMARY KEY', 'FOREIGN KEY', 'CHECK']):
                field = self._parse_field(line)
                if field:
                    fields.append(field)

        return fields, unique_keys, indexes

    def _parse_field(self, line: str) -> Optional[Dict[str, Any]]:
        """解析字段定义"""
        # 支持字段名带反引号、类型参数、unsigned、default、not null、comment
        pattern = (
            r'`(?P<name>\w+)`\s+'
            r'(?P<type>[A-Z]+(?:\(\d+(?:,\d+)?\))?(?:\s+unsigned)?)'
            r'(?:\s+NOT\s+NULL)?'
            r'(?:\s+DEFAULT\s+(?P<default>(?:\'[^\']*\'|[^\s]+)))?'
            r'(?:\s+AUTO_INCREMENT)?'
            r'(?:\s+COMMENT\s+(?P<quote>[\'"])(?P<comment>.*?)(?P=quote))?'
        )
        match = re.match(pattern, line, re.IGNORECASE)
        if not match:
            return None

        field_name = match.group('name')
        field_type_str = match.group('type').upper()
        comment = match.group('comment') or ""
        default = match.group('default')

        field = {
            "name": field_name,
            "comment": comment.replace("\\'", "'").replace('\\"', '"'),
        }
        if default is not None:
            field["default"] = default.strip("'")

        # 解析类型
        self._parse_field_type(field, field_type_str)

        return field

    def _parse_field_type(self, field: Dict[str, Any], type_str: str):
        """解析字段类型"""
        # 支持 unsigned
        unsigned = 'UNSIGNED' in type_str
        type_str = type_str.replace('UNSIGNED', '').strip()
        type_match = re.match(r'([A-Z]+)(?:\(([^)]+)\))?', type_str)
        if not type_match:
            field['type'] = 'string'
            return

        base_type = type_match.group(1)
        params = type_match.group(2)

        mapped_type = self.type_mappings.get(base_type, 'string')
        field['type'] = mapped_type
        if unsigned:
            field['unsigned'] = True

        if base_type in ['VARCHAR', 'CHAR']:
            if params:
                field['max_length'] = int(params)
        elif base_type == 'DECIMAL':
            if params:
                parts = params.split(',')
                field['precision'] = int(parts[0])
                if len(parts) > 1:
                    field['scale'] = int(parts[1])
        elif base_type in ['TINYINT', 'SMALLINT', 'MEDIUMINT']:
            if field.get('name', '').lower() == 'status':
                field['type'] = 'enum'
                field['enum_values'] = {"0": "active", "1": "inactive"}
            else:
                field['type'] = 'int'
        # 其它类型已映射，无需特殊处理

    def _parse_unique_key(self, line: str) -> List[str]:
        """解析唯一键约束"""
        pattern = r'UNIQUE\s+KEY\s+`?\w+`?\s*\(([^)]+)\)'
        match = re.search(pattern, line, re.IGNORECASE)
        if match:
            fields_str = match.group(1)
            return [field.strip().strip('`') for field in fields_str.split(',')]
        return []

    def _parse_index(self, line: str) -> Dict[str, Any]:
        """解析索引定义"""
        pattern = r'(?:KEY|INDEX)\s+`?(\w+)`?\s*\(([^)]+)\)\s*(?:COMMENT\s+([\'"])((?:(?!\3)[^\\]|\\.)*)(\3))?'
        match = re.search(pattern, line, re.IGNORECASE)
        if not match:
            return {}
        index_name = match.group(1)
        fields_str = match.group(2)
        comment = match.group(4) or ""
        fields = [field.strip().strip('`') for field in fields_str.split(',')]
        return {
            "name": index_name,
            "fields": fields,
            "type": "normal",
            "comment": comment.replace("\\'", "'").replace('\\"', '"')
        }

    def _classify_fields(self, fields: List[Dict], unique_keys: List[str]) -> tuple:
        """将字段分类为key_fields, value_fields, status_fields"""
        key_fields = []
        value_fields = []
        status_fields = []

        unique_key_set = set(unique_keys)

        for field in fields:
            field_name = field['name']
            field_type = field['type']

            if field_name in unique_key_set:
                key_fields.append(field)
            else:
                value_fields.append(field)

        return key_fields, value_fields, status_fields


def sql_to_ddl(sql: str) -> dict:
    """
    将SQL DDL语句转换为JSON格式的表结构配置

    Args:
        sql: SQL DDL语句字符串

    Returns:
        转换后的JSON格式表结构配置
    """
    parser = SQLParser()
    return parser.parse_create_table(sql)
