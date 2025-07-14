# -*- codding: utf-8 -*-
import re
from typing import List, Dict, Any, Optional


class SQLParser:
    """SQL DDL解析器"""

    def __init__(self):
        self.type_mappings = {
            'INT': 'int',
            'TINYINT': 'enum',
            'SMALLINT': 'enum',
            'MEDIUMINT': 'enum',
            'DATE': 'date',
            'DECIMAL': 'decimal',
            'VARCHAR': 'string',
            'TEXT': 'string'
        }

    def parse_create_table(self, sql: str) -> Dict[str, Any]:
        """解析CREATE TABLE语句"""
        # 提取表名
        table_name_match = re.search(r'CREATE\s+TABLE\s+(\w+)', sql, re.IGNORECASE)
        if not table_name_match:
            raise ValueError("无法解析表名")

        table_name = table_name_match.group(1)

        # 提取表定义部分
        table_def_match = re.search(r'\((.*)\)', sql, re.DOTALL)
        if not table_def_match:
            raise ValueError("无法解析表定义")

        table_def = table_def_match.group(1)

        # 解析字段和约束
        fields, unique_keys, indexes = self._parse_table_definition(table_def)

        # 分类字段
        key_fields, value_fields, status_fields = self._classify_fields(fields, unique_keys)

        return {
            "table_name": table_name,
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
        # 匹配字段定义: field_name TYPE [COMMENT 'comment']
        pattern = r'(\w+)\s+([A-Z]+(?:\(\d+(?:,\d+)?\))?)\s*(?:COMMENT\s+[\'"]([^\'"]*)[\'"])?'
        match = re.match(pattern, line, re.IGNORECASE)

        if not match:
            return None

        field_name = match.group(1)
        field_type_str = match.group(2).upper()
        comment = match.group(3) or ""

        field = {
            "name": field_name,
            "comment": comment.replace("\\'", "'")  # 反转义单引号
        }

        # 解析类型
        self._parse_field_type(field, field_type_str)

        return field

    def _parse_field_type(self, field: Dict[str, Any], type_str: str):
        """解析字段类型"""
        # 提取基础类型和参数
        type_match = re.match(r'([A-Z]+)(?:\(([^)]+)\))?', type_str)
        if not type_match:
            field['type'] = 'string'
            return

        base_type = type_match.group(1)
        params = type_match.group(2)

        if base_type == 'VARCHAR':
            field['type'] = 'string'
            if params:
                field['max_length'] = int(params)
        elif base_type == 'DECIMAL':
            field['type'] = 'decimal'
            if params:
                parts = params.split(',')
                field['precision'] = int(parts[0])
                if len(parts) > 1:
                    field['scale'] = int(parts[1])
        elif base_type in ['TINYINT', 'SMALLINT', 'MEDIUMINT']:
            field['type'] = 'enum'
            field['enum_values'] = {"0": "active", "1": "inactive"}  # 默认状态枚举
        elif base_type == 'INT':
            field['type'] = 'int'
        elif base_type == 'DATE':
            field['type'] = 'date'
        else:
            field['type'] = 'string'

    def _parse_unique_key(self, line: str) -> List[str]:
        """解析唯一键约束"""
        # 匹配: UNIQUE KEY uk_name (field1, field2, ...)
        pattern = r'UNIQUE\s+KEY\s+\w+\s*\(([^)]+)\)'
        match = re.search(pattern, line, re.IGNORECASE)

        if match:
            fields_str = match.group(1)
            return [field.strip() for field in fields_str.split(',')]

        return []

    def _parse_index(self, line: str) -> Dict[str, Any]:
        """解析索引定义"""
        # 匹配: INDEX index_name (field1, field2, ...) [COMMENT 'comment']
        pattern = r'INDEX\s+(\w+)\s*\(([^)]+)\)\s*(?:COMMENT\s+[\'"]([^\'"]*)[\'"])?'
        match = re.search(pattern, line, re.IGNORECASE)

        if not match:
            return {}

        index_name = match.group(1)
        fields_str = match.group(2)
        comment = match.group(3) or ""
        fields = [field.strip() for field in fields_str.split(',')]

        return {
            "name": index_name,
            "fields": fields,
            "type": "normal",
            "comment": comment.replace("\\'", "'")  # 反转义单引号，与字段注释处理一致
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
            elif field_type == 'enum' or field_name == 'status':
                status_fields.append(field)
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