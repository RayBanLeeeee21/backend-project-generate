# -*- codding: utf-8 -*-
import re
from typing import List, Dict, Any, Optional


class SQLParser:
    """SQL DDL解析器"""

    def __init__(self):
        self.type_mappings = {
            'INT': 'int',
            'BIGINT': 'bigint',
            'TINYINT': 'tinyint',
            'SMALLINT': 'smallint',
            'MEDIUMINT': 'int',
            'DATE': 'date',
            'DATETIME': 'datetime',
            'DECIMAL': 'decimal',
            'VARCHAR': 'string',
            'TEXT': 'text',
            'MEDIUMTEXT': 'mediumtext',
            'JSON': 'json',
            'CHAR': 'string',
            'FLOAT': 'float',
            'DOUBLE': 'float'
        }

    def parse_create_table(self, sql: str) -> Dict[str, Any]:
        """解析CREATE TABLE语句"""
        # 提取表名（支持反引号，关键词大小写不敏感）
        table_name_match = re.search(r'CREATE\s+TABLE\s+`?(\w+)`?', sql, re.IGNORECASE)
        if not table_name_match:
            raise ValueError("无法解析表名")
        table_name = table_name_match.group(1)

        # 提取表定义部分（关键词大小写不敏感）
        table_def_match = re.search(
            r'\((.*)\)\s*(ENGINE|TYPE)?=.*?COMMENT\s*=\s*[\'"](.+?)[\'"]',
            sql, re.DOTALL | re.IGNORECASE
        )
        if table_def_match:
            table_def = table_def_match.group(1)
            table_comment = table_def_match.group(3)
        else:
            table_def_match = re.search(r'\((.*)\)', sql, re.DOTALL | re.IGNORECASE)
            if not table_def_match:
                raise ValueError("无法解析表定义")
            table_def = table_def_match.group(1)
            table_comment = ""

        # 解析字段和约束
        fields, unique_keys, indexes, pk_info = self._parse_table_definition(table_def)

        # 主键字段归类
        id_fields = []
        pk_set = set()
        if pk_info:
            pk_set = set(pk_info['fields'])
            id_fields = [f for f in fields if f['name'] in pk_set]

        # 分类字段（排除主键字段）
        non_id_fields = [f for f in fields if f['name'] not in pk_set]
        key_fields, value_fields, status_fields = self._classify_fields(non_id_fields, indexes)

        return {
            "table_name": table_name,
            "table_comment": table_comment,
            "id_fields": id_fields,
            "key_fields": key_fields,
            "value_fields": value_fields,
            "status_fields": status_fields,
            "unique_keys": unique_keys,
            "indexes": indexes,
            "primary_key": pk_info
        }

    def _parse_table_definition(self, table_def: str) -> tuple:
        """解析表定义部分"""
        lines = [line.strip().rstrip(',') for line in table_def.split('\n') if line.strip()]

        fields = []
        unique_keys = []
        indexes = []
        pk_info = None

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # 关键词大小写不敏感
            if re.match(r'^PRIMARY\s+KEY', line, re.IGNORECASE):
                pk_info = self._parse_primary_key(line)
            elif re.match(r'^UNIQUE\s+KEY', line, re.IGNORECASE):
                uk = self._parse_unique_key(line)
                if uk:
                    unique_keys.append(uk)
            elif re.match(r'^(INDEX|KEY)', line, re.IGNORECASE):
                idx = self._parse_index(line)
                if idx:
                    indexes.append(idx)
            else:
                # 解析字段
                field = self._parse_field(line)
                if field:
                    fields.append(field)

        return fields, unique_keys, indexes, pk_info

    def _parse_primary_key(self, line: str) -> Optional[Dict[str, Any]]:
        """解析主键约束"""
        # PRIMARY KEY (`id`) USING BTREE
        pattern = r'PRIMARY\s+KEY\s*\(([^)]+)\)(?:\s+USING\s+(\w+))?'
        match = re.search(pattern, line, re.IGNORECASE)
        if not match:
            return None
        fields_str = match.group(1)
        index_type = match.group(2) or ""
        fields = [field.strip().strip('`') for field in fields_str.split(',')]
        return {
            "fields": fields,
            "type": "primary",
            "index_type": index_type
        }

    def _parse_field(self, line: str) -> Optional[Dict[str, Any]]:
        """解析字段定义"""
        # 支持字段名带反引号、类型参数、unsigned、default、not null、comment、auto_increment
        pattern = (
            r'`(?P<name>\w+)`\s+'
            r'(?P<type>[A-Z]+(?:\(\d+(?:,\d+)?\))?(?:\s+unsigned)?)'
            r'(?P<not_null>\s+NOT\s+NULL)?'
            r'(?:\s+DEFAULT\s+(?P<default>(?:\'[^\']*\'|[^\s]+)))?'
            r'(?P<auto_increment>\s+AUTO_INCREMENT)?'
            r'(?:\s+COMMENT\s+(?P<quote>[\'"])(?P<comment>.*?)(?P=quote))?'
        )
        match = re.match(pattern, line, re.IGNORECASE)
        if not match:
            return None

        field_name = match.group('name')
        field_type_str = match.group('type').upper()
        comment = match.group('comment') or ""
        default = match.group('default')
        not_null = bool(match.group('not_null'))
        auto_increment = bool(match.group('auto_increment'))

        field = {
            "name": field_name,
            "comment": comment.replace("\\'", "'").replace('\\"', '"'),
        }
        if default is not None:
            field["default"] = default.strip("'").strip('"')
        if not_null:
            field["not_null"] = True
        if auto_increment:
            field["auto_increment"] = True

        # 解析类型
        self._parse_field_type(field, field_type_str)

        return field

    def _parse_field_type(self, field: Dict[str, Any], type_str: str):
        """解析字段类型"""
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

        # int/bigint/smallint/tinyint 带长度
        if base_type in ['INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'MEDIUMINT']:
            if params:
                field['length'] = int(params)
        elif base_type in ['VARCHAR', 'CHAR']:
            if params:
                field['max_length'] = int(params)
        elif base_type == 'DECIMAL':
            if params:
                parts = params.split(',')
                field['precision'] = int(parts[0])
                if len(parts) > 1:
                    field['scale'] = int(parts[1])
        # 其它类型已映射，无需特殊处理

    def _parse_unique_key(self, line: str) -> Optional[Dict[str, Any]]:
        """解析唯一键约束，返回索引名和字段列表"""
        pattern = r'UNIQUE\s+KEY\s+`?(\w+)`?\s*\(([^)]+)\)'
        match = re.search(pattern, line, re.IGNORECASE)
        if match:
            name = match.group(1)
            fields_str = match.group(2)
            fields = [field.strip().strip('`') for field in fields_str.split(',')]
            return {"name": name, "fields": fields}
        return None

    def _parse_index(self, line: str) -> Dict[str, Any]:
        """解析索引定义"""
        pattern = r'(?:KEY|INDEX)\s+`?(\w+)`?\s*\(([^)]+)\)\s*(?:COMMENT\s+([\'"])(.*?)(\3))?'
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

    def _classify_fields(self, fields: List[Dict], indexes: List[Dict]) -> tuple:
        """将字段分类为key_fields, value_fields, status_fields
        key_fields: 在非唯一索引（普通索引）中的字段
        """
        key_fields = []
        value_fields = []
        status_fields = []

        # 收集所有普通索引字段
        index_fields = set()
        for idx in indexes:
            if idx.get('type') == 'normal':
                index_fields.update(idx.get('fields', []))

        for field in fields:
            field_name = field['name']
            field_type = field['type']

            if field_name in index_fields:
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
