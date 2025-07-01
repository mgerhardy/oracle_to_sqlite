#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This script converts Oracle DDL statements to SQLite-compatible DDL.
It handles CREATE TABLE statements, including nested parentheses, and converts Oracle data types to SQLite equivalents.

SELECT DBMS_METADATA.GET_DDL('TABLE', 'FOO', 'SCHEMA') FROM DUAL

Usage:
    echo "CREATE TABLE ..." | ./ddlconvert.py

Or import:
    from ddlconvert import convert_oracle_to_sqlite
"""

import re
import sys

def extract_create_table_block(sql):
    match = re.search(r'CREATE TABLE\s+"?([\w\d_]+)"?\."?([\w\d_]+)"?\s*\(', sql, re.IGNORECASE)
    if not match:
        return None, None
    schema, table_name = match.groups()
    start = match.end()
    depth = 1
    end = start
    while end < len(sql) and depth > 0:
        if sql[end] == '(':
            depth += 1
        elif sql[end] == ')':
            depth -= 1
        end += 1
    block = sql[match.start():end]
    return table_name, block

def convert_oracle_to_sqlite(oracle_sql):
    oracle_sql = re.sub(r'--.*?$', '', oracle_sql, flags=re.MULTILINE)
    oracle_sql = re.sub(r'/\*.*?\*/', '', oracle_sql, flags=re.DOTALL)
    oracle_sql = re.sub(r'\bNOVALIDATE\b', '', oracle_sql, flags=re.IGNORECASE)
    oracle_sql = re.sub(r'\s+', ' ', oracle_sql).strip()

    # Extract external PK constraint
    pk_match = re.search(
        r'ALTER TABLE\s+"?(\w+)"?\."?(\w+)"?\s+ADD CONSTRAINT\s+"?\w+"?\s+PRIMARY KEY\s+\(([^)]+)\)',
        oracle_sql, re.IGNORECASE)
    primary_key_field = pk_match.group(3).strip() if pk_match else None

    # Extract CREATE TABLE block
    table_name, create_block = extract_create_table_block(oracle_sql)
    if not create_block:
        return "-- ERROR: CREATE TABLE block not found"

    # Remove quoted identifiers and schema names
    create_block = re.sub(r'"(\w+)"\."(\w+)"', r'\2', create_block)
    create_block = create_block.replace('"', '')

    # Replace Oracle types
    def map_oracle_type(t):
        t = t.upper()

        # remove optional CHAR | BYTE e.g. VARCHAR2(20 CHAR)
        t = re.sub(r'\(\s*(\d+)\s+(CHAR|BYTE)\s*\)', r'(\1)', t)

        if re.match(r'NUMBER\s*\(\d+,\s*0\)', t):
            return 'INTEGER'
        elif re.match(r'NUMBER\s*\(\d+,\s*\d+\)', t):
            return 'REAL'
        elif re.match(r'NUMBER\s*\(\d+\)', t):
            return 'INTEGER'
        elif 'NUMBER' in t:
            return 'INTEGER'
        elif 'VARCHAR2' in t or 'CHAR' in t or 'CLOB' in t:
            return 'TEXT'
        elif 'DATE' in t or 'TIMESTAMP' in t:
            return 'TEXT'
        elif 'BLOB' in t:
            return 'BLOB'
        return t

    # Extract and clean inside ()
    inner = create_block[create_block.find('(')+1:create_block.rfind(')')]
    items = [i.strip() for i in re.split(r',(?![^(]*\))', inner)]  # avoid splitting inside ()
    cleaned_items = []

    for item in items:
        item = re.sub(r'CONSTRAINT\s+\w+\s+', '', item, flags=re.IGNORECASE)
        item = re.sub(r'\bENABLE\b', '', item, flags=re.IGNORECASE)
        item = re.sub(r'\bDISABLE\b', '', item, flags=re.IGNORECASE)
        item = re.sub(r'\bNOVALIDATE\b', '', item, flags=re.IGNORECASE)
        item = re.sub(r'TABLESPACE\s+\w+', '', item, flags=re.IGNORECASE)
        item = re.sub(r'SEGMENT CREATION\s+\w+', '', item, flags=re.IGNORECASE)
        item = re.sub(r'STORAGE\s*\(.*?\)', '', item, flags=re.IGNORECASE)
        item = re.sub(r'SUPPLEMENTAL LOG DATA\s*\(.*?\)\s*COLUMNS', '', item, flags=re.IGNORECASE)
        item = re.sub(r'USING INDEX\s+.*', '', item, flags=re.IGNORECASE)

        # Fix CHECK (... IN (... NULL ...))
        item = re.sub(r'IN\s*\(([^)]+?)NULL([^)]+?)\)', r'IN (\1\2) OR NEGATION IS NULL', item, flags=re.IGNORECASE)

        # unify CHAR/VARCHAR2(... CHAR|BYTE)
        item = re.sub(r'(CHAR|VARCHAR2)\s*\(\s*(\d+)\s+(CHAR|BYTE)\s*\)', r'\1(\2)', item, flags=re.IGNORECASE)

        # unify NUMBER (10 , 0) → NUMBER(10,0)
        item = re.sub(r'NUMBER\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)', r'NUMBER(\1,\2)', item, flags=re.IGNORECASE)
        item = re.sub(r'NUMBER\s*\(\s*(\d+)\s*\)', r'NUMBER(\1)', item, flags=re.IGNORECASE)

        # Fix type
        parts = item.split()
        if len(parts) >= 2:
            col_name = parts[0]
            col_type = map_oracle_type(parts[1])
            rest = ' '.join(parts[2:])

            # check for sequence usage
            default_seq = re.search(r'DEFAULT\s+\w+\.?\w*\.?NEXTVAL', rest, re.IGNORECASE)
            if default_seq and primary_key_field and col_name.upper() == primary_key_field.upper():
                item = f"{col_name} INTEGER PRIMARY KEY AUTOINCREMENT"
                primary_key_field = None  # PK already handled - skip it later
            else:
                item = f"{col_name} {col_type} {rest}".strip()

        item = item.strip().rstrip(',')
        if item:
            cleaned_items.append(item)

    # Add PRIMARY KEY if found
    if primary_key_field:
        cleaned_items.append(f'PRIMARY KEY ({primary_key_field})')

    # Final assembly
    result = f'CREATE TABLE IF NOT EXISTS {table_name} (\n  ' + ',\n  '.join(cleaned_items) + '\n);'
    return result

def main():
    sql_input = sys.stdin.read().strip()
    print(convert_oracle_to_sqlite(sql_input))

if __name__ == "__main__":
    main()
