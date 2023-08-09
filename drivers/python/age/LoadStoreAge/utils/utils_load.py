# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import csv
from typing import Tuple

def make_graphid(label_id, entry_id) -> int:
    ENTRY_ID_BITS = 32 + 16
    ENTRY_ID_MASK = 0x0000ffffffffffff
    tmp = (label_id << ENTRY_ID_BITS) | (entry_id & ENTRY_ID_MASK)
    return tmp

def get_label_id(conn, GRAPH_NAME, label_name) -> int: 
    stmt = '''SELECT id FROM ag_catalog.ag_label 
                WHERE relation = '%s."%s"'::regclass;
                    ''' % (GRAPH_NAME, label_name)

    with conn.cursor() as cursor:
        try:
            cursor.execute(stmt)
            rows = cursor.fetchall()
        except Exception as ex:
            print(type(ex), ex)
            conn.rollback()
    label_id = rows[0][0]
    return label_id

def get_contents_from_csv(file_path) -> Tuple[list, list]:
    file = open(file_path)
    csvreader = csv.reader(file)
    header = []
    header = next(csvreader)
    records = []
    for record in csvreader:
        records.append(record)
    return header, records

def insert_into_vertices_table(conn, GRAPH_NAME, 
                               label_name, records_lst) -> None:
    cursor = conn.cursor()
    args = ','.join(cursor.mogrify("(%s,%s)", i).decode('utf-8')
            for i in records_lst)
    stmt = '''INSERT INTO %s."%s" VALUES 
                ''' % (GRAPH_NAME, label_name) + (args) 
    with cursor:
        try:
            cursor.execute(stmt)
        except Exception as ex:
            print(type(ex), ex)

def insert_into_edges_table(conn, GRAPH_NAME, 
                            label_name, records_lst) -> None:
    cursor = conn.cursor()
    args = ','.join(cursor.mogrify("(%s,%s,%s,%s)", i).decode('utf-8')
            for i in records_lst)
    stmt = 'INSERT INTO %s."%s" VALUES ' % (GRAPH_NAME, label_name) + (args) 
    with cursor:
        try:
            cursor.execute(stmt)
        except Exception as ex:
            print(type(ex), ex)

def drop_pk_constraint_vertex_table(conn, GRAPH_NAME) -> None:
    stmt = '''ALTER TABLE %s."%s" 
                DROP CONSTRAINT _ag_label_vertex_pkey;
                    ''' % (GRAPH_NAME, '_ag_label_vertex')
    with conn.cursor() as cursor:
        try:
            cursor.execute(stmt)
        except Exception as ex:
            print(type(ex), ex)

def drop_pk_constraint_edge_table(conn, GRAPH_NAME) -> None:
    stmt = '''ALTER TABLE %s."%s" 
                DROP CONSTRAINT _ag_label_edge_pkey;
                    ''' % (GRAPH_NAME, '_ag_label_edge')
    with conn.cursor() as cursor:
        try:
            cursor.execute(stmt)
        except Exception as ex:
            print(type(ex), ex)

def add_pk_constraint_vertex_table(conn, GRAPH_NAME) -> None:
    stmt = '''ALTER TABLE %s."%s" 
                ADD CONSTRAINT _ag_label_vertex_pkey PRIMARY KEY(id);
                    ''' % (GRAPH_NAME, '_ag_label_vertex')
    with conn.cursor() as cursor:
        try:
            cursor.execute(stmt)
        except Exception as ex:
            print(type(ex), ex)

def add_pk_constraint_edge_table(conn, GRAPH_NAME) -> None:
    stmt = '''ALTER TABLE %s."%s" 
                ADD CONSTRAINT _ag_label_edge_pkey PRIMARY KEY(id);
                    ''' % (GRAPH_NAME, '_ag_label_edge')
    with conn.cursor() as cursor:
        try:
            cursor.execute(stmt)
        except Exception as ex:
            print(type(ex), ex)

def create_vertex_label(conn, GRAPH_NAME, label_name) -> None:
    stmt = '''SELECT create_vlabel('%s', '%s')
                WHERE _label_id('%s', '%s') = 0;
                    ''' % (GRAPH_NAME, label_name, GRAPH_NAME, label_name)
    with conn.cursor() as cursor:
        try:
            cursor.execute(stmt)
        except Exception as ex:
            print(type(ex), ex)

def create_edge_label(conn, GRAPH_NAME, label_name) -> None:
    stmt = '''SELECT create_elabel('%s', '%s')
                WHERE _label_id('%s', '%s') = 0;
                    ''' % (GRAPH_NAME, label_name, GRAPH_NAME, label_name)
    with conn.cursor() as cursor:
        try:
            cursor.execute(stmt)
        except Exception as ex:
            print(type(ex), ex)

def vertices_in_mem(conn, GRAPH_NAME, label) -> list:
    stmt = '''SELECT (properties -> 'id')::VARCHAR, id 
                        FROM %s."%s"''' % (GRAPH_NAME, label)
    with conn.cursor() as cursor:
        try:
            cursor.execute(stmt)
            ver = cursor.fetchall()
        except Exception as ex:
            print(type(ex), ex)
    ver2 = []
    for i in ver:
        i = list(i)
        i[0] = i[0].replace('"', '')
        i = tuple(i)
        ver2.append(i)
    ver = ver2
    del ver2
    return ver