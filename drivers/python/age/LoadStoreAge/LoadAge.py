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

import age, csv, json
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

def load_labels_from_file(conn, GRAPH_NAME,
                          file_path: str, label_name: str) -> None:
    
    def load_labels_from_csv() -> None:
        label_id = get_label_id(conn, GRAPH_NAME, label_name)
        header, records = get_contents_from_csv(file_path)
        graphid = make_graphid(label_id, 1)
        records_lst = []
        for record in records:
            res = json.dumps(dict(zip(header, record)))
            records_lst.append((str(graphid), res))
            graphid += 1
        insert_into_vertices_table(conn, GRAPH_NAME, label_name, records_lst)

    def load_labels_from_json() -> None:
        label_id = get_label_id(conn, GRAPH_NAME, label_name)
        with open(file_path, 'r') as openfile:
            json_obj_lst = json.load(openfile)
        records_lst = []
        graphid = make_graphid(label_id, 1)
        for json_obj in json_obj_lst:
            json_obj = json.dumps(json_obj)
            records_lst.append((str(graphid), json_obj))
            graphid += 1
        insert_into_vertices_table(conn, GRAPH_NAME, label_name, records_lst)


    age.setUpAge(conn, GRAPH_NAME)

    stmt = '''SELECT create_vlabel('%s', '%s')
                WHERE _label_id('%s', '%s') = 0;
                    ''' % (GRAPH_NAME, label_name, GRAPH_NAME, label_name)
    with conn.cursor() as cursor:
        try:
            cursor.execute(stmt)
        except Exception as ex:
            print(type(ex), ex)

    drop_pk_constraint_vertex_table(conn, GRAPH_NAME)

    if file_path.endswith('csv'):
        load_labels_from_csv()
    elif file_path.endswith('json'):
        load_labels_from_json()

    add_pk_constraint_vertex_table(conn, GRAPH_NAME)
    conn.commit()


def load_edges_from_file(conn, GRAPH_NAME,
                          file_path: str, label_name: str) -> None:

    def load_edges_from_csv() -> None:
        label_id = get_label_id(conn, GRAPH_NAME, label_name)
        header, records = get_contents_from_csv(file_path)
        vertices_labels = []
        for record in records:
            if record[1] not in vertices_labels:
                vertices_labels.append(record[1])
            if record[3] not in vertices_labels:
                vertices_labels.append(record[3])
        vertices = []
        for label in vertices_labels:
            vertices_dict = {}
            stmt = '''SELECT properties -> 'id', id 
                        FROM %s."%s"''' % (GRAPH_NAME, label)
            with conn.cursor() as cursor:
                try:
                    cursor.execute(stmt)
                    ver = cursor.fetchall()
                except Exception as ex:
                    print(type(ex), ex)
            vertices_dict = dict(ver)
            vertices.append(vertices_dict)
        records_lst = []
        graphid = make_graphid(label_id, 1)
        for record in records:
            start_index = vertices_labels.index(record[1])
            end_index = vertices_labels.index(record[3])
            start_id = vertices[start_index][record[0]]
            end_id = vertices[end_index][record[2]]
            properties = json.dumps(dict(zip(header[4:], record[4:])))
            records_lst.append((str(graphid), str(start_id), 
                                str(end_id), properties))
            graphid += 1
        insert_into_edges_table(conn, GRAPH_NAME, label_name, records_lst)

    def load_edges_from_json() -> None:
        label_id = get_label_id(conn, GRAPH_NAME, label_name)
        with open(file_path, 'r') as openfile:
            json_lst = json.load(openfile)
        vertices_labels = []
        for json_dict in json_lst:
            if json_dict["start_vertex_type"] not in vertices_labels:
                vertices_labels.append(json_dict["start_vertex_type"])
            if json_dict["end_vertex_type"] not in vertices_labels:
                vertices_labels.append(json_dict["end_vertex_type"])
        vertices = []
        for label in vertices_labels:
            vertices_dict = {}
            stmt = '''SELECT properties -> 'id', id 
                        FROM %s."%s"''' % (GRAPH_NAME, label)
            with conn.cursor() as cursor:
                try:
                    cursor.execute(stmt)
                    ver = cursor.fetchall()
                except Exception as ex:
                    print(type(ex), ex)
            vertices_dict = dict(ver)
            vertices.append(vertices_dict)
        records_lst = []
        graphid = make_graphid(label_id, 1)
        for json_dict in json_lst:
            start_index = vertices_labels.index(json_dict["start_vertex_type"])
            end_index = vertices_labels.index(json_dict["end_vertex_type"])
            start_id = vertices[start_index][json_dict["start_id"]]
            end_id = vertices[end_index][json_dict["end_id"]]
            properties = json.dumps(json_dict["properties"])
            records_lst.append((str(graphid), str(start_id), 
                                str(end_id), properties))
            graphid += 1
        insert_into_edges_table(conn, GRAPH_NAME, label_name, records_lst)


    age.setUpAge(conn, GRAPH_NAME)

    stmt = '''SELECT create_elabel('%s', '%s')
                WHERE _label_id('%s', '%s') = 0;
                    ''' % (GRAPH_NAME, label_name, GRAPH_NAME, label_name)
    with conn.cursor() as cursor:
        try:
            cursor.execute(stmt)
        except Exception as ex:
            print(type(ex), ex)

    drop_pk_constraint_edge_table(conn, GRAPH_NAME)

    if file_path.endswith('csv'):
        load_edges_from_csv()
    elif file_path.endswith('json'):
        load_edges_from_json()

    add_pk_constraint_edge_table(conn, GRAPH_NAME)

    conn.commit()