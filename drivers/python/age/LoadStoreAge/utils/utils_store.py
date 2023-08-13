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

import os, json

def setup_data_dir(dir_path) -> str:
    data_dir = 'data'
    try:
        data_dir_path = os.path.join(dir_path, data_dir)
        os.mkdir(data_dir_path)
    except Exception as ex:
        print(type(ex), ex)
    return data_dir_path

def get_label_id(graphid) -> int:
    ENTRY_ID_BITS = 32 + 16
    tmp = graphid >> ENTRY_ID_BITS
    return tmp

def get_graph_oid(conn, GRAPH_NAME) -> int:
    stmt = '''SELECT graph FROM ag_label 
                WHERE relation = '%s._ag_label_vertex'::regclass;
                    ''' % (GRAPH_NAME,)
    with conn.cursor() as cursor:
        try:
            cursor.execute(stmt)
            GRAPH_OID = cursor.fetchall()[0][0]
        except Exception as ex:
            print(type(ex), ex)
    return GRAPH_OID

def get_all_vertex_labels(conn, GRAPH_NAME) -> list:
    with conn.cursor() as cursor:
        try:
            cursor.execute('''
                SELECT c.relname                                                                                       
                FROM pg_inherits 
                JOIN pg_class AS c ON (inhrelid=c.oid)
                JOIN pg_class as p ON (inhparent=p.oid)
                JOIN pg_namespace pn ON pn.oid = p.relnamespace
                JOIN pg_namespace cn ON cn.oid = c.relnamespace
                WHERE p.relname = '_ag_label_vertex' and pn.nspname = %s;  
            ''', (GRAPH_NAME,))
            vertex_labels = cursor.fetchall()
        except Exception as ex: 
            print(type(ex), ex)
    return vertex_labels

def get_all_vertices(conn, GRAPH_NAME, label_name) -> list:
    stmt = '''SELECT (properties)::VARCHAR FROM 
                    %s."%s"''' % (GRAPH_NAME, label_name)
    with conn.cursor() as cursor:
        try:
            cursor.execute(stmt)
            vertices = cursor.fetchall()
        except Exception as ex:
            print(type(ex), ex)
    vertices2 = []
    for i in vertices:
        vertices2.append((json.loads(i[0]),))
    vertices = vertices2
    del vertices2
    return vertices

def get_all_edge_labels(conn, GRAPH_NAME) -> list:
    with conn.cursor() as cursor:
        try:
            cursor.execute('''
                SELECT c.relname                                                                                       
                FROM pg_inherits 
                JOIN pg_class AS c ON (inhrelid=c.oid)
                JOIN pg_class as p ON (inhparent=p.oid)
                JOIN pg_namespace pn ON pn.oid = p.relnamespace
                JOIN pg_namespace cn ON cn.oid = c.relnamespace
                WHERE p.relname = '_ag_label_edge' and pn.nspname = %s;  
            ''', (GRAPH_NAME,))
            
            edge_labels = cursor.fetchall()
        except Exception as ex: 
            print(type(ex), ex)
            conn.rollback()
    return edge_labels

def get_all_edges(conn, GRAPH_NAME, label_name) -> list:
    stmt = '''SELECT start_id, end_id, (properties)::VARCHAR 
                    FROM %s."%s"''' % (GRAPH_NAME, label_name)
    with conn.cursor() as cursor:
        try:
            cursor.execute(stmt)
            edges = cursor.fetchall()
        except Exception as ex:
            print(type(ex), ex)
            conn.rollback()

    edges2 = []
    for i in edges:
        edges2.append((i[0], i[1], json.loads(i[2]),))
    edges = edges2
    del edges2
    return edges

def store_rel_vertex_data(conn, GRAPH_OID, label_id, 
                          vertices_labels_ids, vertices_labels_names):
    stmt = '''SELECT split_part((SELECT relation FROM ag_label 
                            WHERE graph=%s AND id=%s)::regclass::TEXT, '.', 2);
                                ''' % (GRAPH_OID, label_id)
    with conn.cursor() as cursor:
        try:
            cursor.execute(stmt)
            vertices_labels_ids.append(label_id)
            vertices_labels_names.append(cursor.fetchall()[0][0])
        except Exception as ex:
            print(type(ex), ex)

def vertices_in_mem(conn, GRAPH_NAME, label) -> list:
    stmt = '''SELECT id, (properties -> 'id')::VARCHAR 
                        FROM %s.%s''' % (GRAPH_NAME, label)
    with conn.cursor() as cursor:
        try:
            cursor.execute(stmt)
            ver = cursor.fetchall()
        except Exception as ex:
            print(type(ex), ex)

    ver2 = []
    for i in ver:
        ver2.append((i[0], json.loads(i[1]),))
    ver = ver2
    del ver2
    return ver