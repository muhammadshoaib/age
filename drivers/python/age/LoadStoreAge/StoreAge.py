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

import age, csv, json, os

def get_label_id(graphid):
    ENTRY_ID_BITS = 32 + 16
    tmp = graphid >> ENTRY_ID_BITS
    return tmp

def load_labels_into_file(conn, GRAPH_NAME, dir_path: str) -> None:
    
    def load_labels_into_csv() -> None:
        data_file = each_vertex_label[0] + '.csv'
        with open(os.path.join(data_dir_path, data_file), 'w') as file:
            writer = csv.writer(file)
            writer.writerow(list((vertices[0][0]).keys()))
            for each_vertex in vertices:
                writer.writerow(list(each_vertex[0].values()))
        file.close()
 
    def load_labels_into_json() -> None:
        json_list = []
        for each_vertex in vertices:
            json_dict = each_vertex[0]
            json_list.append(json_dict)
        json_object = json.dumps(json_list, indent=4)
        data_file = each_vertex_label[0] + '.json'
        with open(os.path.join(data_dir_path, data_file), 'w') as file:
            file.write(json_object)
        file.close()

    age.setUpAge(conn, GRAPH_NAME)
    data_dir = 'data'
    try:
        data_dir_path = os.path.join(dir_path, data_dir)
        os.mkdir(data_dir_path)
    except Exception as ex:
        print(type(ex), ex)

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

    for each_vertex_label in vertex_labels:
        stmt = '''SELECT properties FROM 
                    %s."%s"''' % (GRAPH_NAME, each_vertex_label[0])
        with conn.cursor() as cursor:
            try:
                cursor.execute(stmt)
                vertices = cursor.fetchall()
            except Exception as ex:
                print(type(ex), ex)
        var = any(isinstance(i,dict) for j in vertices 
                  for i in j[0].values())
        load_labels_into_json() if var else load_labels_into_csv()


def load_edges_into_file(conn, GRAPH_NAME, dir_path: str) -> None:

    def load_edges_into_csv() -> None:
        records_lst = []
        properties_keys = edges[0][2].keys()
        for each_edge in edges:
            start_id = get_label_id(int(each_edge[0]))
            end_id = get_label_id(int(each_edge[1]))
            start_index = vertices_labels_ids.index(start_id)
            end_index = vertices_labels_ids.index(end_id)
            start_vertex_type = vertices_labels_names[start_index]
            end_vertex_type = vertices_labels_names[end_index]
            start_id = vertices[start_index][each_edge[0]]
            end_id = vertices[end_index][each_edge[1]]
            properties_values = each_edge[2].values()
            records_lst.append([start_id, start_vertex_type.replace('"', ''), 
                                end_id, 
                                end_vertex_type.replace('"', '')] 
                                + 
                                list(properties_values))
        
        file_path = each_edge_label[0] + '.csv'

        with open(os.path.join(data_dir_path, file_path), 'w') as file:
            writer = csv.writer(file)
            writer.writerow(['start_id', 'start_vertex_type', 
                            'end_id', 
                            'end_vertex_type'] 
                            + 
                            list(properties_keys))
            writer.writerows(records_lst)
        file.close()

    def load_edges_into_json() -> None:
        records_lst = []
        for each_edge in edges:
            start_id = get_label_id(int(each_edge[0]))
            end_id = get_label_id(int(each_edge[1]))
            start_index = vertices_labels_ids.index(start_id)
            end_index = vertices_labels_ids.index(end_id)
            start_vertex_type = vertices_labels_names[start_index]
            end_vertex_type = vertices_labels_names[end_index]
            start_id = vertices[start_index][each_edge[0]]
            end_id = vertices[end_index][each_edge[1]]
            json_dict = {
                "start_id" : start_id,
                "start_vertex_type" : start_vertex_type.replace('\"', ''),
                "end_id" : end_id,
                "end_vertex_type" : end_vertex_type.replace('\"', ''),
                "properties" : each_edge[2]
            }
            records_lst.append(json_dict)
        file_path = each_edge_label[0] + '.json'
        json_object = json.dumps(records_lst, indent=4)
        with open(os.path.join(data_dir_path, file_path), 'w') as file:
            file.write(json_object)    
        file.close()

    age.setUpAge(conn, GRAPH_NAME)
    data_dir = 'data'
    try:
        data_dir_path = os.path.join(dir_path, data_dir)
        os.mkdir(data_dir_path)
    except Exception as ex:
        print(type(ex), ex)

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

    stmt = '''SELECT graph FROM ag_label 
                WHERE relation = '%s._ag_label_vertex'::regclass;
                    ''' % (GRAPH_NAME,)
    with conn.cursor() as cursor:
        try:
            cursor.execute(stmt)
            GRAPH_OID = cursor.fetchall()[0][0]
        except Exception as ex:
            print(type(ex), ex)

    for each_edge_label in edge_labels:
        stmt = '''SELECT start_id, end_id, properties 
                    FROM %s."%s"''' % (GRAPH_NAME, each_edge_label[0])
        with conn.cursor() as cursor:
            try:
                cursor.execute(stmt)
                edges = cursor.fetchall()
            except Exception as ex:
                print(type(ex), ex)
                conn.rollback()
        vertices_labels_ids, vertices_labels_names = [], []
        for each_edge in edges:
            start_label_id = get_label_id(int(each_edge[0]))
            end_label_id = get_label_id(int(each_edge[1]))
            if start_label_id not in vertices_labels_ids:
                stmt = '''SELECT split_part((SELECT relation FROM ag_label 
                            WHERE graph=%s AND id=%s)::regclass::TEXT, '.', 2);
                                ''' % (GRAPH_OID, start_label_id)
                with conn.cursor() as cursor:
                    try:
                        cursor.execute(stmt)
                        vertices_labels_ids.append(start_label_id)
                        vertices_labels_names.append(cursor.fetchall()[0][0])
                    except Exception as ex:
                        print(type(ex), ex)
            
            if end_label_id not in vertices_labels_ids:
                stmt = '''SELECT split_part((SELECT relation FROM ag_label 
                            WHERE graph=%s AND id=%s)::regclass::TEXT, '.', 2);
                                ''' % (GRAPH_OID, end_label_id)
                with conn.cursor() as cursor:
                    try:
                        cursor.execute(stmt)
                        vertices_labels_ids.append(end_label_id)
                        vertices_labels_names.append(cursor.fetchall()[0][0])
                    except Exception as ex:
                        print(type(ex), ex)
        vertices = []
        for label in vertices_labels_names:
            vertices_dict = {}
            stmt = '''SELECT id, properties -> 'id' 
                        FROM %s.%s''' % (GRAPH_NAME, label)
            with conn.cursor() as cursor:
                try:
                    cursor.execute(stmt)
                    ver = cursor.fetchall()
                except Exception as ex:
                    print(type(ex), ex)
            vertices_dict = dict(ver)
            vertices.append(vertices_dict)
        var = any(isinstance(i,dict) for j in edges 
                  for i in j[2].values())
        load_edges_into_json() if var else load_edges_into_csv()