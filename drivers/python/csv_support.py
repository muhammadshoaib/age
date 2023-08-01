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
import json
import os
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from psycopg2 import extensions as ext
import age 
from age.exceptions import *
from typing import Dict
import time


def storeAgetoCsv(conn: ext.connection, graph_name: str, path: str): 

    """
        - This function exports the specified graph from AGE to CSV files.
        - If a vertex or node has nested properties, it is loaded to a JSON file instead.
        - The Function fetched data directly from the AGE tables bypassing Cypher queries.

        Parameters:
        - conn: A psycopg2 connection object to Postgres.
            Example:
                conn = psycopg2.connect(host="", 
                                    database="", 
                                    user="", 
                                    password="")

        - graph_name: The name of the graph to be exported.
        - path: The directory path where the exported CSV/JSON files will be saved.
        - The function throws an exception if any errors occur during the export process. 
    """

    try: 
        vertex_map = {}  ## Map to store vertex_id : vertex_label
        edge_tables = []

        def writeToCsv(label: str, headers: list, data: list):
            if not data:
                return
            with open(path + label + '.csv', 'w') as file:
                writer = csv.writer(file)
                writer.writerow(headers)
                writer.writerows(data)
                file.close()

        def writeToJson(label: str, data: list):
            if not data:
                return
            with open(path + label + '.json', 'w') as file:
                json.dump(data, file, indent=4)
                file.close()

        def createVerticesCsv(conn: ext.connection, graph_name: str, vertex_label: str):
            try:
                with conn.cursor() as cursor: 
                    query = """
                            SELECT * FROM "%s"."%s"
                        """ % (graph_name, vertex_label)
                    cursor.execute(sql.SQL(query))
                    query_output = cursor.fetchall()
                    ## Prepare the data rows
                    data_for_csv = []
                    data_for_json = []
                    headers = ['id'] # List to store CSV headers

                    for row in query_output:
                        if isinstance(row[1], str):  ## Check if the properties are a string
                            properties = json.loads(row[1])
                        else:  ## If properties are a dictionary
                            properties = row[1]    

                        ## Check if properties are nested, if yes write to JSON else CSV
                        if any(isinstance(v, dict) for v in properties.values()):
                            data_for_json.append({"id": row[0], **properties})
                            vertex_map[row[0]] = vertex_label
                        else:
                            values = [row[0]] + list(properties.values())
                            data_for_csv.append(values)
                            vertex_map[row[0]] = vertex_label

                    headers += list(properties.keys())

                    writeToCsv(vertex_label, headers, data_for_csv)
                    writeToJson(vertex_label, data_for_json)

                    ## Return the vertex_map to be used for the edge tables
                    return vertex_map 
            except Exception as e:
                print("Exception:",e)
                raise e

        def createEdgesCsv(conn: ext.connection, graph_name: str, edge_label: str):
            try:
                with conn.cursor() as cursor: 
                    query = """
                            SELECT * FROM "%s"."%s"
                        """ % (graph_name, edge_label)
                    cursor.execute(sql.SQL(query))
                    query_output = cursor.fetchall()

                    data_for_csv = []
                    data_for_json = []
                    headers = ['start_id', 'start_vertex_type', 'end_id', 'end_vertex_type']
                    for row in query_output:

                        if isinstance(row[3], str):  # Check if the properties are a string
                            properties = json.loads(row[3])
                        else:  
                            properties = row[3]

                        ## Retrieve vertex label from vertex_map else give default value
                        start_vertex_type = vertex_map.get(row[1], '_ag_label_vertex')
                        end_vertex_type = vertex_map.get(row[2], '_ag_label_vertex')

                        ## Check if properties are nested, if yes write to JSON else CSV
                        if any(isinstance(v, dict) for v in properties.values()):
                            data_for_json.append({
                                "start_id": row[1], 
                                "start_vertex_type": start_vertex_type, 
                                "end_id": row[2], 
                                "end_vertex_type": end_vertex_type, 
                                **properties
                            })
                        else:
                            values = [row[1], start_vertex_type, row[2], end_vertex_type] 
                            values.extend(list(properties.values()))
                            data_for_csv.append(values)
                    headers += list(properties.keys())
                    
                    writeToCsv(edge_label, headers, data_for_csv)
                    writeToJson(edge_label, data_for_json)

            except Exception as e:
                print("Exception:", e)
                raise e

        ## Check if graph exists
        with conn.cursor() as cursor:
            query = """
                    SELECT count(*) FROM ag_catalog.ag_graph 
                    WHERE name='%s'
                """ % (graph_name)
            cursor.execute(sql.SQL(query))            
            if cursor.fetchone()[0] == 0:
                raise GraphNotFound(graph_name)
            
            ## Extract graph tables
            query = """
                SELECT table_name  FROM information_schema.tables
                  WHERE table_schema = '%s'
                """ % (graph_name)
            cursor.execute(sql.SQL(query))
            tables = cursor.fetchall()

            for table in tables:
                if table[0] == '_ag_label_vertex' or table[0] == '_ag_label_edge':
                    continue
                query = """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = '%s' AND table_name = '%s'
                """ % (graph_name, table[0])
                cursor.execute(sql.SQL(query))
                columns = cursor.fetchall()

                ## Check if table is edge table by checking specific columns 
                if 'start_id' in columns[1]:
                    edge_tables.append(table[0])
                else:
                    ## Update vertex_map for each vertex table while creating CSV 
                    vertex_map.update(createVerticesCsv(conn, graph_name, table[0]))        
        for table in edge_tables:
            createEdgesCsv(conn, graph_name, table)  
        
    except Exception as e:
        print("Exception:",e)
        raise e

## ------------------------- Function to store CSV files to AGE -------------------------------

def storeCsvtoAge(conn: ext.connection, graph_name: str, path: str, with_labels: bool = True):

    """
    - This function loads CSV / JSON data into an Apache AGE graph database.
    - Data should be appropriately formatted for the function to work.
    FOR CSV FILES:
        In Vertex Files: atleast 1 column named as 'id' is required.
        In Edge Files: Following Columns are required:
                 1. 'start_id', 2. 'end_id', 3. 'start_vertex_type', 4. 'end_vertex_type'
    
    - FOR JSON FILES:
        For Vertex Files: The JSON file should contain a key named 'id' for each vertex.
        For Edge Files: The JSON file should contain keys named as following: 
                1. 'start_id', 2. 'end_id', 3. 'start_vertex_type', 4. 'end_vertex_type' for each edge.   

    NOTE: In CSV and JSON files, additional columns and keys respectively will count as properties.

    - The function also supports bulk data loading to improve performance.
    - The function throws an exception if any errors occur during the loading process.

    Parameters:

    conn: ext.connection - The database connection object.
    graph_name: str - The name of the Apache AGE graph.
    path: str - The path to the CSV or JSON file or directory containing such files.
    with_labels: bool - An optional argument which specifies whether
                         new labels should be created in the database. 
    - Default value  for with_labels is True.
    - If with_labels is False, function assumes that vlabels and elabels already exist.

    NOTE: Set with_labels to False if you are loading data to an existing graph.
    
    Raises:
    Exception: An exception is raised if the given path is neither a file nor a directory 
               or if the file format is invalid.
    """

    try:
        vertex_labels = []
        edge_labels = {}
        vertex_map = {}

        ## Set up Age
        age.setUpAge(conn, graph_name)

        def load_file(file_path: str, data_format: str):
            if data_format == 'csv':
                with open(file_path, 'r') as f:
                    reader = csv.reader(f)
                    headers = next(reader)
                    ## Check if the first column is 'id' and handle duplicate columns
                    id_indices = [i for i, x in enumerate(headers) if x == "id"]
                    for idx in id_indices[1:]:
                        headers[idx] = '_id'
                    f.seek(0)  # reset file pointer to beginning
                    reader = csv.DictReader(f, fieldnames=headers)
                    next(reader)  # Skip the header
                    return list(reader)
            elif data_format == 'json':
                with open(file_path, 'r') as json_file:
                    reader = json.load(json_file)
                    return reader
            else:
                raise ValueError("Invalid File Format! Expected 'csv' or 'json'.")

        def loadVertices(conn, graph_name, vertex_label,
                          file_path, data_format):
            try: 
                with conn.cursor() as cursor:
                    data = []
                    vertex_ids = []  # List to store all vertex_ids
                    reader = load_file(file_path, data_format)

                    for row in reader:
                        vertex_id = row.pop('id')
                        properties = json.dumps(row)
                        data.append((properties,))
                        vertex_ids.append(vertex_id) 
                    ## Dictionary to store vertex_id : generated_id 
                    id_dict = {} 
                    chunk_size =1000  ## Chunk size for bulk insert
                    for i in range(0, len(data), chunk_size):
                        data_chunk = data[i:i+chunk_size]
                        chunk = vertex_ids[i:i+chunk_size]
                        ## Bulk Inserting Data 
                        psycopg2.extras.execute_values(
                            cursor,
                            f"""INSERT INTO "{graph_name}"."{vertex_label}" 
                            (properties) VALUES %s RETURNING id""",
                            data_chunk, page_size=1010
                        )
                        ids = cursor.fetchall()
                        for vertex_id, generated_id in zip(chunk, ids): 
                            id_dict[vertex_id] = generated_id[0]        
                    conn.commit()
                    ## Return the id_dict to be used for the edge tables
                    return id_dict
            except Exception as e:
                print("Exception:", e)
                raise e

        def loadEdges(conn, graph_name: str, edge_label: str,
                       file_path: str, id_dict: Dict[str, int], data_format: str):
            try:
                with conn.cursor() as cursor:
                    data = []
                    reader = load_file(file_path, data_format)

                    for row in reader:
                        start_id = id_dict[row.pop('start_id')]
                        end_id = id_dict[row.pop('end_id')]
                        row.pop('start_vertex_type', None)
                        row.pop('end_vertex_type', None)
                        properties = json.dumps(row)
                        data.append((start_id, end_id, properties))
                    psycopg2.extras.execute_values(
                        cursor,
                        f"""INSERT INTO "{graph_name}"."{edge_label}" 
                        (start_id, end_id, properties) VALUES %s""",
                        data, page_size=1010
                    )
                    conn.commit()
            except Exception as e:
                print("Exception:", e)
                raise e

        ## Process each file in the path for loading
        def processFile(file_path: str, filename: str):
            if filename.endswith('.csv'):
                label = filename.replace('.csv', '')
                data_format = 'csv'
            elif filename.endswith('.json'):
                label = filename.replace('.json', '')
                data_format = 'json'
            else:
                return
            with open(file_path, 'r') as f:
                if data_format == 'csv':
                    reader = csv.DictReader(f)
                    headers = reader.fieldnames
                else:  # json
                    reader = json.load(f)
                    headers = reader[0].keys() if reader else {}

                with conn.cursor() as cursor:
                    ## Check if the file is an edge file
                    if 'start_id' in headers and 'end_id' in headers: 
                        ## Check if the edge label already exists
                        if label not in edge_labels and with_labels:
                            query = f"SELECT create_elabel('{graph_name}', '{label}')"
                            cursor.execute(query)
                        ## Map to handle files with same name i.e. same edge label
                            edge_labels[label] = [file_path]
                        else:
                            edge_labels[label].append(file_path)
                    else: 
                        if label not in vertex_labels and with_labels:
                            query = f"SELECT create_vlabel('{graph_name}', '{label}')"
                            cursor.execute(query)                          
                            vertex_labels.append(label)
                        vertex_map.update(loadVertices(conn, graph_name, label, file_path, data_format)) 
                conn.commit()
        

        if os.path.isdir(path):
            files = os.listdir(path)
            for file in files:
                file_path = os.path.join(path, file)
                processFile(file_path, file)  
        elif os.path.isfile(path):
            filename = os.path.basename(path)
            processFile(path, filename) 
        else:
            raise Exception("The specified path is not a directory or a file!")
        
        for label, file_paths in edge_labels.items():
            for file_path in file_paths:
                if file_path.endswith('.csv'):
                    loadEdges(conn, graph_name, label, file_path, vertex_map, 'csv')
                elif file_path.endswith('.json'):
                    loadEdges(conn, graph_name, label, file_path, vertex_map, 'json')

    except Exception as e:
        print("Exception:",e)
        raise e
    
if __name__ == "__main__":
    conn = psycopg2.connect(host="localhost", 
                            database="testdb", 
                            user="safi", 
                            password="safi")
    
    s = time.time()
    # storeAgetoCsv(conn, "nested", "/Users/safi/Desktop/data/aaa/")
    storeCsvtoAge(conn, "aaa1", "/Users/safi/Desktop/data/aaa/")
    e = time.time()
    print("Time taken:", e-s)
    conn.commit()
    conn.close()
