
import psycopg2
import os
import csv
import pandas as pd
import ast
from age import *
from typing import Dict, Any, Optional
import time


def load_csv(connection: psycopg2.connect,
             GRAPH_NAME: str,
             directory: str) -> None:
    """
    Generating a graph from a csv file
    Must follow the naming rules for generating nodes and edges

    vertices file name format : label_name.csv
    edges file name format : edge_label_name.csv

    Example:
    - Creating nodes with label 'People'
        - `People.csv`
    - Creating edges with label 'Purchase'
        - `edge_Purchase.csv`

    @Params
    -------

    connection - psycopg2.connect
        A connection object when retruning from psycopg2.connect

    GRAPH_NAME - string
        Name of the Graph

    directory - string
        Directiory of all the csv files

    @Returns
    --------
    None
    """

    start_time = time.perf_counter()

    print("-------------------------------------------")
    print('Checking file...')
    print("-------------------------------------------")

    file_name_list = os.listdir(directory)
    # Check if all csv files are valid
    try:
        for file_name in file_name_list:
            file_path = os.path.join(directory, file_name)
            if os.path.splitext(file_path)[1] != '.csv':
                raise Exception('Not a csv file')
            # Try to open the file and parse it as a CSV file using the csv.reader() function
            with open(file_path, 'r') as file:
                csv_reader = csv.reader(file)
                # Iterate through the rows of the CSV file to ensure it's valid
                for row in csv_reader:
                    pass
    except csv.Error as e:
        raise Exception(e)

    age.setUpAge(connection, GRAPH_NAME)

    # python dict to age cypher string
    def cypher_property_to_string(property):
        # print(str(type(property)),  property)
        if isinstance(property, dict):
            p = "{"
            for x, y in property.items():
                p += x + " : "
                if isinstance(y, dict):
                    p += cypher_property_to_string(y)
                    p += ","
                elif isinstance(y, list):
                    p += cypher_property_to_string(y)
                    p += ','
                else:
                    p += "'"
                    p += str(y)
                    p += "',"
            p = p.removesuffix(',')
            p += "}"
        elif isinstance(property, list):
            p = "["
            for x in property:
                if isinstance(x, dict):
                    p += cypher_property_to_string(x)
                    p += ","
                elif isinstance(x, list):
                    p += cypher_property_to_string(x)
                    p += ','
                else:
                    p += "'"
                    p += str(x)
                    p += "',"
            p = p.removesuffix(',')
            p += "]"
        return p

    def get_vertices_query(label: str, properties: Dict[str, Any]) -> None:
        """Get cypher query for vertices"""
        query = """
        SELECT * from cypher(
            '%s',
            $$
                CREATE (v:%s %s)
                RETURN v
            $$
        ) as (v agtype);
        """ % (GRAPH_NAME, label, cypher_property_to_string(properties))
        return query

    def executeQuery(query):
        with connection.cursor() as cursor:
            try:
                cursor.execute(query)
                connection.commit()
            except Exception as ex:
                print(type(ex), ex)
                connection.rollback()

    def extract(data):
        try:
            if isinstance(data, list):
                ls = []
                for x in data:
                    ls.append(extract(x))
                return ls
            elif isinstance(data, dict):
                mp = {}
                for key in data.keys():
                    mp[key] = extract(data[key])
                return mp
            elif isinstance(data, str):
                if type(data) != type(ast.literal_eval(data)):
                    return extract(ast.literal_eval(data))
                return data
            else:
                return data
        except:
            return data

    def load_vertices(file_path: str):

        # Setting up connection to work with Graph
        age.setUpAge(connection, GRAPH_NAME)

        file_name_with_extension = os.path.basename(file_path)
        label = os.path.splitext(file_name_with_extension)[0]

        df = pd.read_csv(file_path)
        query = ""
        for rowNumber, row in df.iterrows():
            properties = df.to_dict(orient='records')[rowNumber]
            properties = extract(properties)
            query += get_vertices_query(label=label, properties=properties)
            query += "\n"
            # print(query)
            if ((rowNumber+1) % 1000 == 0):
                executeQuery(query)
                now_time = time.perf_counter()
                elapsed_time_ms = (now_time - start_time)
                print(
                    f'Loaded {(rowNumber+1)} vertices with label : {label} within {elapsed_time_ms} s')
                query = ''
        if (query != ''):
            executeQuery(query)
            now_time = time.perf_counter()
            elapsed_time_ms = (now_time - start_time)
            print(
                f'Loaded {(rowNumber+1)} vertices with label : {label} within {elapsed_time_ms} s')
            query = ''

    def get_edge_query(
            start_id: str | int,
            start_label: str,
            end_id: str | int,
            end_label: str,
            edge_label: str,
            edge_properties: Dict[str, Any]) -> None:
        """Get cypher query for vertices"""

        del edge_properties['start_id']
        del edge_properties['start_vertex_type']
        del edge_properties['end_id']
        del edge_properties['end_vertex_type']

        query = """
            SELECT * from cypher('%s', 
            $$
                MATCH (a:%s {id:'%s'}), (b:%s {id:'%s'})
                CREATE (a)-[e:%s %s]->(b)
                RETURN e
            $$) as (e agtype);
        """ % (GRAPH_NAME,
                start_label,
                start_id,
                end_label,
                end_id,
                edge_label,
                cypher_property_to_string(edge_properties))
        return query

    def load_edge(file_path: str):
        # Setting up connection to work with Graph
        age.setUpAge(connection, GRAPH_NAME)

        file_name_with_extension = os.path.basename(file_path)
        label = os.path.splitext(file_name_with_extension)[0]
        label = label.removeprefix('edge_')

        df = pd.read_csv(file_path)
        query = ""
        for rowNumber, row in df.iterrows():
            properties = df.to_dict(orient='records')[rowNumber]
            properties = extract(properties)
            # print(properties)
            query += get_edge_query(start_id=properties['start_id'],
                                    start_label=properties['start_vertex_type'],
                                    end_id=properties['end_id'],
                                    end_label=properties['end_vertex_type'],
                                    edge_label=label,
                                    edge_properties=properties)
            query += "\n"
        #     # print(query)
            if ((rowNumber+1) % 1000 == 0):
                executeQuery(query)
                now_time = time.perf_counter()
                elapsed_time_ms = (now_time - start_time)
                print(
                    f'Loaded {(rowNumber+1)} edges with label : {label} within {elapsed_time_ms} s')
                query = ''
        if (query != ''):
            executeQuery(query)
            now_time = time.perf_counter()
            elapsed_time_ms = (now_time - start_time)
            print(
                f'Loaded {(rowNumber+1)} edges with label : {label} within {elapsed_time_ms} s')
            query = ''

    edgeCount = len([s for s in file_name_list if s.startswith('edge_')])

    print(f'{len(file_name_list)} files are ok')
    print(f'Found vertices file : {len(file_name_list) - edgeCount}')
    print(f'Found edge file : {edgeCount} ')
    print("-------------------------------------------")

    # Add Vertices
    for file_name in file_name_list:
        if not file_name.startswith('edge_'):
            load_vertices(file_path=os.path.join(directory, file_name))

    for file_name in file_name_list:
        if file_name.startswith('edge_'):
            load_edge(file_path=os.path.join(directory, file_name))


def save_csv(connection: psycopg2.connect,
             GRAPH_NAME: str,
             directory: Optional[str] = '') -> None:
    """
    Saves all the information of the graph in csv format

    About the algo
    -------------
    - Save the edge relation accoring to the nodes property 'id'
    - if Nodes 'id' property not found then in that case it will use the autogenerated 'gid' of nodes

    @Params
    -------
    connection - psycopg2.connect
        A connection object when retruning from psycopg2.connect

    GRAPH_NAME - string
        Name of the Graph

    directory - string
        Directiory of all the csv files

    @Returns
    --------
    None
    """

    if directory == '':
        directory = os.path.dirname(__file__)

    # Checking if graph exist
    with connection.cursor() as cursor:
        query = """
                    SELECT oid
                    FROM ag_catalog.ag_graph 
                    WHERE name='%s'
                """ % (GRAPH_NAME)
        try:
            cursor.execute(psycopg2.sql.SQL(query))
            oid = cursor.fetchone()[0]
        except:
            raise GraphNotFound(GRAPH_NAME)

    # Get all levels of the graph
    edge_label = []
    vertex_label = []
    with connection.cursor() as cursor:
        query = """
                    SELECT name, kind
                    FROM ag_catalog.ag_label
                    WHERE graph = %s
                """ % (oid)
        cursor.execute(sql.SQL(query))
        for i, row in enumerate(cursor):
            if (i <= 1):
                continue
            if (row[1] == 'v'):
                vertex_label.append(row[0])
            else:
                edge_label.append(row[0])

    age.setUpAge(connection, GRAPH_NAME)

    # Getting counting nodes
    number_of_nodes = 0
    with connection.cursor() as cursor:
        cursor.execute("""
        SELECT count(*) from cypher('%s', $$
                MATCH (V)
                RETURN V
        $$) as (V agtype);
        """ % (GRAPH_NAME))
        for row in cursor:
            number_of_nodes = row[0]

    # Getting how many nodes has id property
    number_of_nodes_with_property_id = 0
    with connection.cursor() as cursor:
        cursor.execute("""
        SELECT count(*) from cypher('%s', $$
                MATCH (V)
                WHERE exists(V.id)
                RETURN V
        $$) as (V agtype);
        """ % (GRAPH_NAME))
        for row in cursor:
            number_of_nodes_with_property_id = row[0]

    has_id_property = True
    if number_of_nodes != number_of_nodes_with_property_id:
        has_id_property = False

    directory = os.path.join(directory, (GRAPH_NAME + '_data'))
    if not os.path.exists(directory):
        os.makedirs(directory)

    for label in vertex_label:
        filename = os.path.join(directory, (label + '.csv'))
        query = """
        SELECT * from cypher('%s', 
        $$
            MATCH (V:%s)
            RETURN V
        $$) as (V agtype);
        """ % (GRAPH_NAME, label)

        data = []
        with connection.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                data.append([row[0].id, row[0].label, row[0].properties])

        property_name = set([])
        property_name.add('id')
        for d in data:
            for x in list(d[2].keys()):
                property_name.add(x)
        property_name = list(property_name)
        property_name.sort()

        df = pd.DataFrame(columns=property_name)

        for d in data:
            mp = {}
            for prop in property_name:
                if prop in d[2].keys():
                    mp[prop] = [d[2][prop]]
                else:
                    mp[prop] = ''
            if not has_id_property:
                mp['id'] = [d[0]]
            new_df = pd.DataFrame(mp)
            df = pd.concat([df, new_df], ignore_index=True)
        df.to_csv(filename, index=False)

    for label in edge_label:
        filename = os.path.join(directory, ('edge_' + label + '.csv'))
        query = """ 
        SELECT * from cypher('%s', 
        $$
            MATCH (V)-[R:%s]->(V2)
            RETURN V,R,V2
        $$) as (V agtype, R agtype, V2 agtype);
        """ % (GRAPH_NAME, label)
        data = []
        with connection.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                data.append(row)

        property_name = set([])
        for d in data:
            for x in list(d[1].properties.keys()):
                property_name.add(x)
        property_name.add('start_id')
        property_name.add('end_id')
        property_name.add('start_vertex_type')
        property_name.add('end_vertex_type')
        property_name = list(property_name)
        property_name.sort()

        df = pd.DataFrame(columns=property_name)

        for d in data:
            mp = {}
            for prop in property_name:
                if prop in d[1].properties.keys():
                    mp[prop] = [d[1].properties[prop]]
                else:
                    mp[prop] = []
            if not has_id_property:
                mp['start_id'] = [d[0].id]
                mp['end_id'] = [d[2].id]
            else:
                mp['start_id'] = [d[0].properties['id']]
                mp['end_id'] = [d[2].properties['id']]
            mp['start_vertex_type'] = [d[0].label]
            mp['end_vertex_type'] = [d[2].label]
            # print(mp)
            new_df = pd.DataFrame(mp)
            df = pd.concat([df, new_df], ignore_index=True)
        df.to_csv(filename, index=False)

    print(
        f'Created a directory\nDirectory name : {os.path.basename(directory)}\nDirectory path : {directory}')
