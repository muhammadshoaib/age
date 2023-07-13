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

from age import *
import psycopg2
import networkx as nx
import json


def networkx_to_age(connection: psycopg2.connect,
                    G: nx.DiGraph,
                    graphName: str):
    node_label_list = set()
    edge_label_list = set()

    """
        - Add default label if label is missing 
        - Extract all distinct node label into node_label_list
        - Extract all distinct edge label into edge_label_list
        - Add properties if not exist 
    """
    try:
        for node, data in G.nodes(data=True):
            if 'label' not in data:
                data['label'] = '_ag_label_vertex'
            if 'properties' not in data:
                data['properties'] = {}
            if not isinstance(data['label'], str):
                raise Exception(f"label of node : {node} must be a string")
            if not isinstance(data['properties'], dict):
                raise Exception(f"properties of node : {node} must be a dict")
            data['properties']['__id__'] = node

            node_label_list.add(data['label'])
        for u, v, data in G.edges(data=True):
            if 'label' not in data:
                data['label'] = '_ag_label_edge'
            if 'properties' not in data:
                data['properties'] = {}
            if not isinstance(data['label'], str):
                raise Exception(f"label of node : {node} must be a string")
            if not isinstance(data['properties'], dict):
                raise Exception(f"properties of node : {node} must be a dict")
            edge_label_list.add(data['label'])
    except Exception as e:
        raise Exception(e)

    # Setup connection with Graph
    age.setUpAge(connection, graphName)

    """
        - create_vlabel for all nodelabel
        - create_elabel for all edgelabel
    """
    try:
        node_label_set = set()
        edge_label_set = set()
        with connection.cursor() as cursor:
            cursor.execute("""
            SELECT name, kind
            FROM ag_catalog.ag_label;
            """)
            for row in cursor:
                if (row[1] == 'v'):
                    node_label_set.add(row[0])
                else:
                    edge_label_set.add(row[0])

        crete_label_statement = ''
        for label in node_label_list:
            if label in node_label_set:
                continue
            crete_label_statement += """SELECT create_vlabel('%s','%s');\n""" % (
                graphName, label)
        if crete_label_statement != '':
            with connection.cursor() as cursor:
                cursor.execute(crete_label_statement)
                connection.commit()

        crete_label_statement = ''
        for label in edge_label_list:
            if label in edge_label_set:
                continue
            crete_label_statement += """SELECT create_elabel('%s','%s');\n""" % (
                graphName, label)
        if crete_label_statement != '':
            with connection.cursor() as cursor:
                cursor.execute(crete_label_statement)
                connection.commit()
    except Exception as e:
        raise Exception(e)

    """Add all node to AGE"""
    try:
        to_be_added = {label: [] for label in node_label_list}
        global added_id
        added_id = {label: [] for label in node_label_list}
        cc = 0

        def addVertex():
            global added_id
            for label, rows in to_be_added.items():
                if (len(rows) == 0):
                    continue
                table_name = """%s."%s" """ % (graphName, label)
                insert_query = f"INSERT INTO {table_name} (properties) VALUES "

                for value in rows:
                    insert_query += f"('{value}'),"
                insert_query = insert_query.removesuffix(',')
                insert_query += ' RETURNING id'

                with connection.cursor() as cursor:
                    cursor.execute(insert_query)
                    connection.commit()
                    for row in cursor:
                        added_id[label].append(row[0])
                rows.clear()

        for node, data in G.nodes(data=True):
            json_string = json.dumps(data['properties'])
            to_be_added[data['label']].append(json_string)
            cc += 1
            if (cc == 1000):
                addVertex()
                cc = 0
        if (cc > 0):
            addVertex()
            cc = 0

        for label, row in added_id.items():
            row.reverse()
        for node, data in G.nodes(data=True):
            data['__gid__'] = added_id[data['label']][-1]
            added_id[data['label']].pop()
    except Exception as e:
        raise Exception(e)

    """Add all edge to AGE"""
    try:
        to_be_added = {label: [] for label in edge_label_list}
        cc = 0

        def addEdge():
            for label, rows in to_be_added.items():
                if (len(rows) == 0):
                    continue
                table_name = """%s."%s" """ % (graphName, label)
                insert_query = f"INSERT INTO {table_name} (start_id,end_id,properties) VALUES "
                for row in rows:
                    insert_query += f"('{row[0]}','{row[1]}','{row[2]}'),"
                insert_query = insert_query.removesuffix(',')
                with connection.cursor() as cursor:
                    cursor.execute(insert_query)
                    connection.commit()
                rows.clear()

        for u, v, data in G.edges(data=True):
            json_string = json.dumps(data['properties'])
            to_be_added[data['label']].append(
                (G.nodes[u]['__gid__'], G.nodes[v]['__gid__'], json_string))
            cc += 1
            if (cc == 1000):
                addEdge()
                cc = 0
        if (cc > 0):
            addEdge()
            cc = 0
    except Exception as e:
        raise Exception(e)

    """delete added __gid__"""
    try:
        for u, data in G.nodes(data=True):
            del data['__gid__']
    except Exception as e:
        raise Exception(e)