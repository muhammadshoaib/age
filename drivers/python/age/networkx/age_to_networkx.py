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
from psycopg2 import sql
from typing import Dict, Any
from age.models import Vertex, Edge, Path


def age_to_networkx(connection: psycopg2.connect,
                    graphName: str,
                    G: None | nx.DiGraph = None,
                    query: str | None = None,
                    isPrint: bool = True
                    ) -> nx.DiGraph:
    """
    This Function Creates a Directed Graph from AGE Graph db. It will load all the nodes and edges it can find from the age db to networkx

    @Params
    -------

    connection - psycopg2.connect
        A connection object when retruning from psycopg2.connect

    graphName - string
        Name of the Graph

    G - Networkx directed Graph
        Previous Networkx Graph

    query - string
        A cypher query

    isPrint - bool
        Printing the progress after certain steps

    @Returns
    --------
    nx.DiGraph
        A Networkx Directed Graph
    """
    # Check if the age graph exists
    with connection.cursor() as cursor:
        cursor.execute(sql.SQL("""
                    SELECT count(*) 
                    FROM ag_catalog.ag_graph 
                    WHERE name='%s'
                """ % (graphName)))
        if cursor.fetchone()[0] == 0:
            raise GraphNotFound(graphName)

    # Create an empty directed graph
    if G == None:
        G = nx.DiGraph()

    # Add id in node where if not exist
    with connection.cursor() as cursor:
        cursor.execute("""
        SELECT * from cypher('%s', $$
        MATCH (n)
        WHERE NOT exists(n.__id__)
        SET n.__id__ = id(n)
        $$) as (V agtype);
        """ % (graphName))
        connection.commit()

    # Add __start_id__ if not exist
    with connection.cursor() as cursor:
        cursor.execute("""
        SELECT * from cypher('%s', $$
        MATCH (a)-[n]->()
        WHERE NOT exists(n.__start_id__)
        SET n.__start_id__ = a.__id__
        $$) as (V agtype);
        """ % (graphName))
        connection.commit()

    # Add __end_id__ if not exist
    with connection.cursor() as cursor:
        cursor.execute("""
        SELECT * from cypher('%s', $$
        MATCH ()-[n]->(b)
        WHERE NOT exists(n.__end_id__)
        SET n.__end_id__ = b.__id__
        $$) as (V agtype);
        """ % (graphName))
        connection.commit()

    def python_dict_to_cypher_property(property):
        """Converting python dictionary to age cypher property string"""
        if isinstance(property, dict):
            properties_str = "{"
            for key, value in property.items():
                properties_str += key + " : "
                if isinstance(value, dict):
                    properties_str += python_dict_to_cypher_property(value)
                    properties_str += ","
                elif isinstance(value, list):
                    properties_str += python_dict_to_cypher_property(value)
                    properties_str += ','
                elif isinstance(value, int):
                    properties_str += str(value)
                    properties_str += ','
                else:
                    properties_str += "'"
                    properties_str += str(value)
                    properties_str += "',"
            properties_str = properties_str.removesuffix(',')
            properties_str += "}"
        elif isinstance(property, list):
            properties_str = "["
            for value in property:
                if isinstance(value, dict):
                    properties_str += python_dict_to_cypher_property(value)
                    properties_str += ","
                elif isinstance(value, list):
                    properties_str += python_dict_to_cypher_property(value)
                    properties_str += ','
                else:
                    properties_str += "'"
                    properties_str += str(value)
                    properties_str += "',"
            properties_str = properties_str.removesuffix(',')
            properties_str += "]"
        return properties_str

    def addNodeToNetworkx(node):
        """Add Nodes in Networkx"""
        G.add_node(node.properties['__id__'],
                   label=node.label,
                   properties=node.properties)

    def addEdgeToNetworkx(edge):
        """Add Edge in Networkx"""
        G.add_edge(edge.properties['__start_id__'],
                   edge.properties['__end_id__'],
                   label=edge.label,
                   properties=edge.properties)

    def addPath(path):
        for x in path:
            if (type(x) == Path):
                addPath(x)
        for x in path:
            if (type(x) == Vertex):
                addNodeToNetworkx(x)
        for x in path:
            if (type(x) == Edge):
                addEdgeToNetworkx(x)

    # Setting up connection to work with Graph
    age.setUpAge(connection, graphName)

    initial_node_cnt = len(G.nodes)
    initial_edge_cnt = len(G.edges)

    # if no query given add all nodes and edges otherwise only execute query
    if (query == None):
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                SELECT * from cypher('%s', $$
                    MATCH (n)
                    RETURN n
                $$) as (n agtype);
                """ % (graphName))
                for i, row in enumerate(cursor):
                    addNodeToNetworkx(row[0])

                    if ((isPrint) and (i % 100 == 0)):
                        print(
                            f'Found {len(G.nodes)-initial_node_cnt} nodes and {len(G.edges)-initial_edge_cnt} edges', end='\r')

            with connection.cursor() as cursor:
                cursor.execute("""
                SELECT * from cypher('%s', $$
                    MATCH ()-[R]->()
                    RETURN R
                $$) as (R agtype);
                """ % (graphName))
                for i, row in enumerate(cursor):
                    addEdgeToNetworkx(row[0])
                    if ((isPrint) and (i % 100 == 0)):
                        print(
                            f'Found {len(G.nodes)-initial_node_cnt} nodes and {len(G.edges)-initial_edge_cnt} edges', end='\r')

        except Exception as ex:
            print(type(ex), ex)
    else:
        try:
            with connection.cursor() as cursor:
                if (isPrint):
                    print(f'Executing query...', end='\r')
                cursor.execute(query)
                for i, row in enumerate(cursor):
                    for content in row:
                        if type(content) == Path:
                            addPath(content)
                        elif type(content) == Vertex:
                            addNodeToNetworkx(content)
                        elif type(content) == Edge:
                            addEdgeToNetworkx(content)
                        if ((isPrint) and (i % 100 == 0)):
                            print(
                                f'Found {len(G.nodes)-initial_node_cnt} nodes and {len(G.edges)-initial_edge_cnt} edges', end='\r')

        except Exception as ex:
            print(type(ex), ex)

    if (isPrint):
        print(
            f'Found {len(G.nodes)-initial_node_cnt} nodes and {len(G.edges)-initial_edge_cnt} edges', end='\r')
        print('')

    return G
