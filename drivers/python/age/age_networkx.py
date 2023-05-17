"""
Author : Moontasir Mahmood
Github : munmud
"""

from . import *
import psycopg2
import networkx as nx
from psycopg2 import sql
from typing import Dict, Any, List, Tuple
from age.models import Vertex, Edge, Path


def ageToNetworkx(connection: psycopg2.connect,
                  GRAPH_NAME: str,
                  G: None | nx.DiGraph = None,
                  query: str | None = None,
                  ) -> nx.DiGraph:
    """
    This Function Creates a Directed Graph from AGE Graph db. It will load all the nodes and edges it can find from the age db to networkx

    @Params
    -------

    connection - psycopg2.connect
        A connection object when retruning from psycopg2.connect

    GRAPH_NAME - string
        Name of the Graph

    query - string
        A cypher query 

    @Returns
    --------
    nx.DiGraph
        A Networkx Directed Graph
    """

    # Create an empty directed graph
    if G == None:
        G = nx.DiGraph()

    # Check if the age graph exists
    with connection.cursor() as cursor:
        qq = """
                    SELECT count(*) 
                    FROM ag_catalog.ag_graph 
                    WHERE name='%s'
                """ % (GRAPH_NAME)
        cursor.execute(sql.SQL(qq))
        if cursor.fetchone()[0] == 0:
            raise GraphNotFound(GRAPH_NAME)

    def python_dict_to_cypher_property(property):
        """
            Converting python dictionary to age cypher property string
        """
        if isinstance(property, dict):
            p = "{"
            for x, y in property.items():
                p += x + " : "
                if isinstance(y, dict):
                    p += python_dict_to_cypher_property(y)
                    p += ","
                elif isinstance(y, list):
                    p += python_dict_to_cypher_property(y)
                    p += ','
                elif isinstance(y, int):
                    p += str(y)
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
                    p += python_dict_to_cypher_property(x)
                    p += ","
                elif isinstance(x, list):
                    p += python_dict_to_cypher_property(x)
                    p += ','
                else:
                    p += "'"
                    p += str(x)
                    p += "',"
            p = p.removesuffix(',')
            p += "]"
        return p

    def addVertex(node):
        G.add_node(node.properties['id'],
                   label=node.label,
                   properties=node.properties)

    def addEdge(edge):
        G.add_edge(edge.properties['start_id'],
                   edge.properties['end_id'],
                   label=edge.label,
                   properties=edge.properties)

    def addPath(path):
        for x in path:
            if (type(x) == Path):
                addPath(x)
        for x in path:
            if (type(x) == Vertex):
                addVertex(x)
        for x in path:
            if (type(x) == Edge):
                addEdge(x)

    # Setting up connection to work with Graph
    age.setUpAge(connection, GRAPH_NAME)

    if (query == None):
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                SELECT * from cypher('%s', $$
                    MATCH (n)   
                    RETURN n
                $$) as (n agtype);
                """ % (GRAPH_NAME))
                for row in cursor:
                    addVertex(row[0])
            with connection.cursor() as cursor:
                cursor.execute("""
                SELECT * from cypher('%s', $$
                    MATCH ()-[R]->()
                    RETURN R
                $$) as (R agtype);
                """ % (GRAPH_NAME))
                for row in cursor:
                    addEdge(row[0])

        except Exception as ex:
            print(type(ex), ex)
    else:
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                for row in cursor:
                    if type(row[0]) == Path:
                        addPath(row[0])
                    if type(row[0]) == Vertex:
                        addVertex(row[0])
                    if type(row[0]) == Edge:
                        addEdge(row[0])
                        print(row[0])

        except Exception as ex:
            print(type(ex), ex)
    return G


def networkxToAge(connection: psycopg2.connect,
                  G: nx.DiGraph,
                  GRAPH_NAME: str) -> None:
    """
    This Function add all the nodes and edges found from a Networkx Directed Graph to Apache AGE Graph Database

    @Params
    -------

    connection - psycopg2.connect
        A connection object when retruning from psycopg2.connect

    G - nx.DiGraph
        A Networkx Directed Graph

    GRAPH_NAME - string
        Name of the Graph

    @Returns
    --------
    None
    """

    # Check if every nodes and edges has label
    for x in G.nodes:
        if 'label' not in G.nodes[x]:
            raise Exception('Node : ', x, ' has no label')
    for x in G.edges:
        if 'label' not in G.edges[x]:
            raise Exception('Edge : ', x, ' has no label')

    # Setting up connection to work with Graph
    age.setUpAge(connection, GRAPH_NAME)

    def python_dict_to_cypher_property(property):
        """
            Converting python dictionary to age cypher property string
        """
        if isinstance(property, dict):
            p = "{"
            for x, y in property.items():
                p += x + " : "
                if isinstance(y, dict):
                    p += python_dict_to_cypher_property(y)
                    p += ","
                elif isinstance(y, list):
                    p += python_dict_to_cypher_property(y)
                    p += ','
                elif isinstance(y, int):
                    p += str(y)
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
                    p += python_dict_to_cypher_property(x)
                    p += ","
                elif isinstance(x, list):
                    p += python_dict_to_cypher_property(x)
                    p += ','
                else:
                    p += "'"
                    p += str(x)
                    p += "',"
            p = p.removesuffix(',')
            p += "]"
        return p

    def addvertex(CREATE_str):
        query = """
            SELECT * from cypher('%s',
                        $$
                            CREATE %s
                        $$) as (a agtype);
            """ % (GRAPH_NAME, CREATE_str)
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                connection.commit()
        except Exception as ex:
            print(type(ex), ex)
            connection.rollback()

    def addEdges(query):
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                connection.commit()
        except Exception as ex:
            print('Exception : ', type(ex), ex)
            connection.rollback()

    def get_edge_query(
            start_id: str | int,
            end_id: str | int,
            edge_label: str,
            edge_properties: Dict[str, Any]) -> None:
        """Get cypher query for vertices"""

        query = """
            SELECT * from cypher('%s', 
            $$
                MATCH (a %s), (b %s)
                CREATE (a)-[e:%s %s]->(b)
            $$) as (e agtype);
        """ % (GRAPH_NAME,
                python_dict_to_cypher_property({'id': start_id}),
                python_dict_to_cypher_property({'id': end_id}),
                edge_label,
                python_dict_to_cypher_property(edge_properties))
        return query

    CREATE_str = ''
    for i, node in enumerate(G.nodes):
        if (len(CREATE_str) != 0):
            CREATE_str += ','
        CREATE_str += '(v%s:%s %s)' % (node, G.nodes[node]['label'],
                                       python_dict_to_cypher_property(G.nodes[node]['properties']))
        if (((i % 500) == 499) or (i == (len(G.nodes)-1))):
            addvertex(CREATE_str)
            print(i+1, ' nodes created')
            CREATE_str = ''

    # Adding pg_index
    label_set = set()
    for node in G.nodes:
        label_set.add(G.nodes[node]['label'])
    for label in label_set:
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                CREATE INDEX %s
                ON %s."%s" USING gin (properties);
                """ % ('temp_' + label, GRAPH_NAME, label))
                connection.commit()
        except Exception as ex:
            print(type(ex), ex)

    query = ''
    for i, e in enumerate(G.edges):
        query += get_edge_query(e[0], e[1], G.edges[e[0], e[1]]
                                ['label'], G.edges[e[0], e[1]]['properties'])
        if ((i % 1000 == 999) or (i == len(G.edges)-1)):
            addEdges(query)
            query = ''
            print('ADDED :', i+1, ' Edges')

    # Deleting pg_index
    for label in label_set:
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                DROP INDEX %s.%s;
                """ % (GRAPH_NAME, 'temp_' + label))
                connection.commit()
        except Exception as ex:
            print(type(ex), ex)
