"""
Author : Moontasir Mahmood
Github : munmud
"""

from age import *
import psycopg2
import networkx as nx
from psycopg2 import sql
from typing import Dict, Any
from age.models import Vertex, Edge, Path


def ageToNetworkx(connection: psycopg2.connect,
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
        WHERE NOT exists(n.id)
        SET n.id = id(n)
        $$) as (V agtype);
        """ % (graphName))

    # Add start_id if not exist
    with connection.cursor() as cursor:
        cursor.execute("""
        SELECT * from cypher('%s', $$
        MATCH (a)-[n]->()
        WHERE NOT exists(n.start_id)
        SET n.start_id = id(a)
        $$) as (V agtype);
        """ % (graphName))

    # Add end_id if not exist
    with connection.cursor() as cursor:
        cursor.execute("""
        SELECT * from cypher('%s', $$
        MATCH ()-[n]->(b)
        WHERE NOT exists(n.end_id)
        SET n.end_id = id(b)
        $$) as (V agtype);
        """ % (graphName))

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
        G.add_node(node.properties['id'],
                   label=node.label,
                   properties=node.properties)

    def addEdgeToNetworkx(edge):
        """Add Edge in Networkx"""
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


def networkxToAge(connection: psycopg2.connect,
                  G: nx.DiGraph,
                  graphName: str,
                  nodeBatchCount: int = 500,
                  edgeBatchCount: int = 1000,
                  isPrint: bool = True
                  ) -> None:
    """
    This Function add all the nodes and edges found from a Networkx Directed Graph to Apache AGE Graph Database

    @Params
    -------

    connection - psycopg2.connect
        A connection object when retruning from psycopg2.connect

    G - nx.DiGraph
        A Networkx Directed Graph

    graphName - string
        Name of the Graph

    nodeBatchCount - int
        max number of nodes in a batch to be created

    edgeBatchCount - int
        max number of edges in a batch to be created

    isPrint - bool
        Printing the progress after certain steps

    @Returns
    --------
    None
    """

    # Check if every nodes and edges has label
    for node in G.nodes:
        if 'label' not in G.nodes[node]:
            raise Exception('Node : ', node, ' has no label')
    for edge in G.edges:
        if 'label' not in G.edges[edge]:
            raise Exception('Edge : ', edge, ' has no label')

    # Create id in node properties if not exist
    for node in G.nodes:
        if 'properties' not in G.nodes[node]:
            G.nodes[node]['properties'] = {}
        if 'id' not in G.nodes[node]['properties']:
            G.nodes[node]['properties']['id'] = node

    # Create start_id, end_id in edge property if not exist
    for edge in G.edges:
        if 'properties' not in G.edges[edge]:
            G.edges[edge]['properties'] = {}
        if 'start_id' not in G.edges[edge]['properties']:
            G.edges[edge]['properties']['start_id'] = edge[0]
        if 'end_id' not in G.edges[edge]['properties']:
            G.edges[edge]['properties']['end_id'] = edge[1]

    # Setting up connection to work with Graph
    age.setUpAge(connection, graphName)

    # Progress Parameters
    global total_iterations
    global current_iteration
    total_iterations = len(G.nodes) + len(G.edges)
    current_iteration = 0

    def print_progress():
        """Print Progress"""
        if (isPrint == False):
            return
        global total_iterations
        global current_iteration
        progress_width = 40  # Width of the progress bar

        # Calculate progress percentage
        progress = current_iteration / total_iterations
        percent_done = int(progress * 100)

        # Calculate the number of characters to display in the progress bar
        bar_width = int(progress * progress_width)
        bar = '#' * bar_width + '-' * (progress_width - bar_width)

        # Print the progress bar and percentage
        print(f'[{bar}] {percent_done}% complete', end='\r')

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

    def addVertexToAge(CREATE_str):
        """Add a batch of nodes to AGE"""
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                SELECT * from cypher('%s',
                            $$
                                CREATE %s
                            $$) as (a agtype);
                """ % (graphName, CREATE_str))
                connection.commit()
        except Exception as ex:
            print(type(ex), ex)
            connection.rollback()
        print_progress()

    def addEdgesToAge(query):
        """Add a batch of edges to AGE"""
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                connection.commit()
        except Exception as ex:
            print('Exception : ', type(ex), ex)
            connection.rollback()

        print_progress()

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
        """ % (graphName,
                python_dict_to_cypher_property({'id': start_id}),
                python_dict_to_cypher_property({'id': end_id}),
                edge_label,
                python_dict_to_cypher_property(edge_properties))
        return query

    # Creating nodes in Batches
    CREATE_str = ''
    for i, node in enumerate(G.nodes):
        current_iteration += 1
        if (len(CREATE_str) != 0):
            CREATE_str += ','
        CREATE_str += '(v%s:%s %s)' % (node, G.nodes[node]['label'],
                                       python_dict_to_cypher_property(G.nodes[node]['properties']))
        if (((i % nodeBatchCount) == (nodeBatchCount-1)) or (i == (len(G.nodes)-1))):
            addVertexToAge(CREATE_str)
            CREATE_str = ''

    # Adding pg_index of all label
    label_set = set()
    for node in G.nodes:
        label_set.add(G.nodes[node]['label'])
    for label in label_set:
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                CREATE INDEX %s
                ON %s."%s" USING gin (properties);
                """ % ('temp_' + label, graphName, label))
                connection.commit()
        except Exception as ex:
            print(type(ex), ex)

    # Creating edges in Batches
    query_list = ''
    for i, e in enumerate(G.edges):
        current_iteration += 1
        query_list += get_edge_query(e[0], e[1], G.edges[e[0], e[1]]
                                     ['label'], G.edges[e[0], e[1]]['properties'])
        if ((i % edgeBatchCount == (edgeBatchCount-1)) or (i == len(G.edges)-1)):
            addEdgesToAge(query_list)
            query_list = ''

    # Deleting pg_index
    for label in label_set:
        try:
            with connection.cursor() as cursor:
                cursor.execute("""DROP INDEX %s.%s;""" %
                               (graphName, 'temp_' + label))
                connection.commit()
        except Exception as ex:
            print(type(ex), ex)
    if (isPrint):
        print('')
