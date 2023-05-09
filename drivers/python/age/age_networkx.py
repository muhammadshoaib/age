"""
Author : Moontasir Mahmood
Github : munmud
"""

from . import *
import psycopg2
import networkx as nx
from psycopg2 import sql
from typing import Dict, Any, List, Tuple


def ageToNetworkx(connection: psycopg2.connect,
                  GRAPH_NAME: str,
                  G: None | nx.DiGraph = None,
                  node_query: str | None = None,
                  node_filters: List[Tuple[str, Dict]] | None = None,
                  edge_query: str | None = None,
                  edge_filters: List[Tuple[str, Dict]] | None = None,
                  add_new_node_from_edge_filters: bool = False,
                  ) -> nx.DiGraph:
    """
    This Function Creates a Directed Graph from AGE Graph db. It will load all the nodes and edges it can find from the age db to networkx

    @Params
    -------

    connection - psycopg2.connect
        A connection object when retruning from psycopg2.connect

    GRAPH_NAME - string
        Name of the Graph

    node_query - string
        A cypher query for filterig node

    node_filters - List[Tuple[str, Dict]] | None
        A list of Tuple for filters to be applied when adding nodes

    edge_query - string
        A cypher query for filterig edge

    edge_filters - List[Tuple[str, Dict]] | None
        A list of Tuple for filters to be applied when adding edges

    add_new_node_from_edge_filters - Boolean
        True : Add all the nodes found from edge filters
        False : Add only edges to only previouly added nodes

    @Returns
    --------
    nx.DiGraph
        A Networkx Directed Graph
    """

    # Check if the age graph exists
    with connection.cursor() as cursor:
        query = """
                    SELECT count(*) 
                    FROM ag_catalog.ag_graph 
                    WHERE name='%s'
                """ % (GRAPH_NAME)
        cursor.execute(sql.SQL(query))
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

    def is_maintained_filter_structure(filters):
        """
            checking if the proper structure followed for given filtering
        """
        if filters != None:
            if not isinstance(filters, list):
                raise Exception('filters is not a List')
            for item in filters:
                if not isinstance(item, tuple) or len(item) != 2:
                    raise Exception(
                        'filters elements are not a Tuple of length 2 for element :', item)
                if not isinstance(item[0], str) or not isinstance(item[1], dict):
                    raise Exception(
                        'filters elements have no label or property for element:', item)

    def addNodesToNetworkx(query):
        with connection.cursor() as cursor:
            try:
                cursor.execute(query)
                for row in cursor:
                    G.add_node(row[0].id,
                               label=row[0].label,
                               properties=row[0].properties)
            except Exception as ex:
                print(type(ex), ex)

    def addEdgesToNetworkx(query):
        with connection.cursor() as cursor:
            try:
                cursor.execute(query)
                for row in cursor:
                    if (add_new_node_from_edge_filters):
                        G.add_node(row[0][0].id,
                                   label=row[0][0].label,
                                   properties=row[0][0].properties)
                        G.add_node(row[0][2].id,
                                   label=row[0][2].label,
                                   properties=row[0][2].properties)

                    if G.has_node(row[0][1].start_id) and G.has_node(row[0][1].end_id):
                        G.add_edge(row[0][1].start_id,
                                   row[0][1].end_id,
                                   label=row[0][1].label,
                                   properties=row[0][1].properties)
            except Exception as ex:
                print(type(ex), ex)

    is_maintained_filter_structure(node_filters)
    is_maintained_filter_structure(edge_filters)

    # Setting up connection to work with Graph
    age.setUpAge(connection, GRAPH_NAME)

    # Create an empty directed graph
    if G == None:
        G = nx.DiGraph()

    if node_query != None:
        query = """
        SELECT * FROM cypher('%s', 
        $$ %s $$) as (v agtype);
        """ % (GRAPH_NAME, node_query)
        addNodesToNetworkx(query)

    if node_filters != None:
        for label, prop in node_filters:
            if label != '':
                query = """
                            SELECT * FROM cypher('%s', 
                            $$ 
                                MATCH (n:%s %s) 
                                RETURN (n) 
                            $$) as (v agtype);
                        """ % (GRAPH_NAME, label, python_dict_to_cypher_property(prop))
            else:
                query = """
                        SELECT * FROM cypher('%s', 
                        $$ 
                            MATCH (n %s) 
                            RETURN (n) 
                        $$) as (v agtype);
                    """ % (GRAPH_NAME, python_dict_to_cypher_property(prop))
            addNodesToNetworkx(query)

    if edge_query != None:
        query = """
            SELECT * from cypher(
                '%s', $$ %s$$
            ) as (v agtype); 
        """ % (GRAPH_NAME, edge_query)
        addEdgesToNetworkx(query)

    if edge_filters != None:
        with connection.cursor() as cursor:
            for label, prop in edge_filters:
                if label != '':
                    query = """
                            SELECT * from cypher(
                                '%s', 
                                $$ 
                                    MATCH v=(N)-[R:%s %s]-(N2)
                                    RETURN v
                                $$
                            ) as (v agtype); 
                            """ % (GRAPH_NAME, label, python_dict_to_cypher_property(prop))
                else:
                    query = """
                            SELECT * from cypher(
                                '%s', 
                                $$ 
                                    MATCH v=(N)-[R %s]-(N2)
                                    RETURN v
                                $$
                            ) as (v agtype); 
                            """ % (GRAPH_NAME, python_dict_to_cypher_property(prop))
                addEdgesToNetworkx(query)
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

    mapId = {}  # Used to map the user id with Graph id

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

    def set_vertices(id: int | str, label: str, properties: Dict[str, Any]) -> None:
        """Add a vertices to the graph"""
        with connection.cursor() as cursor:
            query = """
            SELECT * from cypher(
                '%s', 
                $$ 
                    CREATE (v:%s %s) 
                    RETURN v
                $$
            ) as (v agtype); 
            """ % (GRAPH_NAME, label, python_dict_to_cypher_property(properties))
            # print(query)
            try:
                cursor.execute(query)
                for row in cursor:
                    mapId[id] = row[0].id

                # When data inserted or updated, You must commit.
                connection.commit()
            except Exception as ex:
                print(type(ex), ex)
                # if exception occurs, you must rollback all transaction.
                connection.rollback()

    def set_edge(id1: int | str, id2: int | str, edge_label: str, edge_properties: Dict[str, Any]) -> None:
        """Add edge to the graph"""
        with connection.cursor() as cursor:
            query = """
            SELECT * from cypher(
                '%s', 
                $$ 
                    MATCH (a), (b)
                    WHERE id(a) = %s AND id(b) = %s
                    CREATE (a)-[r:%s %s]->(b)
                    RETURN r
                $$) as (v agtype);
            """ % (
                GRAPH_NAME,
                mapId[id1],
                mapId[id2],
                edge_label,
                python_dict_to_cypher_property(edge_properties)
            )
            try:
                cursor.execute(query)
                connection.commit()
                for row in cursor:
                    # print(row[0].id)
                    pass
            except Exception as ex:
                print('Exception : ', type(ex), ex)
                # if exception occurs, you must rollback all transaction.
                connection.rollback()

    for node in G.nodes:
        set_vertices(id=node,
                     label=G.nodes[node]['label'],
                     properties=G.nodes[node]['properties'])

    for u, v in G.edges:
        set_edge(id1=u,
                 id2=v,
                 edge_label=G.edges[u, v]['label'],
                 edge_properties=G.edges[u, v]['properties'])

    # print("Successfully Added nodes with id mapped as below\n" , mapId)
