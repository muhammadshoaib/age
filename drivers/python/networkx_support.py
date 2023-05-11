"""
Author: Syed Safi Ullah Shah
GitHubID: safi50 
DiscordID: safi50
"""
import psycopg2 
from psycopg2 import extensions as ext
from psycopg2 import sql
import networkx as nx
import age
from age.models import Vertex, Edge, Path
from age.exceptions import *



def cypher_to_networkx(conn: ext.connection, graph_name: str,  query: str) -> nx.DiGraph:
    """
    Execute a Cypher query and load the result into a NetworkX DiGraph object.

    Parameters:
    - conn (ext.connection): A connection object to the AGE database. This should be an open
                             connection, and it will not be closed by the function.
    - query (str): A Cypher query to execute on the AGE database. The query should return a
                   collection of Vertex, Edge, or Path elements that can be loaded into a NetworkX graph.

    Returns:
    - nx.DiGraph

    Note: The function will commit the transaction on the provided connection before returning,
          so any changes made in the same transaction outside this function will also be committed.

    Note: If the result of the Cypher query cannot be loaded into a NetworkX graph (for example, if
          the query returns scalar values or non-graph elements), the function will simply print the
        output and return an empty graph.

    """
    try: 
        # Initializing an empty NetworkX Directional graph
        G = nx.DiGraph() 
        # Setting up Age Connection
        age.setUpAge(conn, graph_name)


        # executing the cypher query
        with conn.cursor() as cursor:
            cursor.execute(query)

            for row in cursor:
                if not isinstance(row[0], (Vertex, Edge, Path)):
                    print("Output:", row)
                    return G
                
                # Checking if the output is a PATH and extracting the vertices and edges from it
                ag_element = row[0] if row[0].gtype == age.TP_PATH else row
                
                # Loading the nodes and edges into the NetworkX graph
                for item in ag_element:
                    if item.gtype == age.TP_VERTEX:
                        G.add_node(item.id,
                                    label=item.label,
                                    properties=item.properties)
                    elif item.gtype == age.TP_EDGE:
                        G.add_edge(item.start_id,
                                    item.end_id,
                                    label=item.label,
                                    properties=item.properties)
                        
        conn.commit()
        return G
    
    except Exception as e:
        print("Exception:",e)
        return G


def graph_to_networkx(conn: ext.connection, graph_name: str) -> nx.DiGraph:
    """
    Load an AGE graph into a NetworkX DiGraph object.

    Parameters:
    - conn (ext.connection): A connection object to the AGE database. This should be an open
                             connection, and it will not be closed by the function.
    - graph_name (str): The name of the AGE graph in the database

    Returns:
    - nx.DiGraph: A NetworkX DiGraph object representing the AGE graph. Each node in the DiGraph
                  has an 'id', 'label', and 'properties' attribute corresponding to the properties
                  of the AGE node. Each edge has a 'label' and 'properties' attribute corresponding
                  to the properties of the AGE edge.

    Note: The function will commit the transaction on the provided connection before returning,
          so any changes made in the same transaction outside this function will also be committed.

    Note: If an exception occurs while loading the graph, the function will print the exception
          message and return the partially loaded graph. If the graph_name does not exist, the
          returned graph will be empty.
        """

    try:
        # Initializing an empty NetworkX Directional graph
        G = nx.DiGraph() 
        # Setting up Age Connection
        age.setUpAge(conn, graph_name)


        # Check if the age graph exists
        with conn.cursor() as cursor:
       
            # Get all vertices from the age graph
            cursor.execute("""SELECT * FROM cypher(%s, $$ MATCH (n) RETURN (n) $$) as (v agtype);""", (graph_name,))
            for row in cursor:
                # Loading the nodes into the NetworkX graph
                G.add_node(row[0].id , label=row[0].label, properties=row[0].properties)

            # Get all edges from the age graph
            cursor.execute("""SELECT * FROM cypher(%s, $$ MATCH ()-[r]-() RETURN (r) $$) as (e agtype);""", (graph_name,))
            for row in cursor:
                # Loading the edges into the NetworkX graph
                G.add_edge(row[0].start_id, row[0].end_id, label=row[0].label, properties=row[0].properties)

        conn.commit()
        return G
    
    except Exception as e:
        print("Exception:",e)
        return G


def dict_to_props(d):
    """
    Convert a Python dictionary to a string representation of AGE properties.
    """

    items = [f"{k}:{repr(v)}" for k, v in d.items()]
    return "{" + ", ".join(items) + "}"


def networkx_to_age(conn: ext.connection, nx_graph: nx.DiGraph, age_graph: str):

    try:
        """
        Checks connection with AGE and if not, sets it up.
        Also checks if graph exists and if not, creates it
        """
        age.setUpAge(conn, age_graph)

        with conn.cursor() as cursor:

            # Adding vertices to the AGE graph
            vertices = []
            for node, props in nx_graph.nodes(data=True):
                props["label"] = node
                props = dict_to_props(props)
                
                """
                Checking if the node label is a string or not.
                Since only str values are allowed to be labels in AGE, whereas, node labels can be non-str
                values in Networkx, we have this check.
                If label name is not a string, we simply load the vertex label as a property. 
                
                """
                if (isinstance(node, str)):
                    vertices.append(f"(n:{node} {props})")
                else:
                    vertices.append(f"(:vertex {props})")
            
            #Creating a single cypher query to load all the vertices to AGE graph instead of multiple queries
            cypher_query = ", ".join(vertices)

            #Check to avoid empty cypher query
            if cypher_query:
                cursor.execute(f"SELECT * FROM cypher('{age_graph}', $$ CREATE {cypher_query} $$) as (v agtype);")
                for row in cursor:
                    print(row)

            # Add edges
            for start, end, props in nx_graph.edges(data=True):

                #Converting networkX properties to loadable AGE properties
                props = dict_to_props(props)

                #Since Edges need to match the vertices, we need a separate cypher query for each edge.
                cursor.execute(f"SELECT * FROM cypher('{age_graph}', $$  MATCH (a),(b) WHERE a.label= {start} AND b.label = {end}  CREATE (a)-[e:edge {props}]->(b) return e $$) as (e agtype);")
                for row in cursor:
                    print(row)

            # Committing the changes
            conn.commit()
            print("-> Loaded NetworkX {} into AGE graph '{}'".format(nx_graph, age_graph))


    except Exception as e:
        print(e)
        conn.rollback()
