"""
Author : Aditya Gupta
Github : im-aditya30
"""

import age
from age.exceptions import *
import psycopg2
import networkx as nx
import matplotlib.pyplot as plt
from argparse import Namespace 


def load_age_graph_to_networkx(
        host: str, 
        port: str, 
        dbname: str, 
        user: str, 
        password: str, 
        GRAPH_NAME: str
    ) -> nx.DiGraph:
    """
    This function creates a NetworkX graph for the corresponding AGE graph
    @Params
    -------
    host - string
        Host IP address where database is running
        
    port - string
        Port where database is running
    
    dbname - string
        Database Name
        
    user - string
        Username to connect to database
    
    password: 
        Password to connect to the database
    
    GRAPH_NAME - string
        Name of the Graph
        
    @Returns
    --------
    nx.DiGraph
        A Networkx Directed Graph
    """
    try:
        # Create a NetworkX graph
        nx_graph = nx.DiGraph() 
        
        # Connect with the age graph
        DSN = "host={} port={} dbname={} user={} password={}".format(host, port, dbname, user, password)

        ag = age.connect(graph=GRAPH_NAME, dsn=DSN)
        
        with ag.connection.cursor() as cursor:
            # Add nodes to the NetworkX graph
            
            ag.cypher(cursor, "MATCH (n) RETURN (n)", cols=["v"])
            for row in cursor:
                nx_graph.add_node(row[0].id , label=row[0].label, properties=row[0].properties)

            # Add edges to the NetworkX graph
            ag.cypher(cursor, "MATCH ()-[r]-() RETURN (r)", cols=["e"])
            for row in cursor:
                nx_graph.add_edge(row[0].start_id, row[0].end_id, id=row[0].id, label=row[0].label, properties=row[0].properties)

        # commit to save changes
        ag.commit()
        # connection close
        ag.close()


        return nx_graph

    except Exception as e:
        print(e)
        ag.rollback()
        

def load_graph_from_networkx_to_age(
        host: str, 
        port: str, 
        dbname: str, 
        user: str, 
        password: str, 
        nx_graph:nx.DiGraph, 
        GRAPH_NAME:str
    ) -> None:
    """
    This function creates a NetworkX graph for the corresponding AGE graph
    @Params
    -------
    host - string
        Host IP address where database is running
        
    port - string
        Port where database is running
    
    dbname - string
        Database Name
        
    user - string
        Username to connect to database
    
    password: 
        Password to connect to the database
    
    nx_graph - nx.DiGraph
        NetworkX graph from where to load AGE graph
        
    GRAPH_NAME - string
        Name of the Graph
        
    @Returns
    --------
    None
    """
    try:
        # Connect to the age graph or create a age graph to load graph
        DSN = "host={} port={} dbname={} user={} password={}".format(host, port, dbname, user, password)

        ag = age.connect(graph=GRAPH_NAME, dsn=DSN)
        
        # A helper function to convert quoted keys of property to unqouted strings
        def __get_properties(property):
            items = list(f"{key}:\"{value}\"" for key, value in property.items())
            return "{"+",".join(items)+"}"
        
        # Create nodes in graph
        for node in nx_graph.nodes():
            vals = Namespace(**nx_graph.nodes[node])
            label = vals.label.strip("'")
            properties = __get_properties(vals.properties)
            with ag.connection.cursor() as cursor:
                ag.cypher(cursor, "CREATE (n:{} {}) RETURN n".format(label, properties))
                for row in cursor:
                    print(row[0])
                    
        # Create edges in graph
        for start_id, end_id in nx_graph.edges():
            vals = Namespace(**nx_graph.edges[start_id, end_id])
            label = vals.label.strip("'")
            properties = __get_properties(vals.properties)
            with ag.connection.cursor() as cursor:
                ag.cypher(cursor, """
                        MATCH (a), (b) 
                        WHERE  id(a) = {} AND id(b) = {} 
                        CREATE (a)-[e:{} {}]->(b)  
                        RETURN e""".format(start_id, end_id, label, properties))
                for row in cursor:
                    print(row[0])
        # commit to save changes
        ag.commit()
        # connection close
        ag.close()
    except Exception as e:
        print(e)
        ag.rollback()