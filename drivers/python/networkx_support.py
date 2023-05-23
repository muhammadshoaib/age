"""
Author: Syed Safi Ullah Shah
GitHubID: safi50 
DiscordID: safi50
"""
import psycopg2 
from psycopg2 import extensions as ext
# from psycopg2 import sql
import networkx as nx
import age
# from age.age import Age
from age.models import Vertex, Edge, Path
from age.exceptions import *


class NetworkXSupport:

    """
        DSN = "host=localhost port=5432 dbname={database_name} user={user_name} password={user_password}"
    """

    def age_to_networkx(graph_name:str, dsn:str) -> nx.DiGraph():
        try: 
            # initializing empty NetworkX DiGraph
            G = nx.DiGraph()
            nodes = []
            edges = []

            #connecting with AGE 
            ag = age.connect(graph=graph_name, dsn=dsn)
            #Extracting and Loading Vertices to NetworkX DiGraph
            cursor = ag.execCypher("MATCH (n) RETURN (n)")
            for row in cursor:

                nodes.append((row[0].id, {'label': row[0].label, **row[0].properties}))

            #Adding nodes to NetworkX DiGraph    
            G.add_nodes_from(nodes)

            # Extracting and Loading Edges to NetworkX Digraph
            cursor = ag.execCypher("MATCH ()-[e]->() RETURN e")
            for row in cursor:

                edges.append((row[0].start_id, row[0].end_id, 
                {'label': row[0].label, "id": row[0].id, 'properties': row[0].properties}))

            #Adding edges to NetworkX DiGraph
            G.add_edges_from(edges)

            return G            

        except Exception as e:
            print("Exception:",e)


    def cypher_to_networkx(graph_name:str, dsn:str, cypherQuery:str) -> nx.DiGraph():
        try:

            G = nx.DiGraph()
            
            ag = age.connect(graph=graph_name, dsn=dsn)

            cursor = ag.execCypher(cypherQuery)
            for row in cursor: 
                if not isinstance(row[0], (Vertex, Edge, Path)):
                    print("Output:", row)
                    return G
                
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
                        
            return G
        
        except Exception as e:
            print(e)

   
    
    def networkx_to_age(nx_graph:nx.DiGraph(), age_graph:str, dsn:str):

        def dict_to_props(d):
            """
            Convert a Python dictionary to a string representation of AGE properties.
            """

            items = [f"{k}:{repr(v)}" for k, v in d.items()]
            return "{" + ", ".join(items) + "}"

        try:
            """
            Checks connection with AGE and if not, sets it up.
            """
            ag = age.connect(graph=age_graph, dsn=dsn)


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
 
            if len(vertices) / 1600 > 1:
                batches = len(vertices) // 1600  
                for iter in range(batches):

                    #Loading Vertices in batches of 1600 to avoid Cypher query size limit
                    cypher_query = ", ".join(vertices[i] for i in range(iter*1600, (iter+1)*1600))

                #Check to avoid empty cypher query
                    if cypher_query:
                        ag.execCypher(f"CREATE {cypher_query} Return 1")
                    cypher_query = ""
  

            # Add edges
            for start, end, props in nx_graph.edges(data=True):

                #Converting networkX properties to loadable AGE properties
                props = dict_to_props(props)

                #Since Edges need to match the vertices, we need a separate cypher query for each edge.
                ag.execCypher(f"MATCH (a),(b) WHERE a.label={start} AND b.label={end} CREATE (a)-[e:edge {props}]->(b) RETURN 2")
               
            # Committing the changes
            ag.commit()
            print("-> Loaded NetworkX {} into AGE graph '{}'".format(nx_graph, age_graph))

        except Exception as e:
            print(e)
            ag.rollback()
