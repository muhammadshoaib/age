import age
import networkx as nx
from psycopg2 import extensions as ext

# Loads AGE graph or query output to NetworkX
def load_to_networkx(graph_name:str, DSN:str, query_output:ext.cursor=None) -> nx.DiGraph:
    def addNode(node):
        graph.add_node(node.id, label=node.label, properties=node.properties)
    def addEdge(edge):
        graph.add_edge(edge.start_id, edge.end_id, id= edge.id,
                           label=edge.label, properties=edge.properties)   
    def addPath(path):
        for entity in path:
            if isinstance(entity, age.models.Path):
                addPath(entity)
            elif isinstance(entity, age.models.Vertex):
                addNode(entity)
            elif isinstance(entity, age.models.Edge):
                addEdge(entity)

    # Connect to PostgreSQL and AGE graph
    ag = age.connect(graph=graph_name, dsn=DSN)
    # Create NetworkX DiGraph object
    graph = nx.DiGraph()

    # If Cypher query output is parsed as an argument, load the output
    if (query_output != None):
        try:
            query_output = query_output.fetchall()
            for row in query_output:
                if isinstance(row[0], age.models.Path):
                    addPath(row[0])
                elif isinstance(row[0], age.models.Vertex):
                    addNode(row[0])
                elif isinstance(row[0], age.models.Edge):
                    addEdge(row[0])
        except Exception as e:
            print(type(e), e)
    # If Cypher query output is not parsed, load AGE graph instead        
    else:
        try:
            # Add Nodes
            cursor = ag.execCypher("MATCH (n) RETURN (n)")
            for row in cursor:
                addNode(row[0])
            # Add edges
            cursor = ag.execCypher("MATCH ()-[e]->() RETURN (e)")
            for row in cursor:
                addEdge(row[0])
        except Exception as e:
            print(type(e), e)   
    return graph


# Loads to NetworkX from AGE graph 
def load_from_networkx(G:nx.DiGraph(), graph_name:str, DSN:str):
    # Check for label in all nodes in NetworkX DiGraph object
    for node in G.nodes:
        if 'label' not in G.nodes[node]:
            raise Exception('Node ', node, ' has no label')
    # Check for label in all edges in NetworkX DiGraph object
    for edge in G.edges:
        if 'label' not in G.edges[edge]:
            raise Exception('Edge ', edge, ' has no label')
    
    # Connect to PostgreSQL and AGE graph  
    ag = age.connect(graph=graph_name, dsn=DSN)

    # Convert Python dictionary to JSON-type string
    def dict_to_JSstring(property):
            items = list(f"{key}:\"{value}\"" for key, value in property.items())
            return "{"+",".join(items)+"}"
    
    def addNode(node):
        try:
            ag.execCypher(f"""CREATE (n:{G.nodes[node]['label']} 
            {dict_to_JSstring(G.nodes[node]['properties'])}) 
            RETURN n""")
            ag.commit()
        except Exception as e:
            print(type(e), e)
    def addEdge(start, end):
        try:
            ag.execCypher(f"""MATCH (a), (b) 
            WHERE  a.id = '{start}' AND b.id = '{end}' 
            CREATE (a)-[e:{G.edges[start, end]['label']} 
            {dict_to_JSstring(G.edges[start, end]['properties'])}]->(b) 
            RETURN e""")
            ag.commit()
        except Exception as e:
            print(type(e), e)
    
    # Create Nodes
    for node in G.nodes():
        addNode(node)
    # Create Edges
    for start, end in G.edges():
        addEdge(start, end)
