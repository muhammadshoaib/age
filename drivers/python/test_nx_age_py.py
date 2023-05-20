from age.models import Vertex
import unittest
import decimal
from age import *
from nx_age import *
import networkx as nx

DSN = "host=localhost port=5432 dbname=tee_demodb user=tito password=jacz"
TEST_GRAPH_NAME = "test"

class TestAgeBasic(unittest.TestCase):
    ag = None
    def setUp(self):
        print("Connecting to Test Graph.....")
        self.ag = age.connect(graph=TEST_GRAPH_NAME, dsn=DSN)

    def tearDown(self):
        # Clear test data
        print("Deleting Test Graph.....")
        age.deleteGraph(self.ag.connection, self.ag.graphName)
        self.ag.close()

    def testload_to_networkx(self):
        ag = self.ag
        # Create nodes
        ag.execCypher("CREATE (n:Person {name: %s}) ", params=('Jack',))
        ag.execCypher("CREATE (n:Person {name: %s}) ", params=('Andy',))
        ag.execCypher("CREATE (n:Person {name: %s}) ", params=('Smith',))
        ag.commit()
        # Create Edges
        ag.execCypher("""MATCH (a:Person), (b:Person)
                      WHERE a.name = 'Andy' AND b.name = 'Jack'
                      CREATE (a)-[r:workWith {weight: 3}]->(b)""")
        ag.execCypher("""MATCH (a:Person), (b:Person) 
                    WHERE  a.name = %s AND b.name = %s 
                    CREATE p=((a)-[r:workWith {weight: 10}]->(b)) """, params=('Jack', 'Smith',))
        ag.commit()

        G = load_to_networkx(graph_name=TEST_GRAPH_NAME, DSN=DSN, query_output=None)
        
        # Check that the graph has the expected properties
        self.assertIsInstance(G, nx.DiGraph)
        # Check that the graph has the correct number of nodes and edges
        self.assertEqual(len(G.nodes), 3)
        self.assertEqual(len(G.edges), 2)
        # Check that the node properties are correct
        for node in G.nodes:
            self.assertEqual(G.nodes[node]['label'], 'Person')
            self.assertIn('name', G.nodes[node]['properties'])
            self.assertEqual(str, type(G.nodes[node]['label']))

        # Check that the edge properties are correct
        for edge in G.edges:
            self.assertEqual(G.edges[edge]['label'], 'workWith')
            self.assertIn('weight', G.edges[edge]['properties'])
            self.assertEqual(int, type(G.edges[edge]['properties']['weight']))

    def testload_from_networkx(self):
        graph = nx.DiGraph()
        graph.add_node(1, label='Person', properties={'name': 'Tito', 'age': '26', 'id': 1})
        graph.add_node(2, label='Person', properties={'name': 'Austen', 'age': '26', 'id': 2})
        graph.add_edge(1, 2, label='KNOWS', properties={'since': '1997', 'start_id': 1, 'end_id': 2})

        load_from_networkx(G=graph, graph_name='test_graph', DSN=DSN)

        ag = age.connect(graph='test_graph', dsn=DSN)

        # Check that node(s) were created
        cursor = ag.execCypher('MATCH (n) RETURN n')
        result = cursor.fetchall()
        # Check number of vertices created
        self.assertEqual(len(result), 2)
        # Checks if type of property in query output is a Vertex
        self.assertEqual(age.models.Vertex, type(result[0][0]))
        self.assertEqual(age.models.Vertex, type(result[1][0]))

        # Check that edge(s) was created
        cursor = ag.execCypher('MATCH ()-[e]->() RETURN e')
        result = cursor.fetchall()
        # Check number of edge(s) created
        self.assertEqual(len(result), 1)
        # Checks if type of property in query output is an Edge
        self.assertEqual(age.models.Edge, type(result[0][0]))

if __name__ == '__main__':
    unittest.main()
