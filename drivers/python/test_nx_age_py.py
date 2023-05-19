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
        # Check that the node attributes are correct
        for node in G.nodes:
            self.assertEqual(G.nodes[node]['label'], 'Person')
            self.assertIn('name', G.nodes[node]['properties'])
            self.assertEqual(str, type(G.nodes[node]['label']))

        # Check that the edge attributes are correct
        for edge in G.edges:
            self.assertEqual(G.edges[edge]['label'], 'workWith')
            self.assertIn('weight', G.edges[edge]['properties'])
            self.assertEqual(int, type(G.edges[edge]['properties']['weight']))

        # Test the function with an invalid graph name
        with self.assertRaises(Exception):
            load_to_networkx('nonexistent_graph')

if __name__ == '__main__':
    unittest.main()