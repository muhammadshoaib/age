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

import age
import os
import unittest
from tempfile import TemporaryDirectory
from age.LoadStoreAge import StoreAge
import argparse

DSN = "host=127.0.0.1 port=5432 dbname=postgres user=postgres password=agens"

class TestAgeStore(unittest.TestCase):
    ag = None
    def setUp(self):
        print("")
        print("Connecting to Test Graph.....")
        # Setting up connection to work with Graph
        TEST_DB = self.args.database
        TEST_USER = self.args.user
        TEST_PASSWORD = self.args.password
        TEST_PORT = self.args.port
        TEST_HOST = self.args.host
        TEST_GRAPH_NAME = self.args.graphName
        self.ag = age.connect(graph=TEST_GRAPH_NAME, host=TEST_HOST, port=TEST_PORT, dbname=TEST_DB, user=TEST_USER, password=TEST_PASSWORD)

    def tearDown(self):
        # Clear test data
        print("Deleting Test Graph.....")
        age.deleteGraph(self.ag.connection, self.ag.graphName)
        self.ag.close()

    def testload_labels_into_csv(self):
        print("\n----------------------------------")
        print("Test 1: Testing load_labels_into_csv")
        print("----------------------------------\n")

        ag = self.ag
        
        # Create nodes
        ag.execCypher("CREATE (n:Person {id: 1, name: %s, age: 30}) ", params=('John',))
        ag.execCypher("CREATE (n:Person {id: 2, name: %s, age: 25}) ", params=('Jane',))
        ag.execCypher("CREATE (n:Person {id: 45, name: %s, age: 45}) ", params=('Jack',))
        ag.commit()

        with TemporaryDirectory() as temp_dir:
            load_labels_into_file(ag.connection, ag.graphName, temp_dir)

            expected_file = "Person.csv"
            file_path = os.path.join(temp_dir + '/data', expected_file)
            self.assertTrue(os.path.exists(file_path))

            # Check the content and structure of the CSV file
            with open(file_path, 'r') as file:
                reader = csv.reader(file)
                header = next(reader)
                self.assertEqual(header, ['id', 'age', 'name'])

                for row in reader:
                    # Check number of columns
                    self.assertEqual(len(row), 3)
                    self.assertTrue(row[0].isdigit())
                    self.assertTrue(row[1].isdigit())
                    self.assertTrue(row[2].isalpha())
        print("Test 1: Successful...\n")

    def testload_labels_into_json(self):
        print("\n-----------------------------------")
        print("Test 2: Testing load_labels_into_json")
        print("-----------------------------------\n")

        ag = self.ag
        
        # Create nodes
        ag.execCypher("""CREATE (n:Person {id: 1, name: %s, age: 30,
                      hobbies: {games: {field: %s}}}) """, params=('John', 'Football'))
        ag.execCypher("""CREATE (n:Person {id: 2, name: %s, age: 25,
                      hobbies: {games: {field: %s}}}) """, params=('Jane', 'Rugby'))
        ag.execCypher("""CREATE (n:Person {id: 45, name: %s, age: 45,
                      hobbies: {games: {field: %s}}}) """, params=('Jack', 'Basketball'))
        ag.commit()

        with TemporaryDirectory() as temp_dir:
            load_labels_into_file(ag.connection, ag.graphName, temp_dir)

            expected_file = "Person.json"
            file_path = os.path.join(temp_dir + '/data', expected_file)
            self.assertTrue(os.path.exists(file_path))

            # Check the content and structure of the JSON file
            with open(file_path, 'r') as file:
                json_data = json.load(file)
                # Check if it's a list of dictionaries
                self.assertIsInstance(json_data, list)  
                
                for json_object in json_data:
                    self.assertIsInstance(json_object, dict)
                    self.assertIn("id", json_object)  
                    self.assertIsInstance(json_object["id"], int)
                    self.assertIn("name", json_object)
                    self.assertIsInstance(json_object["name"], str)
                    self.assertIn("age", json_object)
                    self.assertIsInstance(json_object["age"], int)
        print("Test 2: Successful...\n")
    
    def testload_edges_into_csv(self):
        print("\n---------------------------------")
        print("Test 3: Testing load_edges_into_csv")
        print("---------------------------------\n")

        ag = self.ag

        # Create nodes
        ag.execCypher("CREATE (n:Person {id: 1, name: %s, age: 30}) ", params=('John',))
        ag.execCypher("CREATE (n:Person {id: 2, name: %s, age: 25}) ", params=('Jane',))
        ag.execCypher("CREATE (n:Person {id: 45, name: %s, age: 45}) ", params=('Jack',))
        ag.commit()

        # Create Edges
        ag.execCypher("""MATCH (a:Person), (b:Person)
                      WHERE a.name = 'John' AND b.name = 'Jack'
                      CREATE (a)-[r:Friends {weight: 3}]->(b)""")
        ag.execCypher("""MATCH (a:Person), (b:Person) 
                    WHERE  a.name = 'Jack' AND b.name = 'Jane' 
                    CREATE (a)-[r:Friends {weight: 3}]->(b)""")
        ag.commit()

        with TemporaryDirectory() as temp_dir:
            load_edges_into_file(ag.connection, ag.graphName, temp_dir)

            file_path = os.path.join(temp_dir + '/data', "Friends.csv")
            self.assertTrue(os.path.exists(file_path))

            # Check the content and structure of the CSV file
            with open(file_path, 'r') as file:
                reader = csv.reader(file)
                header = next(reader)
                self.assertEqual(header, ['start_id', 'start_vertex_type',
                                          'end_id', 'end_vertex_type', 'weight'])

                for row in reader:
                    # Check number of columns
                    self.assertEqual(len(row), 5)
                    self.assertTrue(row[0].isdigit())
                    self.assertTrue(row[1].isalpha())
                    self.assertTrue(row[2].isdigit())
                    self.assertTrue(row[3].isalpha())
                    self.assertTrue(row[4].isdigit())
        print("Test 3: Successful...\n")

    def testload_edges_into_json(self):
        print("\n----------------------------------")
        print("Test 4: Testing load_edges_into_json")
        print("----------------------------------\n")

        ag = self.ag

        # Create nodes
        ag.execCypher("CREATE (n:Person {id: 1, name: %s, age: 30}) ", params=('John',))
        ag.execCypher("CREATE (n:Person {id: 2, name: %s, age: 25}) ", params=('Jane',))
        ag.execCypher("CREATE (n:Person {id: 45, name: %s, age: 45}) ", params=('Jack',))
        ag.commit()

        # Create Edges
        ag.execCypher("""MATCH (a:Person), (b:Person)
                      WHERE a.name = %s AND b.name = %s
                      CREATE (a)-[r:Friends {weight: 3, likes: {eating: {food: %s}}}]->(b)"""
                      , params=('John', 'Jack', 'Pizza'))
        ag.execCypher("""MATCH (a:Person), (b:Person)
                      WHERE  a.name = %s AND b.name = %s
                      CREATE (a)-[r:Friends {weight: 3, likes: {eating: {food: %s}}}]->(b)"""
                      , params=('Jack', 'Jane', 'Rice'))
        ag.commit()

        with TemporaryDirectory() as temp_dir:
            load_edges_into_file(ag.connection, ag.graphName, temp_dir)

            file_path = os.path.join(temp_dir + '/data', "Friends.json")
            self.assertTrue(os.path.exists(file_path))

            # Check the content and structure of the JSON file
            with open(file_path, 'r') as file:
                json_data = json.load(file)
                # Check if it's a list of dictionaries
                self.assertIsInstance(json_data, list)

                for json_object in json_data:
                    self.assertIsInstance(json_object, dict)
                    self.assertIn("start_id", json_object)
                    self.assertIsInstance(json_object["start_id"], int)
                    self.assertIn("start_vertex_type", json_object)
                    self.assertIsInstance(json_object["start_vertex_type"], str)
                    self.assertIn("end_id", json_object)
                    self.assertIsInstance(json_object["end_id"], int)
                    self.assertIn("end_vertex_type", json_object)
                    self.assertIsInstance(json_object["end_vertex_type"], str)
                    self.assertIn("properties", json_object)
                    self.assertIsInstance(json_object["properties"], dict)
        print("Test 4: Successful...\n")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-host', 
                        '--host', 
                        help='Optional Host Name. Default Host is "127.0.0.1" ', 
                        default="127.0.0.1")
    parser.add_argument('-port', 
                        '--port', 
                        help='Optional Port Number. Default port no is 5432', 
                        default=5432)
    parser.add_argument('-db', 
                        '--database', 
                        help='Required Database Name', 
                        required=True)
    parser.add_argument('-u', 
                        '--user', 
                        help='Required Username Name', 
                        required=True)
    parser.add_argument('-pass', 
                        '--password', 
                        help='Required Password for authentication', 
                        required=True)
    parser.add_argument('-gn', 
                        '--graphName', 
                        help='Optional Graph Name to be created. Default graphName is "test_graph"', 
                        default="test_graph")

    args = parser.parse_args()
    suite = unittest.TestSuite()
    suite.addTest(TestAgeStore('testload_labels_into_csv'))
    suite.addTest(TestAgeStore('testload_labels_into_json'))
    suite.addTest(TestAgeStore('testload_edges_into_csv'))
    suite.addTest(TestAgeStore('testload_edges_into_json'))
    TestAgeStore.args = args
    unittest.TextTestRunner().run(suite)