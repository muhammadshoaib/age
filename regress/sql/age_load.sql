/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

\! cp -r regress/age_load/data regress/instance/data/age_load

LOAD 'age';

SET search_path TO ag_catalog;
SELECT create_graph('agload_test_graph');

SELECT create_vlabel('agload_test_graph','Country');
SELECT load_labels_from_file('agload_test_graph', 'Country',
    'age_load/countries.csv');

SELECT create_vlabel('agload_test_graph','City');
SELECT load_labels_from_file('agload_test_graph', 'City',
    'age_load/cities.csv');

SELECT create_elabel('agload_test_graph','has_city');
SELECT load_edges_from_file('agload_test_graph', 'has_city',
     'age_load/edges.csv');

SELECT table_catalog, table_schema, lower(table_name) as table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'agload_test_graph' ORDER BY table_name ASC;

SELECT COUNT(*) FROM agload_test_graph."Country";
SELECT COUNT(*) FROM agload_test_graph."City";
SELECT COUNT(*) FROM agload_test_graph."has_city";

SELECT COUNT(*) FROM cypher('agload_test_graph', $$MATCH(n) RETURN n$$) as (n agtype);

SELECT COUNT(*) FROM cypher('agload_test_graph', $$MATCH (a)-[e]->(b) RETURN e$$) as (n agtype);

SELECT create_vlabel('agload_test_graph','Country2');
SELECT load_labels_from_file('agload_test_graph', 'Country2',
                             'age_load/countries.csv', false);

SELECT create_vlabel('agload_test_graph','City2');
SELECT load_labels_from_file('agload_test_graph', 'City2',
                             'age_load/cities.csv', false);

SELECT COUNT(*) FROM agload_test_graph."Country2";
SELECT COUNT(*) FROM agload_test_graph."City2";

SELECT id FROM agload_test_graph."Country" LIMIT 10;
SELECT id FROM agload_test_graph."Country2" LIMIT 10;

SELECT * FROM cypher('agload_test_graph', $$MATCH(n:Country {iso2 : 'BE'})
    RETURN id(n), n.name, n.iso2 $$) as ("id(n)" agtype, "n.name" agtype, "n.iso2" agtype);
SELECT * FROM cypher('agload_test_graph', $$MATCH(n:Country2 {iso2 : 'BE'})
    RETURN id(n), n.name, n.iso2 $$) as ("id(n)" agtype, "n.name" agtype, "n.iso2" agtype);

SELECT * FROM cypher('agload_test_graph', $$MATCH(n:Country {iso2 : 'AT'})
    RETURN id(n), n.name, n.iso2 $$) as ("id(n)" agtype, "n.name" agtype, "n.iso2" agtype);
SELECT * FROM cypher('agload_test_graph', $$MATCH(n:Country2 {iso2 : 'AT'})
    RETURN id(n), n.name, n.iso2 $$) as ("id(n)" agtype, "n.name" agtype, "n.iso2" agtype);

SELECT * FROM cypher('agload_test_graph', $$
    MATCH (u:Country {region : "Europe"})
    WHERE u.name =~ 'Cro.*'
    RETURN u.name, u.region
$$) AS (result_1 agtype, result_2 agtype);

SELECT drop_graph('agload_test_graph', true);

--
-- Test property type conversion
--
SELECT create_graph('agload_conversion');

SELECT create_vlabel('agload_conversion','Person1');
SELECT load_labels_from_file('agload_conversion', 'Person1', 'age_load/conversion_vertices.csv');
SELECT * FROM cypher('agload_conversion', $$ MATCH (n) RETURN properties(n) $$) as (a agtype);

SELECT create_vlabel('agload_conversion','Person2');
SELECT load_labels_from_file('agload_conversion', 'Person2', 'age_load/conversion_vertices.csv');

SELECT create_elabel('agload_conversion','Edges');
SELECT load_edges_from_file('agload_conversion', 'Edges', 'age_load/conversion_edges.csv');
SELECT * FROM cypher('agload_conversion', $$ MATCH ()-[e]->() RETURN properties(e) $$) as (a agtype);

SELECT drop_graph('agload_conversion', true);
