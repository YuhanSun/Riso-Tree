# Riso-Tree

## How to deploy the experiment for maxPNSize

### 0. Rename the empty graph db folder to neo4j-community-3.4.12_Gleenes_1.0_-1_new_version

### 1. Generate 0-1 hop pn for spatial nodes
**required files:**

1. graph.txt
2. graph_label.txt
3. entity.txt
4. entity_string_label.txt

I will output **spatialNodesZeroOneHopPN_-1.txt**

### 2. Load graph nodes and edges
**required files:**

1. Graph database file folder. (graph.db folder should be removed at the beginning)
2. graph.txt

### 3. Construct RTree
**required files:**

1. Graph database file folder.
2. entity.txt
3. spatialNodesZeroOneHopPN_-1.txt

### 4. Generate containID.txt

1-4 are run by using constructTree_single.sh
If fail the load step (either Step 2 or 3, need to delete this folder: data/databases/graph.db).

### 5. Rename the generated graph database folder to from neo4j-community-3.4.12_Gleenes_1.0_-1_new_version to four different folder names. It can done manually or using copy_maxPNSize.sh (path modification required).
1. neo4j-community-3.4.12_Gleenes_1.0_10_new_version
2. neo4j-community-3.4.12_Gleenes_1.0_40_new_version
3. neo4j-community-3.4.12_Gleenes_1.0_160_new_version
4. neo4j-community-3.4.12_Gleenes_1.0_640_new_version


### 6. Generate the PN files (done already. Skip this time)
Run maxPNSize_construct_PN_single.sh

### 7. Load PN files into graph db. (maxPNSize_PNLoad_single.sh)
**required files**

1. PN files
2. Graph db folder
3. containID.txt

If this step fails after the graph db is accessed, need to re-run from scratch.

### 7. Execute the query and output the result to disk. (maxPNSize_query_multiple_queries_single.sh)



