package graph;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import commons.Neo4jGraphUtility;

public class Neo4j_API {

  public GraphDatabaseService graphDb;

  public Neo4j_API(String dbpath) {
    graphDb = Neo4jGraphUtility.getDatabaseService(dbpath);
  }

  public Neo4j_API(GraphDatabaseService service) {
    this.graphDb = service;
  }

  public void ShutDown() {
    if (graphDb != null)
      graphDb.shutdown();
  }

  public Node GetNodeByID(long id) {
    Node node = null;
    try {
      node = graphDb.getNodeById(id);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return node;
  }

}
