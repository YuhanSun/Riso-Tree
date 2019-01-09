package graph;

import java.io.File;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Neo4j_API {

  public GraphDatabaseService graphDb;

  public Neo4j_API(String dbpath) {
    graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbpath));
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
