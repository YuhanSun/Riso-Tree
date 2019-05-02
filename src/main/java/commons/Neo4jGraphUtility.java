package commons;

import java.io.File;
import java.util.HashSet;
import java.util.logging.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Neo4jGraphUtility {

  private static final Logger LOGGER = Logger.getLogger(Neo4jGraphUtility.class.getName());

  /**
   * Return all the graph neighbors for the given node. Only consider "GRAPH_LINK". RTree-related
   * edges are not included.
   * 
   * @param node
   * @return
   */
  public static HashSet<Node> getGraphNeighbors(Node node) {
    Iterable<Relationship> rels = node.getRelationships(RelationshipType.withName("GRAPH_LINK"));
    HashSet<Node> neighbors = new HashSet<>();
    for (Relationship relationship : rels) {
      neighbors.add(relationship.getOtherNode(node));
    }
    return neighbors;
  }

  /**
   * Get dababase service. Stop the program if dbPath does not exist.
   *
   * @param dbPath
   * @return
   */
  public static GraphDatabaseService getDatabaseService(String dbPath) {
    LOGGER.info("get dbservice from " + dbPath);
    if (!Util.pathExist(dbPath)) {
      Util.println(dbPath + "does not exist!");
      System.exit(-1);
    }
    GraphDatabaseService dbservice =
        new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbPath));
    return dbservice;
  }

  public static GraphDatabaseService getDatabaseServiceNotExistCreate(String dbPath) {
    return new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbPath));
  }

}
