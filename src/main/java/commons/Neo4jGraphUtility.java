package commons;

import java.io.File;
import java.util.HashSet;
import java.util.logging.Logger;
import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

public class Neo4jGraphUtility {

  private static final Logger LOGGER = Logger.getLogger(Neo4jGraphUtility.class.getName());

  public static boolean isNodeSpatial(Node node) {
    return node.hasProperty(Config.latitude_property_name);
  }

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
   * Return all the graph neighbors for the given node. Consider all edges except for RTree edges.
   * This is used because the inserted edges is not "GRAPH_LINK" but "GRAPH_INSERT". If use the
   * {@code getGraphNeighbors} function, those edges will be lost.
   * 
   * @param node
   * @return
   */
  public static HashSet<Node> getGraphNeighborsNoRTree(Node node) {
    Iterable<Relationship> rels = node.getRelationships();
    HashSet<Node> neighbors = new HashSet<>();
    for (Relationship relationship : rels) {
      if (relationship.isType(RTreeRelationshipTypes.RTREE_REFERENCE)) {
        continue;
      }
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
      Util.println(dbPath + " does not exist!");
      System.exit(-1);
    }
    // GraphDatabaseService dbservice =
    // new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbPath));
    GraphDatabaseService dbservice =
        new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(new File(dbPath))
            .setConfig(GraphDatabaseSettings.pagecache_memory, "20M").newGraphDatabase();
    return dbservice;
  }

  public static GraphDatabaseService getDatabaseServiceNotExistCreate(String dbPath) {
    if (!Util.pathExist(dbPath)) {
      LOGGER.info("path does not exist and database is created in " + dbPath);
    }
    return new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbPath));
  }

}
