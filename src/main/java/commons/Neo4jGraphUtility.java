package commons;

import java.io.File;
import java.util.HashSet;
import java.util.logging.Logger;
import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

public class Neo4jGraphUtility {

  private static final Logger LOGGER = Logger.getLogger(Neo4jGraphUtility.class.getName());

  /**
   * Check if a label is spatial by checking whether the first node found using {@code findNodes} is
   * a spatial node or not. It is not an accurate logic but is enough for generating query graph for
   * LAGAQ-join.
   *
   * @param node
   * @return
   */
  public static boolean isLabelSpatial(GraphDatabaseService service, Label label) {
    ResourceIterator<Node> nodeIterator = service.findNodes(label);
    if (nodeIterator.hasNext()) {
      Node node = nodeIterator.next();
      return isNodeSpatial(node);
    }
    return false;
  }

  /**
   * Check if a node is spatial by checking whether it has {@code latitude_property_name}.
   *
   * @param node
   * @return
   */
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
   * Get database service. Stop the program if dbPath does not exist.
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

  public static long getInAndOutEdgeCount(GraphDatabaseService service, String label) {
    return getInEdgeCount(service, label) + getOutEdgeCount(service, label);
  }

  public static long getOutEdgeCount(GraphDatabaseService service, String label) {
    String query = String.format("match (n:`%s`)-[]->() return count(*)", label);
    Result result = service.execute(query);
    long count = -1;
    while (result.hasNext()) {
      count = (long) result.next().get("count(*)");
      break;
    }
    if (count == -1) {
      throw new RuntimeException(query + " \nis wrong!");
    }
    return count;
  }

  public static long getInEdgeCount(GraphDatabaseService service, String label) {
    String query = String.format("match (n:`%s`)<-[]-() return count(*)", label);
    Result result = service.execute(query);
    long count = -1;
    while (result.hasNext()) {
      count = (long) result.next().get("count(*)");
      break;
    }
    if (count == -1) {
      throw new RuntimeException(query + " \nis wrong!");
    }
    return count;
  }

  public static long getLabelCount(GraphDatabaseService service, String label) {
    String query = String.format("match (n:`%s`) return count(*)", label);
    Result result = service.execute(query);
    long count = -1;
    while (result.hasNext()) {
      count = (long) result.next().get("count(*)");
      break;
    }
    if (count == -1) {
      throw new RuntimeException(query + " \nis wrong!");
    }
    return count;
  }
}
