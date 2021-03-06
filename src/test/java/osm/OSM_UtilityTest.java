package osm;

import static org.junit.Assert.*;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import commons.Config;
import commons.Enums;
import commons.Enums.system;
import commons.Labels.OSMLabel;
import commons.Labels.RTreeRel;
import commons.Util;

public class OSM_UtilityTest {

  static Config config = new Config();
  static String dataset = config.getDatasetName();
  static String version = config.GetNeo4jVersion();
  static Enums.system systemName = config.getSystemName();
  static int MAX_HOPNUM = config.getMaxHopNum();
  static int nonspatial_label_count = config.getNonSpatialLabelCount();

  static String db_path;
  static String graph_path;
  static String entityPath;
  static String label_list_path;
  static String log_path;
  static String dataDirectory;

  static ArrayList<Integer> labels;// all labels in the graph

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    switch (systemName) {
      case Ubuntu:
        db_path = String.format(
            "/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version,
            dataset);
        graph_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
        entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
        label_list_path =
            String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/label.txt", dataset);
        log_path = String.format("/mnt/hgfs/Experiment_Result/Riso-Tree/%s/set_label.log", dataset);
        break;
      case Windows:
        dataDirectory = "D:\\Ubuntu_shared\\GeoMinHop\\data";
        db_path = String.format("%s\\%s\\%s_%s\\data\\databases\\graph.db", dataDirectory, dataset,
            version, dataset);
        graph_path = String.format("%s\\%s\\graph.txt", dataDirectory, dataset);
        entityPath = String.format("%s\\%s\\entity.txt", dataDirectory, dataset);
        label_list_path = String.format("%s\\%s\\label.txt", dataDirectory, dataset);
        log_path = String.format("%s\\%s\\set_label.log", dataDirectory, dataset);
      default:
        break;
    }

    if (nonspatial_label_count == 1)
      labels = new ArrayList<Integer>(Arrays.asList(0, 1));
    else {
      labels = new ArrayList<Integer>(1 + nonspatial_label_count);
      labels.add(1);
      for (int i = 0; i < nonspatial_label_count; i++)
        labels.add(i + 2);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Test
  public void getReferenceTest() {
    Util.println(db_path);
    GraphDatabaseService databaseService =
        new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
    Transaction tx = databaseService.beginTx();
    ResourceIterator<Node> referenceNodes = databaseService.findNodes(OSMLabel.OSM_NODE);
    while (referenceNodes.hasNext())
      Util.println(referenceNodes.next().getAllProperties());
  }

  @Test
  public void getAllReferenceNode() {
    String layer_name = dataset;
    GraphDatabaseService databaseService =
        new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
    Transaction tx = databaseService.beginTx();
    ResourceIterator<Node> nodes = databaseService.findNodes(OSMLabel.ReferenceNode);
    while (nodes.hasNext()) {
      Node head = nodes.next();
      Iterable<Relationship> relationships =
          head.getRelationships(RTreeRel.LAYER, Direction.OUTGOING);
      for (Relationship relationship : relationships) {
        Node rtree_layer_node = relationship.getEndNode();
        Util.println(rtree_layer_node);
        Util.println(rtree_layer_node.getAllProperties());
      }
    }
  }

}
