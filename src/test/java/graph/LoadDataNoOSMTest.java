package graph;

import static org.junit.Assert.*;
import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import commons.Config;
import commons.Config.system;
import commons.Utility;

public class LoadDataNoOSMTest {

  static Config config = new Config();
  static String dataset = config.getDatasetName();
  static String version = config.GetNeo4jVersion();
  static system systemName = config.getSystemName();

  static String db_path, graph_pos_map_path;
  static String querygraphDir, spaPredicateDir;
  static GraphDatabaseService databaseService;

  static int nodeCount = 7, query_id = 0;
  // static int name_suffix = 7;
  // static int name_suffix = 75;
  // static int name_suffix = 7549;//0.1
  static int name_suffix = 75495;// 0.1
  static String queryrect_path = null, querygraph_path = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    switch (systemName) {
      case Ubuntu:
        db_path = String.format(
            "/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version,
            dataset);
        graph_pos_map_path =
            "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/node_map_RTree.txt";
        querygraphDir =
            String.format("/mnt/hgfs/Google_Drive/Projects/risotree/query/query_graph/%s", dataset);
        spaPredicateDir = String
            .format("/mnt/hgfs/Google_Drive/Projects/risotree/query/spa_predicate/%s", dataset);
        querygraph_path = String.format("%s/%d.txt", querygraphDir, nodeCount);
        queryrect_path = String.format("%s/queryrect_%d.txt", spaPredicateDir, name_suffix);
        break;
      case Windows:
        String dataDirectory = "D:\\Ubuntu_shared\\GeoMinHop\\data";
        db_path = String.format("%s\\%s\\%s_%s\\data\\databases\\graph.db", dataDirectory, dataset,
            version, dataset);
        graph_pos_map_path =
            "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset + "\\node_map_RTree.txt";
        querygraphDir =
            String.format("D:\\Google_Drive\\Projects\\risotree\\query\\query_graph\\%s", dataset);
        spaPredicateDir = String
            .format("D:\\Google_Drive\\Projects\\risotree\\query\\spa_predicate\\%s", dataset);
        querygraph_path = String.format("%s\\%d.txt", querygraphDir, nodeCount);
        queryrect_path = String.format("%s\\queryrect_%d.txt", spaPredicateDir, name_suffix);
      default:
        break;
    }
    databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
  }

  @After
  public void tearDown() throws Exception {
    databaseService.shutdown();
  }

  @Test
  public void nodeMapTest() {
    Transaction tx = databaseService.beginTx();
    Node node = databaseService.getNodeById(78589);
    Utility.print(node);
    Utility.print(node.getAllProperties());

    node = databaseService.getNodeById(10353);
    Utility.print(node);
    Utility.print(node.getAllProperties());

    tx.success();
    tx.close();
  }

}
