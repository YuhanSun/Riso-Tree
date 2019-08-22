package experiment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import commons.Config;
import commons.Config.Datasets;
import commons.Config.system;
import commons.GraphUtil;
import commons.MyRectangle;
import commons.Neo4jGraphUtility;
import commons.OwnMethods;
import commons.RTreeUtility;
import commons.ReadWriteUtil;
import commons.RisoTreeUtil;
import commons.Util;

/**
 * This is used for analyze experiment results in the RisoTree Paper.
 * 
 * @author ysun138
 *
 */
public class Analyze {
  private static final Logger logger = Logger.getLogger(Analyze.class.getName());

  static Config config = new Config();
  static system systemName;
  static String version, dataset, lon_name, lat_name;
  static int nonspatial_label_count;

  static String dbPath, entityPath, mapPath, graphPath, labelListPath, hmbrPath;
  static ArrayList<String> dataset_a =
      new ArrayList<String>(Arrays.asList(Config.Datasets.Gowalla_100.name(),
          Config.Datasets.foursquare_100.name(), Config.Datasets.Patents_100_random_80.name(),
          Config.Datasets.go_uniprot_100_random_80.name()));

  // static ArrayList<Entity> entities;

  static void initParameters() {
    systemName = config.getSystemName();
    version = config.GetNeo4jVersion();
    dataset = config.getDatasetName();
    lon_name = config.GetLongitudePropertyName();
    lat_name = config.GetLatitudePropertyName();
    nonspatial_label_count = config.getNonSpatialLabelCount();
    switch (systemName) {
      case Ubuntu:
        dbPath = String.format(
            "/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version,
            dataset);
        entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
        labelListPath =
            String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/label.txt", dataset);
        // static String map_path =
        // String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map.txt", dataset);
        /**
         * use this because osm node are not seen as spatial graph but directly use RTree leaf node
         * as the spatial vertices in the graph
         */
        mapPath =
            String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map_RTree.txt", dataset);
        graphPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
        hmbrPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/HMBR.txt", dataset);
        break;
      case Windows:
        dbPath = String.format(
            "D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset,
            version, dataset);
        entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
        labelListPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\label.txt", dataset);
        mapPath =
            String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\node_map_RTree.txt", dataset);
        graphPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\graph.txt", dataset);
        hmbrPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\HMBR.txt", dataset);
      default:
        break;
    }

    // entities = OwnMethods.ReadEntity(entityPath);
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    Util.println(config.getDatasetName());
    config.setDatasetName(Datasets.Yelp_100.toString());
    initParameters();
    // getAverageDegree();
    // getSpatialEntityCount();
    // get2HopNeighborCount();

  }

  public static void leafNodesOverlapAnalysis(String dbPath, String dataset, String logPath)
      throws Exception {
    GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(dbPath);
    Transaction tx = service.beginTx();
    List<Node> leafNodes = RTreeUtility.getRTreeLeafLevelNodes(service, dataset);
    double total = 0.0;
    for (Node node : leafNodes) {
      MyRectangle sourceRect = RTreeUtility.getNodeMBR(node);
      List<Node> overlapNodes = RTreeUtility.getOverlapLeafNodes(service, dataset, sourceRect);
      long id = node.getId();
      for (Node overlapNode : overlapNodes) {
        if (id == overlapNode.getId()) {
          continue;
        }
        total += sourceRect.intersect(RTreeUtility.getNodeMBR(overlapNode)).area();
      }
    }
    tx.success();
    tx.close();
    Util.close(service);
    ReadWriteUtil.WriteFile(logPath, true, String.format("%s,%f", dbPath, total));
  }

  /**
   * Get the PathNeighbor count for a PN file.
   *
   * @param filepath
   * @throws Exception
   */
  public static void getPNNonEmptyCount(String filepath, String logPath) throws Exception {
    Map<Long, Map<String, int[]>> pns = ReadWriteUtil.readLeafNodesPathNeighbors(filepath);
    int countNeighbors = 0; // the count of neighbors in the PN
    int PNPropertyCount = 0; // the count of PN itself
    for (long leafNodeId : pns.keySet()) {
      Map<String, int[]> pn = pns.get(leafNodeId);
      PNPropertyCount += pn.size();
      for (String pnKey : pn.keySet()) {
        countNeighbors += pn.get(pnKey).length;
      }
    }
    ReadWriteUtil.WriteFile(logPath, true,
        String.format("%s,%d,%d\n", filepath, PNPropertyCount, countNeighbors));
  }

  public static void getPNSizeDistribution(String dbPath, String dataset, String outputPath)
      throws Exception {
    GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(dbPath);
    Map<Object, Object> histgram = new TreeMap<>();
    Transaction tx = service.beginTx();
    List<Node> leafNodes = RTreeUtility.getRTreeLeafLevelNodes(service, dataset);
    int index = 0;
    for (Node node : leafNodes) {
      getNodePNSizeDistribution(node, histgram);
      index++;
      if (index % 10000 == 0) {
        logger.info("" + index);
      }
    }
    tx.success();
    tx.close();
    Util.close(service);
    ReadWriteUtil.WriteMap(outputPath, false, histgram);
  }

  private static void getNodePNSizeDistribution(Node node, Map<Object, Object> histgram) {
    Map<String, Object> properties = node.getAllProperties();
    for (String key : properties.keySet()) {
      if (RisoTreeUtil.isPNProperty(key)) {
        int size = ((int[]) properties.get(key)).length;
        int count = (int) histgram.getOrDefault(size, 0);
        histgram.put(size, count + 1);
        // logger.info(String.format("size: %d, count: %d", size, count));
      }
    }
  }

  public static void getSpatialEntityCount() {
    Util.println("read entity from " + entityPath);
    int spaCount = OwnMethods.GetSpatialEntityCount(entityPath);
    Util.println("spatial count: " + spaCount);
  }

  public static void getAverageDegree() {
    Util.println("read graph from " + graphPath);
    ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graphPath);
    int edgeCount = 0;
    for (ArrayList<Integer> neighbors : graph) {
      edgeCount += neighbors.size();
    }
    Util.println("node count: " + graph.size());
    Util.println("edge count: " + edgeCount);
    Util.println("average edge count: " + (double) edgeCount / graph.size());
  }

  public static void get1HopNeighborCount() {

  }

  public static void get2HopNeighborCount() {
    for (String dataset : dataset_a) {
      config.setDatasetName(dataset);
      initParameters();
      ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graphPath);
      int count = 0;
      int i = 0;
      for (ArrayList<Integer> list : graph) {
        Util.println(i);
        HashSet<Integer> hop2Neighbors = new HashSet<>();
        for (int neighborId : list) {
          for (int id : graph.get(neighborId))
            hop2Neighbors.add(id);
        }
        count += hop2Neighbors.size();
        i++;
      }
      Util.println("count: " + count);
    }

  }

}
