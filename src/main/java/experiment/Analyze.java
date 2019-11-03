package experiment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;
import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.strtree.STRtree;
import commons.Config;
import commons.Config.Datasets;
import commons.Config.system;
import commons.Entity;
import commons.GraphUtil;
import commons.MyRectangle;
import commons.Neo4jGraphUtility;
import commons.OwnMethods;
import commons.RTreeUtility;
import commons.ReadWriteUtil;
import commons.RisoTreeUtil;
import commons.SaveRectanglesAsImage;
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

  public static void degreeAvg(String graphPath, String entityPath, String outputPath) {
    Util.checkPathExist(graphPath);
    Util.checkPathExist(entityPath);
    ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graphPath);
    ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
    double[] res = degreeAvg(graph, entities);
    ReadWriteUtil.WriteFile(outputPath, true,
        String.format("%s\t%f\t%f\n", graphPath, res[0], res[1]));
  }

  /**
   * Get average degree for all nodes and spatial nodes.
   *
   * @param graph
   * @param entities
   * @return
   */
  public static double[] degreeAvg(ArrayList<ArrayList<Integer>> graph,
      ArrayList<Entity> entities) {
    int sumDegree = 0, spatialCount = 0, spatialSumDegree = 0;
    if (graph.size() != entities.size()) {
      throw new RuntimeException("graph size entities size mismatch!");
    }
    for (int i = 0; i < graph.size(); i++) {
      int degree = graph.get(i).size();
      sumDegree += degree;
      if (entities.get(i).IsSpatial) {
        spatialCount++;
        spatialSumDegree += degree;
      }
    }
    return new double[] {(double) sumDegree / graph.size(),
        (double) spatialSumDegree / spatialCount};
  }

  public static void degreeSD(String graphPath, String outputPath) {
    ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graphPath);
    double sd = degreeSD(graph);
    ReadWriteUtil.WriteFile(outputPath, true,
        String.format("%s\t%s\n", graphPath, String.valueOf(sd)));
  }

  public static double degreeSD(ArrayList<ArrayList<Integer>> graph) {
    int sum = 0;
    for (ArrayList<Integer> neighbors : graph) {
      sum += neighbors.size();
    }
    double avg = ((double) sum) / graph.size();
    double deviation = 0;
    for (ArrayList<Integer> neighbors : graph) {
      deviation += Math.pow(neighbors.size() - avg, 2);
    }
    return Math.sqrt(deviation / graph.size());
  }

  /**
   * Analyze the average overlap for each tree leaf node. Read all rectangles of leaf nodes and
   * store them in memory. Build an in-memory RTree to perform the self-join.
   *
   * @param dbPath
   * @param dataset
   * @param logPath
   * @throws Exception
   */
  public static void leafNodesOverlapAnalysisInMemory(String dbPath, String dataset, String logPath)
      throws Exception {
    GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(dbPath);
    List<MyRectangle> rectangles = new LinkedList<>();
    Transaction tx = service.beginTx();
    List<Node> leafNodes = RTreeUtility.getRTreeLeafLevelNodes(service, dataset);
    for (Node node : leafNodes) {
      rectangles.add(RTreeUtility.getNodeMBR(node));
    }
    tx.success();
    tx.close();
    Util.close(service);

    double total = 0.0;
    STRtree stRtree = OwnMethods.constructSTRTreeWithMyRectangles(rectangles);
    for (MyRectangle rectangle : rectangles) {
      Envelope searchRect =
          new Envelope(rectangle.min_x, rectangle.max_x, rectangle.min_y, rectangle.max_y);
      List<?> overlaps = stRtree.query(searchRect);
      for (Object object : overlaps) {
        MyRectangle overlapRect = (MyRectangle) object;
        if (overlapRect.equals(rectangle)) {
          continue;
        }
        total += rectangle.intersect(overlapRect).area();
      }
    }

    ReadWriteUtil.WriteFile(logPath, true,
        String.format("%s\t%f\t%f\n", dbPath, total, total / leafNodes.size()));
  }


  /**
   * Analyze the average overlap for each tree leaf node. For each leaf node, compute the overlapped
   * area with all other leaf nodes. In-disk is too slow. Replaced by a in memory version.
   *
   * @param dbPath
   * @param dataset
   * @param logPath
   * @throws Exception
   */
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
    ReadWriteUtil.WriteFile(logPath, true,
        String.format("%s\t%f\t%f\n", dbPath, total, total / leafNodes.size()));
  }

  /**
   * Compute the average area of leaf nodes.
   *
   * @param dbPath
   * @param dataset
   * @param logPath
   * @throws Exception
   */
  public static void leafNodesAvgArea(String dbPath, String dataset, String logPath)
      throws Exception {
    GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(dbPath);
    Transaction tx = service.beginTx();
    List<Node> leafNodes = RTreeUtility.getRTreeLeafLevelNodes(service, dataset);
    double total = 0.0;
    for (Node node : leafNodes) {
      MyRectangle rectangle = RTreeUtility.getNodeMBR(node);
      total += rectangle.area();
    }
    tx.success();
    tx.close();
    Util.close(service);
    ReadWriteUtil.WriteFile(logPath, true,
        String.format("%s\t%f\t%f\n", dbPath, total, total / leafNodes.size()));
  }

  public static void treeNodesAvgArea(String dbPath, String dataset, String logPath) {
    GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(dbPath);
    Transaction tx = service.beginTx();
    List<List<Node>> nodes = RTreeUtility.getRTreeNodesInLevels(service, dataset);
    int level = 0;
    int treeNodeCount = 0;
    ReadWriteUtil.WriteFile(logPath, true, dbPath + "\n");
    ReadWriteUtil.WriteFile(logPath, true, "level\ttotal\tcount\tavg\n");
    for (List<Node> nodeThisLevel : nodes) {
      double total = 0.0;
      for (Node node : nodeThisLevel) {
        MyRectangle rectangle = RTreeUtility.getNodeMBR(node);
        total += rectangle.area();
      }
      ReadWriteUtil.WriteFile(logPath, true, String.format("%d\t%f\t%d\t%f\n", level, total,
          nodeThisLevel.size(), total / nodeThisLevel.size()));
      treeNodeCount += nodeThisLevel.size();
      level++;
    }

    tx.success();
    tx.close();
    Util.close(service);
  }

  public static void getSpatialIndexSize(String dbPath, String dataset, String outputPath) {
    GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(dbPath);
    Transaction tx = service.beginTx();
    List<List<Node>> nodes = RTreeUtility.getRTreeNodesInLevels(service, dataset);
    int treeNodeCount = 0;
    for (List<Node> nodeThisLevel : nodes) {
      treeNodeCount += nodeThisLevel.size();
    }

    List<Node> leafNodes = nodes.get(nodes.size() - 1);
    for (Node node : leafNodes) {
      Iterable<Relationship> rels =
          node.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_REFERENCE);
      Iterator<Relationship> iterator = rels.iterator();
      while (iterator.hasNext()) {
        iterator.next();
        treeNodeCount++;
      }
    }

    int sizeInBytes = treeNodeCount * 5 * 4;
    double sizeInMBs = sizeInBytes / 1024 / 1024;
    ReadWriteUtil.WriteFile(outputPath, true,
        String.format("RTree size: %d bytes (%f MB)\n\n", sizeInBytes, sizeInMBs));

    tx.success();
    tx.close();
    Util.close(service);
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
    int nonEmptyPNCount = 0;
    for (long leafNodeId : pns.keySet()) {
      Map<String, int[]> pn = pns.get(leafNodeId);
      PNPropertyCount += pn.size();
      for (String pnKey : pn.keySet()) {
        int len = pn.get(pnKey).length;
        if (len != 0) {
          countNeighbors += pn.get(pnKey).length;
          nonEmptyPNCount++;
        }
      }
    }
    // assume each neighbor id is 32 bits.
    double PNNeighborSizeInMB = ((double) countNeighbors) * 32 / 4 / 1024 / 1024;
    // assume a key requires 32 bits.
    double PNKeySizeInMB = ((double) PNPropertyCount) * 32 / 4 / 1024 / 1024;
    double totalSize = PNNeighborSizeInMB + PNKeySizeInMB;
    ReadWriteUtil.WriteFile(logPath, true,
        String.format("%s\t%d\t%d\t%d\t%f\t%f\t%f\n", filepath, PNPropertyCount, nonEmptyPNCount,
            countNeighbors, PNKeySizeInMB, PNNeighborSizeInMB, totalSize));
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

  public static void visualizeLeafNodes(String dbPath, String dataset, String rectanglesExtend,
      String imageExtend, String outputPath) throws Exception {
    GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(dbPath);
    Transaction tx = service.beginTx();
    List<Node> nodes = RTreeUtility.getRTreeLeafLevelNodes(service, dataset);
    List<MyRectangle> rectangles = new ArrayList<>(nodes.size());
    for (Node node : nodes) {
      MyRectangle rectangle = RTreeUtility.getNodeMBR(node);
      rectangles.add(rectangle);
    }
    SaveRectanglesAsImage.saveAsJPG(rectangles, rectanglesExtend, imageExtend, outputPath);
    tx.success();
    tx.close();
    service.shutdown();
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
