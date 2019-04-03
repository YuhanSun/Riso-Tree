package experiment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import commons.Config;
import commons.Config.system;
import commons.Entity;
import commons.GraphUtil;
import commons.Labels.RTreeRel;
import commons.OwnMethods;
import commons.RTreeUtility;
import commons.Util;
import graph.Construct_RisoTree;

public class IndexSize {

  static Config config = new Config();
  static String dataset = config.getDatasetName();
  static String version = config.GetNeo4jVersion();
  static system systemName = config.getSystemName();
  static int MAX_HOPNUM = config.getMaxHopNum();
  static int nonspatial_label_count = config.getNonSpatialLabelCount();

  static String db_path, graphPath, entityPath, containIDPath, PNPath;
  static ArrayList<Integer> labels;// all labels in the graph

  public static void initializeParametersServer() {
    String dir = "/hdd2/data/ysun138/RisoTree/" + dataset;
    db_path = String.format("%s/%s/data/databases/graph.db", dir, version);
    graphPath = String.format("%s/graph.txt", dir);
    entityPath = String.format("%s/entity.txt", dir);
    containIDPath = String.format("%s/containID.txt", dir);
    PNPath = String.format("%s/PathNeighbors", dir);
    nonspatial_label_count = 100;
    if (nonspatial_label_count == 1)
      labels = new ArrayList<Integer>(Arrays.asList(0, 1));
    else {
      labels = new ArrayList<Integer>(1 + nonspatial_label_count);
      labels.add(1);
      for (int i = 0; i < nonspatial_label_count; i++)
        labels.add(i + 2);
    }
  }

  public static void initializeParameters() {
    switch (systemName) {
      case Ubuntu:
        db_path = String.format(
            "/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version,
            dataset);
        graphPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
        entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
        containIDPath =
            String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/containID.txt", dataset);
        PNPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/PathNeighbors", dataset);
        break;
      case Windows:
        db_path = String.format(
            "D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset,
            version, dataset);
        graphPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\graph.txt", dataset);
        entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
        containIDPath =
            String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\containID.txt", dataset);
        PNPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\PathNeighbors", dataset);
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

  public static void main(String[] args) {
    initializeParameters();
    // initializeParametersServer();

    // calculateIndexSize();
    // calculateValidIndexSize();
    // graphSize();

    // LoadDataNoOSM.main(null);
    // Construct_RisoTree.main(null);
    getRTreeSize();
    getOneHopIndexFromFile();
    getTwoHopIndexFromFile();
    // getOneHopIndexSize();
    // getTwoHopIndexSize();
  }

  public static void graphSize() {
    try {
      ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graphPath);
      Util.println(OwnMethods.getGraphSize(graph));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Get 2 hop index size from file
   */
  public static void getTwoHopIndexFromFile() {
    try {
      BufferedReader reader = new BufferedReader(new FileReader(new File(PNPath + "_2")));
      String line = null;
      line = reader.readLine();
      long nodeID = Long.parseLong(line);
      int count = 0; // number of integer

      while (true) {
        while ((line = reader.readLine()) != null) {
          if (line.matches("\\d+$") == false) {
            // OwnMethods.Print(line);
            String[] lineList = line.split(",", 2);
            String key = lineList[0];

            String content = lineList[1];
            String[] contentList = content.substring(1, content.length() - 1).split(", ");
            count += contentList.length;
            count++;
          } else
            break;
        }

        if (line == null)
          break;
        nodeID = Long.parseLong(line);
      }
      reader.close();
      Util.println("2 hop size:" + count * 4 + " bytes");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Read from database
   */
  public static void getTwoHopIndexSize() {
    try {
      int count = 0;// count of integers stored in each list
      Util.println(db_path);
      GraphDatabaseService databaseService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = databaseService.beginTx();
      HashMap<Long, ArrayList<Integer>> containIDMap =
          Construct_RisoTree.readContainIDMap(containIDPath);
      for (long key : containIDMap.keySet()) {
        Node node = databaseService.getNodeById(key);
        Map<String, Object> properties = node.getAllProperties();
        for (String propertyKey : properties.keySet())
          if (propertyKey.matches("PN_\\d+_\\d+_size$")) {
            count += (Integer) properties.get(propertyKey);
            count++;
          }
      }
      Util.println("Two hop index:" + count * 4 + " bytes");
      tx.success();
      tx.close();
      databaseService.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Get 1 hop index size from file
   */
  public static void getOneHopIndexFromFile() {
    try {
      String indexPath = PNPath + "_1";
      Util.println("read index from " + indexPath);
      BufferedReader reader = new BufferedReader(new FileReader(new File(indexPath)));
      String line = null;
      line = reader.readLine();
      long nodeID = Long.parseLong(line);
      int count = 0; // number of integer

      while (true) {
        while ((line = reader.readLine()) != null) {
          if (line.matches("\\d+$") == false) {
            // OwnMethods.Print(line);
            String[] lineList = line.split(",", 2);
            String key = lineList[0];

            String content = lineList[1];
            String[] contentList = content.substring(1, content.length() - 1).split(", ");
            count += contentList.length;
            count++;
          } else
            break;
        }

        if (line == null)
          break;
        nodeID = Long.parseLong(line);
      }
      reader.close();
      Util.println("1 hop size:" + count * 4 + " bytes");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Read from database
   */
  public static void getOneHopIndexSize() {
    try {
      int count = 0;// count of integers stored in each list
      Util.println(db_path);
      GraphDatabaseService databaseService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = databaseService.beginTx();
      HashMap<Long, ArrayList<Integer>> containIDMap =
          Construct_RisoTree.readContainIDMap(containIDPath);
      for (long key : containIDMap.keySet()) {
        Node node = databaseService.getNodeById(key);
        Map<String, Object> properties = node.getAllProperties();
        for (String propertyKey : properties.keySet())
          if (propertyKey.matches("PN_\\d+_size$")) {
            count += (Integer) properties.get(propertyKey);
            count++;
          }
      }
      Util.println("One hop index:" + count * 4 + " bytes");
      tx.success();
      tx.close();
      databaseService.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Use RTree non-leaf nodes count plus count of spatial entities
   */
  public static void getRTreeSize() {
    try {
      Util.println(db_path);
      GraphDatabaseService databaseService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = databaseService.beginTx();
      // long start = System.currentTimeMillis();
      Node rootNode = RTreeUtility.getRTreeRoot(databaseService, dataset);
      // OwnMethods.Print("start time :"+ (System.currentTimeMillis() - start));
      // start = System.currentTimeMillis();
      TraversalDescription td = databaseService.traversalDescription().depthFirst()
          .relationships(RTreeRel.RTREE_CHILD, Direction.OUTGOING);
      Traverser traverser = td.traverse(rootNode);
      ResourceIterable<Node> nodes = traverser.nodes();
      // OwnMethods.Print("get nodes time:" + (System.currentTimeMillis() - start));
      // start = System.currentTimeMillis();
      int count = 0; // count of number of nodes
      for (Node node : nodes)
        count++;
      // OwnMethods.Print("iterate nodes time:" + (System.currentTimeMillis() - start));
      // OwnMethods.Print("count:"+count);
      tx.success();
      tx.close();
      databaseService.shutdown();

      ArrayList<Entity> entities = OwnMethods.ReadEntity(entityPath);
      int spaCount = OwnMethods.GetSpatialEntityCount(entities);
      count += spaCount;
      Util.println("RTree size:" + count * 5 * 4 + " bytes");

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Calculate index size for NL
   */
  public static void calculateIndexSize() {
    try {
      int size = 0;
      int visitedNodeCount = 0;
      Util.println(db_path);
      GraphDatabaseService databaseService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = databaseService.beginTx();

      List<Node> nodes = RTreeUtility.getRTreeLeafLevelNodes(databaseService, dataset);
      for (Node node : nodes) {
        visitedNodeCount++;
        for (int i = 0; i < labels.size(); i++) {
          for (int j = 1; j <= MAX_HOPNUM; j++) {
            String NL_size_propertyname = String.format("NL_%d_%d_size", j, labels.get(i));
            if (node.hasProperty(NL_size_propertyname)) {
              int NL_size = (Integer) node.getProperty(NL_size_propertyname);
              size += NL_size;
            } else
              Util.println(String.format("Not has %s", NL_size_propertyname));
          }
        }
      }

      // Node root = OSM_Utility.getRTreeRoot(databaseService, dataset);
      // Queue<Node> queue = new LinkedList<Node>();
      // queue.add(root);
      // while ( queue.isEmpty() == false)
      // {
      // Node node = queue.poll();
      // visitedNodeCount++;
      // for ( int i = 2; i < labels.size(); i++)
      // {
      // for ( int j = 1; j <= max_hop_num; j++)
      // {
      // String NL_size_propertyname = String.format("NL_%d_%d_size", j, i);
      // if ( node.hasProperty(NL_size_propertyname))
      // {
      // int NL_size = (Integer) node.getProperty(NL_size_propertyname);
      // size += NL_size;
      // }
      // }
      // }
      // Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING,
      // Labels.RTreeRel.RTREE_CHILD);
      // for ( Relationship relationship : rels)
      // {
      // Node next = relationship.getEndNode();
      // queue.add(next);
      // }
      // }

      tx.success();
      tx.close();
      databaseService.shutdown();

      Util.println(String.format("visited node count: %d", visitedNodeCount));
      Util.println(String.format("index size:%d", size * 4));

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static void calculateValidIndexSize() {
    try {
      int size = 0;
      int visitedNodeCount = 0;
      Util.println(db_path);
      GraphDatabaseService databaseService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = databaseService.beginTx();

      List<Node> nodes = RTreeUtility.getRTreeLeafLevelNodes(databaseService, dataset);
      for (Node node : nodes) {
        visitedNodeCount++;

        int spa_count = (Integer) node.getProperty("count");
        for (int i = 2; i < labels.size(); i++) {
          for (int j = 1; j <= MAX_HOPNUM; j++) {
            String NL_size_propertyname = String.format("NL_%d_%d_size", j, i);
            if (node.hasProperty(NL_size_propertyname)) {
              int NL_size = (Integer) node.getProperty(NL_size_propertyname);
              if (NL_size < spa_count)
                size += NL_size;
            }
          }
        }
      }

      tx.success();
      tx.close();
      databaseService.shutdown();

      Util.println(String.format("visited node count: %d", visitedNodeCount));
      Util.println(String.format("index size:%d", size));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
