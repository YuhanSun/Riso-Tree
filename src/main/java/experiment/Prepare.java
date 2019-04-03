package experiment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.strtree.GeometryItemDistance;
import com.vividsolutions.jts.index.strtree.STRtree;
import commons.Config;
import commons.Entity;
import commons.Labels.OSMRelation;
import commons.Query_Graph;
import commons.RTreeUtility;
import commons.Util;
import commons.Config.system;
import commons.Labels.GraphLabel;
import commons.OwnMethods;

/**
 * convert {0,1} two labels graph to more selective graph with 10 or 100 labels for Gowalla dataset,
 * new queries for the new graph are generated here
 * 
 * @author yuhansun
 *
 */
public class Prepare {

  static Config config = new Config();
  static String dataset = config.getDatasetName();
  static String version = config.GetNeo4jVersion();
  static system systemName = config.getSystemName();
  static int MAX_HOPNUM = config.getMaxHopNum();
  static int nonspatial_label_count = config.getNonSpatialLabelCount();

  static String db_path;
  static String vertex_map_path;
  static String graph_path;
  static String entityPath;
  static String geo_id_map_path;
  static String label_list_path;
  static String graph_node_map_path;
  static String rtree_map_path;
  static String log_path;

  static int nonspatial_vertex_count;
  static int spatialVertexCount;
  static ArrayList<Integer> labels;// all labels in the graph

  static ArrayList<Entity> entities = null;

  // for patent_20
  // static double startSelectivity = 0.000001;
  // static double endSelectivity = 0.002;

  // for patent_80
  // static double startSelectivity = 0.00001;
  // static double endSelectivity = 0.02;

  // for go_uniprot_80
  static double startSelectivity = 0.000001;
  static double endSelectivity = 0.2;


  // for switching point
  // static double startSelectivity = 0.1;
  // static double endSelectivity = 0.2;

  static String queryDir;
  static String center_id_path;

  static String queryGraphDir;

  static void iniParametersServer() {
    String dir = "/hdd2/data/ysun138/RisoTree";
    String dataDir = dir + "/" + dataset;
    graph_path = dataDir + "/graph.txt";
    entityPath = dataDir + "/entity.txt";
    label_list_path = dataDir + "/label.txt";
    graph_node_map_path = dataDir + "/node_map_RTree.txt";
    queryDir = dir + "/query";
    queryGraphDir = String.format("%s/query_graph/%s/", queryDir, dataset);
    // center_id_path = String.format("%s/spa_predicate/%s/%s_centerids.txt", queryDir, dataset,
    // dataset);
    systemName = system.Ubuntu;
  }

  static void initParameters() {
    switch (systemName) {
      case Ubuntu:
        db_path = String.format(
            "/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version,
            dataset);
        vertex_map_path =
            String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map.txt", dataset);
        graph_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
        entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
        geo_id_map_path =
            String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/geom_osmid_map.txt", dataset);
        label_list_path =
            String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/label.txt", dataset);
        graph_node_map_path =
            String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map_RTree.txt", dataset);
        rtree_map_path =
            String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/rtree_map.txt", dataset);
        log_path = String.format("/mnt/hgfs/Experiment_Result/Riso-Tree/%s/set_label.log", dataset);
        queryDir = "/mnt/hgfs/Google_Drive/Projects/risotree/query";
        queryGraphDir = String.format("%s/query_graph/%s/", queryDir, dataset);
        center_id_path =
            String.format("%s/spa_predicate/%s/%s_centerids.txt", queryDir, dataset, dataset);
        break;
      case Windows:
        db_path = String.format(
            "D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset,
            version, dataset);
        vertex_map_path =
            String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\node_map.txt", dataset);
        graph_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\graph.txt", dataset);
        entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
        geo_id_map_path =
            String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\geom_osmid_map.txt", dataset);
        label_list_path =
            String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\label.txt", dataset);
        graph_node_map_path =
            String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\node_map_RTree.txt", dataset);
        rtree_map_path =
            String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\rtree_map.txt", dataset);
        log_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\set_label.log", dataset);
        queryDir = "D:\\Google_Drive\\Projects\\risotree\\query";
        queryGraphDir = String.format("%s\\query_graph\\%s\\", queryDir, dataset);
        center_id_path =
            String.format("%s\\spa_predicate\\%s\\%s_centerids.txt", queryDir, dataset, dataset);
      default:
        break;
    }

    entities = OwnMethods.ReadEntity(entityPath);
    spatialVertexCount = OwnMethods.GetSpatialEntityCount(entities);
    nonspatial_vertex_count = entities.size() - spatialVertexCount;

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
    // initParameters();
    iniParametersServer();

    // String oldDataset = "Patents_10_random_20";
    // modifyLayerName(oldDataset);
    // modifyLayerNameTest();

    // setNewLabel();
    // newLabelTest();

    generateRandomQueryGraph();
    // generateQueryRectangleCenterID();
    // generateQueryRectangleForSelectivity();


    // generateNewNLList();

  }

  public static void newNLListTest() {
    try {
      GraphDatabaseService dbService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = dbService.beginTx();

      Node node = dbService.getNodeById(3852215);

      tx.success();
      tx.close();
      dbService.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * generate new NLList for 10 new nonspatial labels NL_x_0_list will be utilized to generate new
   * list but not removed while NL_x_1_list will not be touched
   */
  public static void generateNewNLList() {
    try {
      GraphDatabaseService dbService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = dbService.beginTx();

      ArrayList<Integer> labelList = OwnMethods.readIntegerArray(label_list_path);
      HashMap<String, String> map_str = OwnMethods.ReadMap(rtree_map_path);
      HashMap<Integer, Long> rtreeMap = new HashMap<Integer, Long>();
      for (String key_str : map_str.keySet()) {
        String value_str = map_str.get(key_str);
        rtreeMap.put(Integer.parseInt(key_str), Long.parseLong(value_str));
      }
      // Node root = OSM_Utility.getRTreeRoot(dbService, dataset);
      // OwnMethods.Print(root.getId());
      // OwnMethods.Print(rtreeMap.get(0));

      for (int key : rtreeMap.keySet()) {
        Util.println(key);
        Node node = dbService.getNodeById(rtreeMap.get(key));
        for (int hop = 1; hop <= MAX_HOPNUM; hop++) {
          String oriNLListPropertyName = String.format("NL_%d_0_list", hop);
          int[] oriNLList = (int[]) node.getProperty(oriNLListPropertyName);

          HashMap<Integer, ArrayList<Integer>> newNLList =
              new HashMap<Integer, ArrayList<Integer>>();
          for (int label : labels)
            newNLList.put(label, new ArrayList<Integer>());
          for (int id : oriNLList) {
            int label = labelList.get(id);
            newNLList.get(label).add(id);
          }
          for (int label : labels) {
            String NLListPropertyName = String.format("NL_%d_%d_list", hop, label);
            String NLListSizePropertyName = String.format("NL_%d_%d_size", hop, label);

            ArrayList<Integer> NLListLabel = newNLList.get(label);
            int size = NLListLabel.size();
            if (size == 0) {
              node.setProperty(NLListSizePropertyName, 0);
              continue;
            } else {
              node.setProperty(NLListSizePropertyName, size);
              int[] NLListLabelArray = new int[size];
              for (int i = 0; i < size; i++)
                NLListLabelArray[i] = NLListLabel.get(i);
              node.setProperty(NLListPropertyName, NLListLabelArray);
            }
          }
        }
      }

      tx.success();
      tx.close();
      dbService.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void modifyLayerNameTest() {
    try {
      GraphDatabaseService dbService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = dbService.beginTx();

      Node root = RTreeUtility.getRTreeRoot(dbService, dataset);
      Util.println(root.getAllProperties());
      Node layer_node =
          root.getSingleRelationship(RTreeRelationshipTypes.RTREE_ROOT, Direction.INCOMING)
              .getStartNode();
      Util.println(layer_node.getAllProperties());

      if (layer_node.getSingleRelationship(OSMRelation.LAYERS, Direction.INCOMING) == null)
        Util.println("No OSM layer");
      else {
        Node osmLayerNode =
            layer_node.getSingleRelationship(OSMRelation.LAYERS, Direction.INCOMING).getStartNode();
        Util.println(String.format("osmlayernode : %s", osmLayerNode.getAllProperties()));
      }
      tx.success();
      tx.close();
      dbService.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Change the layername to new layername including layer property in RTree layer
   */
  public static void modifyLayerName(String oldDataset) {
    try {
      GraphDatabaseService dbService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = dbService.beginTx();

      Node root = RTreeUtility.getRTreeRoot(dbService, oldDataset);
      Util.println(root.getAllProperties());
      Node layer_node =
          root.getSingleRelationship(RTreeRelationshipTypes.RTREE_ROOT, Direction.INCOMING)
              .getStartNode();
      Util.println(layer_node.getAllProperties());
      layer_node.setProperty("layer", dataset);

      tx.success();
      tx.close();
      dbService.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Change the layername to new layername including layer property in RTree layer and name property
   * in OSM data
   */
  public static void modifyLayerNameOSM(String oldDataset) {
    try {
      GraphDatabaseService dbService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = dbService.beginTx();

      Node root = RTreeUtility.getRTreeRoot(dbService, oldDataset);
      Util.println(root.getAllProperties());
      Node layer_node =
          root.getSingleRelationship(RTreeRelationshipTypes.RTREE_ROOT, Direction.INCOMING)
              .getStartNode();
      Util.println(layer_node.getAllProperties());
      layer_node.setProperty("layer", dataset);

      Node osmLayerNode =
          layer_node.getSingleRelationship(OSMRelation.LAYERS, Direction.INCOMING).getStartNode();
      Util.println(String.format("osmlayernode : %s", osmLayerNode.getAllProperties()));
      osmLayerNode.setProperty("name", dataset);
      tx.success();
      tx.close();
      dbService.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Based on center id to generate the rectangles that can enclose a specific number of spatial
   * vertices. Their region area can be different but the same cardinality.
   */
  public static void generateQueryRectangleForSelectivity() {
    try {
      int experiment_count = 500;
      Util.println("Read entity from " + entityPath);
      if (entities == null)
        entities = OwnMethods.ReadEntity(entityPath);
      int spa_count = OwnMethods.GetSpatialEntityCount(entities);
      STRtree stRtree = OwnMethods.ConstructSTRee(entities);

      Util.println("Read center id from " + center_id_path);
      ArrayList<Integer> center_ids = OwnMethods.ReadCenterID(center_id_path);
      ArrayList<Integer> final_center_ids =
          OwnMethods.GetRandom_NoDuplicate(center_ids, experiment_count);

      double selectivity = startSelectivity;
      while (selectivity < endSelectivity) {
        int name_suffix = (int) (selectivity * spa_count);
        String output_path = null;

        switch (systemName) {
          case Ubuntu:
            output_path = String.format("%s/spa_predicate/%s/queryrect_%d.txt", queryDir, dataset,
                name_suffix);
            break;
          case Windows:
            output_path = String.format("%s\\spa_predicate\\%s\\queryrect_%d.txt", queryDir,
                dataset, name_suffix);
            break;
        }

        Util.println("Write query rectangles to " + output_path);
        String write_line = "";
        for (int id : final_center_ids) {
          double lon = entities.get(id).lon;
          double lat = entities.get(id).lat;
          GeometryFactory factory = new GeometryFactory();
          Point center = factory.createPoint(new Coordinate(lon, lat));
          Object[] result = stRtree.kNearestNeighbour(center.getEnvelopeInternal(),
              new GeometryFactory().toGeometry(center.getEnvelopeInternal()),
              new GeometryItemDistance(), name_suffix);
          double radius = 0.0;
          for (Object object : result) {
            // Entity entity = (Entity) object;
            // double dist = Utility.distance(lon, lat, entity.lon, entity.lat);
            Point point = (Point) object;
            double dist = Util.distance(lon, lat, point.getX(), point.getY());
            if (dist > radius)
              radius = dist;
          }
          // keep the rectangle area the same with the circle
          double a = Math.sqrt(Math.PI) * radius;
          double minx = center.getX() - a / 2;
          double miny = center.getY() - a / 2;
          double maxx = center.getX() + a / 2;
          double maxy = center.getY() + a / 2;

          write_line = String.format("%f\t%f\t%f\t%f\n", minx, miny, maxx, maxy);
          OwnMethods.WriteFile(output_path, true, write_line);
        }
        selectivity *= 10;
      }
    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * Generate a list of spatial vertices.
   */
  public static void generateQueryRectangleCenterID() {
    Util.println("read entity from " + entityPath);

    if (entities == null)
      entities = OwnMethods.ReadEntity(entityPath);

    OwnMethods.generateQueryRectangleCenterID(entities, center_id_path, 500);
  }

  /**
   * Generate the query graph for the experiment
   */
  public static void generateRandomQueryGraph() {
    if (!OwnMethods.pathExist(queryGraphDir)) {
      Util.println(queryGraphDir + " does not exist!");
      System.exit(-1);
    }

    Util.println("read graph from " + graph_path);
    ArrayList<ArrayList<Integer>> datagraph = OwnMethods.ReadGraph(graph_path);
    if (entities == null) {
      Util.println("read entity from " + entityPath);
      entities = OwnMethods.ReadEntity(entityPath);
    }

    Util.println("read label list from " + label_list_path);
    ArrayList<Integer> labels = OwnMethods.readIntegerArray(label_list_path);

    Util.println("data graph size: " + datagraph.size());
    Util.println("entity size: " + entities.size());
    Util.println("label size: " + labels.size());
    // for ( int node_count = 35; node_count <= 35; node_count+=5)
    // for ( int node_count = 5; node_count <= 15; node_count+=2)
    int node_count = 3;
    {
      int spa_pred_count = 1;
      String querygraph_path = String.format("%s%d.txt", queryGraphDir, node_count);

      ArrayList<Query_Graph> query_Graphs = new ArrayList<Query_Graph>(10);
      while (query_Graphs.size() != 10) {
        Query_Graph query_Graph =
            OwnMethods.GenerateRandomGraph(datagraph, labels, entities, node_count, spa_pred_count);

        query_Graph.iniStatistic();

        if (query_Graphs.size() == 0)
          query_Graphs.add(query_Graph);
        else
          for (int j = 0; j < query_Graphs.size(); j++) {
            Query_Graph queryGraph = query_Graphs.get(j);
            if (queryGraph.isIsomorphism(query_Graph) == true)
              break;
            if (j == query_Graphs.size() - 1) {
              Util.println(query_Graph.toString() + "\n");
              query_Graphs.add(query_Graph);
            }
          }
        Util.println(query_Graphs.size());
      }
      Util.println("output query graph to " + querygraph_path);
      Util.WriteQueryGraph(querygraph_path, query_Graphs);
    }
  }

  public static void newLabelTest() {
    try {
      GraphDatabaseService dbService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = dbService.beginTx();

      for (int i = 0; i < 10; i++) {
        int labelIndex = i + 2;
        Label label = DynamicLabel.label(String.format("GRAPH_%d", labelIndex));
        ResourceIterator<Node> nodes = dbService.findNodes(label);
        int count = 0;
        while (nodes.hasNext()) {
          count++;
          nodes.next();
        }
        Util.println(count);
      }

      ResourceIterator<Node> nodes = dbService.findNodes(GraphLabel.GRAPH_0);
      while (nodes.hasNext()) {
        Node node = nodes.next();
        int id = (Integer) node.getProperty("id");
        Iterable<Label> labels = node.getLabels();
        Util.println(String.format("%d, %s", id, labels.toString()));
      }

      tx.success();
      tx.close();
      dbService.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void setNewLabel() {
    try {
      ArrayList<Integer> label_list = OwnMethods.readIntegerArray(label_list_path);
      GraphDatabaseService dbService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = dbService.beginTx();
      ResourceIterator<Node> nodes = dbService.findNodes(GraphLabel.GRAPH_0);
      int index = 0;
      while (nodes.hasNext()) {
        Util.println(index);
        index++;
        Node node = nodes.next();
        // node.removeLabel(GraphLabel.GRAPH_0);
        int id = (Integer) node.getProperty("id");
        int labelIndex = label_list.get(id);
        String label_name = String.format("GRAPH_%d", labelIndex);
        Label label = DynamicLabel.label(label_name);
        node.addLabel(label);
      }

      tx.success();
      tx.close();
      dbService.shutdown();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
