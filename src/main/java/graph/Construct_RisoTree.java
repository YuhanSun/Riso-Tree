package graph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import commons.ArrayUtil;
import commons.Config;
import commons.Config.system;
import commons.GraphUtil;
import commons.Labels.RTreeRel;
import commons.Neo4jGraphUtility;
import commons.OwnMethods;
import commons.RTreeUtility;
import commons.ReadWriteUtil;
import commons.RisoTreeUtil;
import commons.Util;

/**
 * 
 * @author yuhansun Construct PN index and load it into db.
 *
 */
public class Construct_RisoTree {

  private static final Logger LOGGER = Logger.getLogger(Construct_RisoTree.class.getName());

  static Config config = new Config();
  static final String PNPrefix = Config.PNPrefix;
  static final String PNSizePrefix = Config.PNSizePrefix;

  static String dataset, version;
  static system systemName;
  static int MAX_HOPNUM;
  static int nonspatial_label_count;

  static String db_path;
  // static String vertex_map_path;
  static String graph_path;
  static String geo_id_map_path;
  static String label_list_path;
  static String graph_node_map_path;
  static String log_path;
  static String containIDPath;
  static String PNPath;

  static ArrayList<Integer> labels;// all labels in the graph

  private final static int PNLogCount = 3000;


  static void initParametersServer() {
    systemName = Config.system.Ubuntu;
    dataset = Config.Datasets.wikidata_100.name();
    String dir = "/hdd2/data/ysun138/RisoTree/" + dataset;
    MAX_HOPNUM = config.getMaxHopNum();
    db_path = dir + "/neo4j-community-3.1.1/data/databases/graph.db";
    graph_path = dir + "/graph.txt";
    label_list_path = dir + "/label.txt";
    graph_node_map_path = dir + "/node_map_RTree.txt";
    containIDPath = dir + "/containID.txt";
    PNPath = dir + "/PathNeighbors";
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

  static void initParameters() {
    systemName = config.getSystemName();
    version = config.GetNeo4jVersion();
    dataset = config.getDatasetName();
    MAX_HOPNUM = config.getMaxHopNum();
    nonspatial_label_count = config.getNonSpatialLabelCount();
    switch (systemName) {
      case Ubuntu:
        db_path = String.format(
            "/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version,
            dataset);
        // vertex_map_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map.txt",
        // dataset);
        graph_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
        // geo_id_map_path =
        // String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/geom_osmid_map.txt", dataset);
        label_list_path =
            String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/label.txt", dataset);
        // graph_node_map_path =
        // String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map.txt", dataset);
        log_path = String.format("/mnt/hgfs/Experiment_Result/Riso-Tree/%s/set_label.log", dataset);
        containIDPath =
            String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/containID.txt", dataset);
        PNPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/PathNeighbors", dataset);
        break;
      case Windows:
        db_path = String.format(
            "D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset,
            version, dataset);
        // vertex_map_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\node_map.txt",
        // dataset);
        graph_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\graph.txt", dataset);
        // geo_id_map_path =
        // String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/geom_osmid_map.txt", dataset);
        label_list_path =
            String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\label.txt", dataset);
        // graph_node_map_path =
        // String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\node_map.txt", dataset);
        log_path = String.format("/mnt/hgfs/Experiment_Result/Riso-Tree/%s/set_label.log", dataset);
        containIDPath =
            String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\containID.txt", dataset);
        PNPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\PathNeighbors", dataset);
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

  public Construct_RisoTree() {
    initParameters();
  }

  public Construct_RisoTree(Config config, boolean isServer) {
    this.config = config;
    if (isServer)
      initParametersServer();
    else
      initParameters();
  }

  public Construct_RisoTree(Config pConfig) {
    config = pConfig;
    initParameters();
  }

  public static void main(String[] args) {
    try {
      initParameters();
      // initParametersServer();

      generateContainSpatialID();
      constructPN();
      // LoadPN();
      // generatePNSize();

      // ConstructNL();
      // ClearNL();
      //
      // ArrayList<Integer> label_list = OwnMethods.readIntegerArray(label_list_path);
      // String neighborListPath =
      // "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/"+dataset+"/2hop_neighbor_spa.txt";
      // Construct(neighborListPath, 2, labels, label_list);

      // for the second hop
      // LoadLeafNodeNList();
      // MergeLeafNL();

      // generateNL_size();
      // NL_size_Check();
      // set_Spatial_Label();

      // set a lot of labels
      // set_NL_list_label(max_hop_num, labels);
      // set_NL_list_label_DeepestNonLeaf(max_hop_num, labels);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * Not used
   */
  // public static void generatePNSize()
  // {
  // try {
  // BufferedReader reader = new BufferedReader(new FileReader(new File(PNPath)));
  // Map<String, String> config = new HashMap<String, String>();
  // config.put("dbms.pagecache.memory", "20g");
  // BatchInserter inserter = BatchInserters.inserter(new File(db_path).getAbsoluteFile(), config);
  //
  // String line = null;
  // line = reader.readLine();
  // long nodeID = Long.parseLong(line);
  //
  // Map<String, Object> properties = new HashMap<String, Object>();
  // int count = 0;
  // while (true)
  // {
  // while ( (line = reader.readLine()) != null)
  // {
  // if (line.matches("\\d+$") == false)
  // {
  //// OwnMethods.Print(line);
  // String[] lineList = line.split(",", 2);
  // String key = lineList[0];
  //
  // String content = lineList[1];
  // String[] contentList = content.substring(1, content.length() - 1)
  // .split(", ");
  //
  // properties.put(key + "_size", contentList.length);
  // }
  // else break;
  // }
  // count++;
  // inserter.setNodeProperties(nodeID, properties);
  // if ( count == 500)
  // {
  // inserter.shutdown();
  // inserter = BatchInserters.inserter(new File(db_path).getAbsoluteFile(), config);
  // }
  //
  // if ( line == null)
  // break;
  // nodeID = Long.parseLong(line);
  // OwnMethods.Print(nodeID);
  // }
  // inserter.shutdown();
  // } catch (Exception e) {
  // e.printStackTrace();
  // System.exit(-1);
  // }
  // /**
  // * very slow
  // */
  //// try {
  //// HashMap<Long, ArrayList<Integer>> containIDMap = readContainIDMap(containIDPath);
  //// GraphDatabaseService dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new
  // File(db_path));
  //// Transaction tx = dbservice.beginTx();
  //// for ( long nodeID : containIDMap.keySet())
  //// {
  //// OwnMethods.Print(nodeID);
  //// Node node = dbservice.getNodeById(nodeID);
  //// Map<String, Object> properties = node.getAllProperties();
  //// Map<String, Object> addProperties = new HashMap<String, Object>();
  //// for ( String key : properties.keySet())
  //// if ( key.contains("PN") && key.contains("size") == false)
  //// {
  //// int[] pathNeighbors = (int[]) properties.get(key);
  //// addProperties.put(key + "_size", pathNeighbors.length);
  //// }
  //// for ( String key : addProperties.keySet())
  //// node.setProperty(key, addProperties.get(key));
  //// }
  //// tx.success();
  //// tx.close();
  //// dbservice.shutdown();
  //// } catch (Exception e) {
  //// e.printStackTrace();
  //// }
  // }

  /**
   * The file only contains two hop information
   */
  public static void LoadPN() {
    BufferedReader reader = null;
    try {
      int hop = 1;

      String indexPath = PNPath + "_" + hop;
      Util.println("read index from " + indexPath);
      Map<String, String> config = new HashMap<String, String>();
      config.put("dbms.pagecache.memory", "100g");
      if (!OwnMethods.pathExist(db_path))
        throw new Exception(db_path + " does not exist!");
      BatchInserter inserter = BatchInserters.inserter(new File(db_path).getAbsoluteFile(), config);

      reader = new BufferedReader(new FileReader(new File(indexPath)));
      String line = null;
      line = reader.readLine();
      long nodeID = Long.parseLong(line);

      while (true) {
        Map<String, Object> properties = inserter.getNodeProperties(nodeID);
        while ((line = reader.readLine()) != null) {
          if (line.matches("\\d+$") == false) {
            // OwnMethods.Print(line);
            String[] lineList = line.split(",", 2);
            String key = lineList[0];

            String content = lineList[1];
            String[] contentList = content.substring(1, content.length() - 1).split(", ");

            int[] value = new int[contentList.length];
            for (int i = 0; i < contentList.length; i++)
              value[i] = Integer.parseInt(contentList[i]);
            properties.put(key, value);
            properties.put(key + "_size", value.length);
          } else
            break;
        }
        inserter.setNodeProperties(nodeID, properties);

        if (line == null)
          break;
        nodeID = Long.parseLong(line);
        Util.println(nodeID);
      }
      inserter.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * Load specific hop of path neighbor from a given file. The path file will be
   * PNPathAndPreffix_hop.txt.
   *
   * @param PNPathAndPreffix
   * @param hop
   * @param db_path
   * @throws Exception
   */
  public static void wikiLoadPN(String PNPathAndPreffix, int hop, String db_path) throws Exception {
    String indexPath = PNPathAndPreffix + "_" + hop + ".txt";
    LOGGER.info("read index from " + indexPath);
    BufferedReader reader = new BufferedReader(new FileReader(new File(indexPath)));
    Map<String, String> config = new HashMap<String, String>();
    config.put("dbms.pagecache.memory", "100g");
    if (!Util.pathExist(db_path)) {
      Util.close(reader);
      throw new Exception(db_path + " does not exist!");
    }
    BatchInserter inserter = BatchInserters.inserter(new File(db_path).getAbsoluteFile(), config);

    String line = null;
    line = reader.readLine();
    long nodeID = Long.parseLong(line);
    int index = 0;

    while (true) {
      Map<String, Object> properties = inserter.getNodeProperties(nodeID);
      while ((line = reader.readLine()) != null) {
        if (line.matches("\\d+$") == false) { // path neighbor lines
          String[] lineList = line.split(",", 2);
          String key = lineList[0];

          String content = lineList[1];
          String[] contentList = content.substring(1, content.length() - 1).split(", ");

          int[] value = new int[contentList.length];
          for (int i = 0; i < contentList.length; i++)
            value[i] = Integer.parseInt(contentList[i]);
          properties.put(key, value);
          properties.put(RisoTreeUtil.getPNSizeName(key), value.length);
        } else {
          break;
        }
      }
      inserter.setNodeProperties(nodeID, properties);
      index++;
      if (index % PNLogCount == 0) {
        LOGGER.info("" + index);
      }

      if (line == null) {
        break;
      }
      nodeID = Long.parseLong(line);
    }
    Util.close(reader);
    Util.close(inserter);
  }

  /**
   * The file only contains two hop information
   */
  public void LoadPN(String PNPathAndPreffix, int MAX_HOPNUM, String db_path) {
    try {
      for (int hop = 2; hop <= MAX_HOPNUM; hop++) {
        String indexPath = PNPathAndPreffix + "_" + hop + ".txt";
        Util.println("read index from " + indexPath);
        BufferedReader reader = new BufferedReader(new FileReader(new File(indexPath)));
        Map<String, String> config = new HashMap<String, String>();
        config.put("dbms.pagecache.memory", "100g");
        if (!OwnMethods.pathExist(db_path))
          throw new Exception(db_path + " does not exist!");
        BatchInserter inserter =
            BatchInserters.inserter(new File(db_path).getAbsoluteFile(), config);

        String line = null;
        line = reader.readLine();
        long nodeID = Long.parseLong(line);

        while (true) {
          Map<String, Object> properties = inserter.getNodeProperties(nodeID);
          while ((line = reader.readLine()) != null) {
            if (line.matches("\\d+$") == false) {
              // OwnMethods.Print(line);
              String[] lineList = line.split(",", 2);
              String key = lineList[0];

              String content = lineList[1];
              String[] contentList = content.substring(1, content.length() - 1).split(", ");

              int[] value = new int[contentList.length];
              for (int i = 0; i < contentList.length; i++)
                value[i] = Integer.parseInt(contentList[i]);
              properties.put(key, value);
              properties.put(key + "_size", value.length);
            } else
              break;
          }
          inserter.setNodeProperties(nodeID, properties);

          if (line == null)
            break;
          nodeID = Long.parseLong(line);
          Util.println(nodeID);
        }
        reader.close();
        inserter.shutdown();
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * One hop is directly loaded into db. Two hops is constructed based on one hop loaded, and it is
   * written to file for future batchinsert. Such function has to be run separately for one hop and
   * two hops.
   * 
   * @throws Exception
   */
  public static ArrayList<Long> constructPNTime() throws Exception {
    ArrayList<Long> constructTime = new ArrayList<Long>();
    Util.println("read contain map from " + containIDPath);
    HashMap<Long, ArrayList<Integer>> containIDMap = readContainIDMap(containIDPath);
    Util.println("read graph from " + graph_path);
    ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graph_path);
    Util.println("read label list from " + label_list_path);
    ArrayList<Integer> labelList = OwnMethods.readIntegerArray(label_list_path);
    // FileWriter writer1 = new FileWriter(new File(PNPath + "_"+1));
    HashMap<Long, HashMap<String, ArrayList<Integer>>> PN =
        new HashMap<Long, HashMap<String, ArrayList<Integer>>>();

    for (long nodeID : containIDMap.keySet())
      PN.put(nodeID, new HashMap<String, ArrayList<Integer>>());

    // for one hop
    Util.println("construct 1 hop");
    long start = System.currentTimeMillis();
    for (long nodeId : containIDMap.keySet()) {
      // OwnMethods.Print(nodeId);
      // writer1.write(nodeId + "\n");
      TreeSet<Integer> pathNeighbors = new TreeSet<Integer>();
      for (int spaID : containIDMap.get(nodeId))
        for (int neighborID : graph.get(spaID))
          pathNeighbors.add(neighborID);

      HashMap<Integer, ArrayList<Integer>> pathLabelNeighbor =
          new HashMap<Integer, ArrayList<Integer>>();
      for (int neighborID : pathNeighbors) {
        int label = labelList.get(neighborID);
        if (pathLabelNeighbor.containsKey(label))
          pathLabelNeighbor.get(label).add(neighborID);
        else {
          ArrayList<Integer> arrayList = new ArrayList<Integer>();
          arrayList.add(neighborID);
          pathLabelNeighbor.put(label, arrayList);
        }
      }

      for (int pathLabel : pathLabelNeighbor.keySet()) {
        String propertyName = String.format("PN_%d", pathLabel);
        ArrayList<Integer> arrayList = pathLabelNeighbor.get(pathLabel);
        PN.get(nodeId).put(propertyName, arrayList);
        // writer1.write(String.format("%s,%s\n", propertyName, arrayList));
      }
    }
    long onehopTime = System.currentTimeMillis() - start;
    Util.println("one hop time:" + onehopTime);
    constructTime.add(onehopTime);
    // writer1.close();

    // more than one hop
    int hop = 2;
    while (hop <= MAX_HOPNUM)
    // int hop = 2;
    {
      Util.println(String.format("construct %d hop", hop));
      // FileWriter writer2 = new FileWriter(new File(PNPath+"_"+hop));
      // String regex = "PN";
      // for ( int i = 0; i < hop - 1; i++)
      // regex += "_\\d+";
      // regex += "$";
      start = System.currentTimeMillis();
      for (long nodeID : containIDMap.keySet()) {
        // OwnMethods.Print(nodeID);
        // writer2.write(nodeID + "\n");
        // Node node = dbservice.getNodeById(nodeID);
        // Map<String, Object> properties = node.getAllProperties();

        for (String key : PN.get(nodeID).keySet()) {
          ArrayList<Integer> curPathNeighbors = PN.get(nodeID).get(key);
          TreeSet<Integer> nextPathNeighbors = new TreeSet<Integer>();
          for (int curNeighborID : curPathNeighbors)
            for (int id : graph.get(curNeighborID))
              nextPathNeighbors.add(id);

          HashMap<Integer, ArrayList<Integer>> pathLabelNeighbors =
              new HashMap<Integer, ArrayList<Integer>>();
          for (int neighborID : nextPathNeighbors) {
            int label = labelList.get(neighborID);
            if (pathLabelNeighbors.containsKey(label))
              pathLabelNeighbors.get(label).add(neighborID);
            else {
              ArrayList<Integer> arrayList = new ArrayList<Integer>();
              arrayList.add(neighborID);
              pathLabelNeighbors.put(label, arrayList);
            }
          }
          //
          // for ( int pathEndLabel : pathLabelNeighbors.keySet())
          // {
          // // String propertyName = String.format("%s_%d", key, pathEndLabel);
          // ArrayList<Integer> arrayList = pathLabelNeighbors.get(pathEndLabel);
          // int [] array = new int[arrayList.size()];
          // for ( int i = 0; i < arrayList.size(); i++)
          // array[i] = arrayList.get(i);
          //
          // // node.setProperty(propertyName, array);
          // // writer2.write(String.format("%s,%s\n", propertyName, arrayList));
          // }
        }
      }
      long twohopTime = System.currentTimeMillis() - start;
      Util.println("two hop time:" + twohopTime);
      constructTime.add(twohopTime);
      // writer2.close();
      hop++;
    }
    return constructTime;
  }

  /**
   * One hop is directly loaded into db. Two hops is constructed based on one hop loaded, and it is
   * written to file for future batchinsert. Such function has to be run separately for one hop and
   * two hops.
   */
  public static void constructPN() {
    try {
      Util.println("read contain map from " + containIDPath);
      HashMap<Long, ArrayList<Integer>> containIDMap = readContainIDMap(containIDPath);
      Util.println("read graph from " + graph_path);
      ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graph_path);
      Util.println("read label list from " + label_list_path);
      ArrayList<Integer> labelList = OwnMethods.readIntegerArray(label_list_path);
      GraphDatabaseService dbservice = Neo4jGraphUtility.getDatabaseService(db_path);
      FileWriter writer1 = new FileWriter(new File(PNPath + "_" + 1));
      // for one hop
      Transaction tx = dbservice.beginTx();
      for (long nodeId : containIDMap.keySet()) {
        writer1.write(nodeId + "\n");
        TreeSet<Integer> pathNeighbors = new TreeSet<Integer>();
        for (int spaID : containIDMap.get(nodeId)) {
          for (int neighborID : graph.get(spaID)) {
            pathNeighbors.add(neighborID);
          }
        }

        HashMap<Integer, ArrayList<Integer>> pathLabelNeighbor =
            new HashMap<Integer, ArrayList<Integer>>();
        for (int neighborID : pathNeighbors) {
          int label = labelList.get(neighborID);
          if (pathLabelNeighbor.containsKey(label))
            pathLabelNeighbor.get(label).add(neighborID);
          else {
            ArrayList<Integer> arrayList = new ArrayList<Integer>();
            arrayList.add(neighborID);
            pathLabelNeighbor.put(label, arrayList);
          }
        }

        for (int pathLabel : pathLabelNeighbor.keySet()) {
          String propertyName = String.format("PN_%d", pathLabel);
          ArrayList<Integer> arrayList = pathLabelNeighbor.get(pathLabel);
          int[] array = new int[arrayList.size()];
          for (int i = 0; i < arrayList.size(); i++)
            array[i] = arrayList.get(i);

          dbservice.getNodeById(nodeId).setProperty(propertyName, array);
          dbservice.getNodeById(nodeId).setProperty(propertyName + "_size", array.length);
          writer1.write(String.format("%s,%s\n", propertyName, arrayList));
        }
      }
      writer1.close();
      tx.success();
      tx.close();

      // more than one hop
      Transaction tx2 = dbservice.beginTx();

      int hop = 2;
      while (hop <= MAX_HOPNUM)
      // int hop = 2;
      {
        FileWriter writer2 = new FileWriter(new File(PNPath + "_" + hop));
        String regex = "PN";
        for (int i = 0; i < hop - 1; i++)
          regex += "_\\d+";
        regex += "$";

        for (long nodeID : containIDMap.keySet()) {
          Util.println(nodeID);
          writer2.write(nodeID + "\n");
          Node node = dbservice.getNodeById(nodeID);
          Map<String, Object> properties = node.getAllProperties();

          for (String key : properties.keySet()) {
            if (key.matches(regex)) {
              int[] curPathNeighbors = (int[]) properties.get(key);
              TreeSet<Integer> nextPathNeighbors = new TreeSet<Integer>();
              for (int curNeighborID : curPathNeighbors)
                for (int id : graph.get(curNeighborID))
                  nextPathNeighbors.add(id);

              HashMap<Integer, ArrayList<Integer>> pathLabelNeighbors =
                  new HashMap<Integer, ArrayList<Integer>>();
              for (int neighborID : nextPathNeighbors) {
                int label = labelList.get(neighborID);
                if (pathLabelNeighbors.containsKey(label))
                  pathLabelNeighbors.get(label).add(neighborID);
                else {
                  ArrayList<Integer> arrayList = new ArrayList<Integer>();
                  arrayList.add(neighborID);
                  pathLabelNeighbors.put(label, arrayList);
                }
              }

              for (int pathEndLabel : pathLabelNeighbors.keySet()) {
                String propertyName = String.format("%s_%d", key, pathEndLabel);
                ArrayList<Integer> arrayList = pathLabelNeighbors.get(pathEndLabel);
                int[] array = new int[arrayList.size()];
                for (int i = 0; i < arrayList.size(); i++)
                  array[i] = arrayList.get(i);

                // node.setProperty(propertyName, array);
                writer2.write(String.format("%s,%s\n", propertyName, arrayList));
              }
            }
          }
        }
        writer2.close();
        hop++;
      }
      tx2.success();
      tx2.close();

      dbservice.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static void wikiConstructPNSingleHop(String containIDPath, String db_path,
      String graph_path, String label_list_path, String labelStringMapPath, int hop,
      String PNPathAndPreffix) throws Exception {
    HashMap<Long, ArrayList<Integer>> containIDMap = readContainIDMap(containIDPath);
    ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graph_path);
    ArrayList<ArrayList<Integer>> label_list = GraphUtil.ReadGraph(label_list_path);
    String[] labelStringMap = ReadWriteUtil.readMapAsArray(labelStringMapPath, 50000000);

    long constructTime = 0;
    if (hop == 0) {
      constructTime =
          wikiConstructPNTimeZeroHop(containIDMap, labelStringMap, label_list, PNPathAndPreffix);
    } else if (hop > 0) {
      GraphDatabaseService dbservice = Neo4jGraphUtility.getDatabaseService(db_path);
      constructTime = wikiConstructPNTimeMultiHop(containIDMap, labelStringMap, dbservice, graph,
          label_list, hop, PNPathAndPreffix);
      Util.close(dbservice);
    } else {
      throw new Exception(String.format("hop = %d is invalid!", hop));
    }

    Util.println("construction time: " + constructTime);
  }

  public static long wikiConstructPNTimeZeroHop(HashMap<Long, ArrayList<Integer>> containIDMap,
      String[] labelStringMap, ArrayList<ArrayList<Integer>> label_list, String PNPathAndPreffix)
      throws Exception {
    // 1-hop
    LOGGER.info("construct 0-hop");
    long start = System.currentTimeMillis();
    FileWriter writer1 = new FileWriter(new File(getPNFilePath(PNPathAndPreffix, 0)));
    int index = 0;
    for (long nodeId : containIDMap.keySet()) {
      index++;
      if (index % PNLogCount == 0) {
        LOGGER.info("" + index);
      }
      writer1.write(nodeId + "\n");
      // 0-hop path neighbors are spatial objects themselves.
      TreeSet<Integer> pathNeighbors = new TreeSet<>(containIDMap.get(nodeId));
      HashMap<Integer, ArrayList<Integer>> pathLabelNeighbor =
          dividedByLabels(pathNeighbors, label_list);
      outPathLabelNeighbors(pathLabelNeighbor, PNPrefix, writer1, labelStringMap);
    }
    Util.close(writer1);
    return System.currentTimeMillis() - start;
  }

  private static long wikiConstructPNTimeMultiHop(HashMap<Long, ArrayList<Integer>> containIDMap,
      String[] labelStringMap, GraphDatabaseService dbservice, ArrayList<ArrayList<Integer>> graph,
      ArrayList<ArrayList<Integer>> label_list, int hop, String PNPathAndPreffix) throws Exception {
    // more than one hop
    Transaction tx2 = dbservice.beginTx();
    LOGGER.info(String.format("construct %d hop", hop));
    FileWriter writer2 = new FileWriter(new File(getPNFilePath(PNPathAndPreffix, hop)));

    int index = 0;
    long start = System.currentTimeMillis();
    for (long nodeID : containIDMap.keySet()) {
      index++;
      if (index % PNLogCount == 0) {
        LOGGER.info("" + index);
      }

      writer2.write(nodeID + "\n");
      Node node = dbservice.getNodeById(nodeID);
      constructPNOutputForNode(node, labelStringMap, graph, label_list, hop, writer2);

    }
    Util.close(writer2);
    tx2.success();
    tx2.close();
    return System.currentTimeMillis() - start;
  }

  private static void constructPNOutputForNode(Node node, String[] labelStringMap,
      ArrayList<ArrayList<Integer>> graph, ArrayList<ArrayList<Integer>> label_list, int hop,
      FileWriter writer2) throws Exception {
    Map<String, Object> properties = node.getAllProperties();
    for (String key : properties.keySet()) {
      if (RisoTreeUtil.isPNProperty(key) && StringUtils.countMatches(key, '_') == (hop)) {
        int[] curPathNeighbors = (int[]) properties.get(key);
        TreeSet<Integer> nextPathNeighbors = getNextPathNeighborsInSet(curPathNeighbors, graph);
        HashMap<Integer, ArrayList<Integer>> pathLabelNeighbors =
            dividedByLabels(nextPathNeighbors, label_list);
        outPathLabelNeighbors(pathLabelNeighbors, key, writer2, labelStringMap);
      }
    }
  }

  private static void outPathLabelNeighbors(HashMap<Integer, ArrayList<Integer>> pathLabelNeighbors,
      String key, FileWriter writer2, String[] labelStringMap) throws Exception {
    for (int pathEndLabel : pathLabelNeighbors.keySet()) {
      String propertyName = getAttachName(key, pathEndLabel, labelStringMap);
      ArrayList<Integer> arrayList = pathLabelNeighbors.get(pathEndLabel);
      writer2.write(String.format("%s,%s\n", propertyName, arrayList));
    }
  }

  // private static long wikiConstructPNTimeOneHop(HashMap<Long, ArrayList<Integer>> containIDMap,
  // String[] labelStringMap, GraphDatabaseService dbservice, ArrayList<ArrayList<Integer>> graph,
  // ArrayList<ArrayList<Integer>> label_list, String PNPathAndPreffix) throws Exception {
  // // 1-hop
  // LOGGER.info("construct 1-hop");
  // long start = System.currentTimeMillis();
  // FileWriter writer1 = new FileWriter(new File(getPNFilePath(PNPathAndPreffix, 1)));
  // Transaction tx = dbservice.beginTx();
  // int index = 0;
  // for (long nodeId : containIDMap.keySet()) {
  // index++;
  // if (index % 2000 == 0) {
  // LOGGER.info("" + index);
  // }
  // writer1.write(nodeId + "\n");
  // TreeSet<Integer> pathNeighbors = new TreeSet<>();
  // for (int spaID : containIDMap.get(nodeId)) {
  // for (int neighborID : graph.get(spaID)) {
  // pathNeighbors.add(neighborID);
  // }
  // }
  //
  // HashMap<Integer, ArrayList<Integer>> pathLabelNeighbor =
  // dividedByLabels(pathNeighbors, label_list);
  //
  // for (int pathLabel : pathLabelNeighbor.keySet()) {
  // String labelStr = labelStringMap[pathLabel];
  // String propertyName = String.format("%s_%s", PNPrefix, labelStr);
  // ArrayList<Integer> arrayList = pathLabelNeighbor.get(pathLabel);
  // writer1.write(String.format("%s,%s\n", propertyName, arrayList));
  // }
  // }
  // Util.close(writer1);
  // tx.success();
  // tx.close();
  // return System.currentTimeMillis() - start;
  // }

  public static void wikiConstructPNTime(String containIDPath, String db_path, String graph_path,
      String label_list_path, String labelStringMapPath, int MAX_HOPNUM, String PNPathAndPreffix) {
    try {
      HashMap<Long, ArrayList<Integer>> containIDMap = readContainIDMap(containIDPath);
      ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graph_path);
      ArrayList<ArrayList<Integer>> label_list = GraphUtil.ReadGraph(label_list_path);
      GraphDatabaseService dbservice = Neo4jGraphUtility.getDatabaseService(db_path);
      String[] labelStringMap = ReadWriteUtil.readMapAsArray(labelStringMapPath, 50000000);

      HashMap<Integer, Long> constructTime = wikiConstructPNTime(containIDMap, labelStringMap,
          dbservice, graph, label_list, MAX_HOPNUM, PNPathAndPreffix);
      Util.println(constructTime);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static HashMap<Integer, Long> wikiConstructPNTime(
      HashMap<Long, ArrayList<Integer>> containIDMap, String[] labelStringMap,
      GraphDatabaseService dbservice, ArrayList<ArrayList<Integer>> graph,
      ArrayList<ArrayList<Integer>> label_list, int MAX_HOPNUM, String PNPathAndPreffix)
      throws Exception {
    HashMap<Integer, Long> constructTime = new HashMap<>();

    // 1-hop
    LOGGER.info("construct 1-hop");
    FileWriter writer1 = new FileWriter(new File(getPNFilePath(PNPathAndPreffix, 1)));
    Transaction tx = dbservice.beginTx();
    int index = 0;
    for (long nodeId : containIDMap.keySet()) {
      index++;
      if (index % 2000 == 0) {
        LOGGER.info("" + index);
      }
      writer1.write(nodeId + "\n");
      TreeSet<Integer> pathNeighbors = new TreeSet<>();
      for (int spaID : containIDMap.get(nodeId)) {
        for (int neighborID : graph.get(spaID)) {
          pathNeighbors.add(neighborID);
        }
      }

      HashMap<Integer, ArrayList<Integer>> pathLabelNeighbor =
          dividedByLabels(pathNeighbors, label_list);

      Node node = dbservice.getNodeById(nodeId);
      for (int pathLabel : pathLabelNeighbor.keySet()) {
        String labelStr = labelStringMap[pathLabel];
        String propertyName = String.format("%s_%s", PNPrefix, labelStr);
        ArrayList<Integer> arrayList = pathLabelNeighbor.get(pathLabel);
        int[] array = ArrayUtil.listToArrayInt(arrayList);

        node.setProperty(propertyName, array);
        node.setProperty(propertyName + "_size", array.length);
        writer1.write(String.format("%s,%s\n", propertyName, arrayList));
      }
    }
    writer1.close();
    tx.success();
    tx.close();

    // more than one hop
    Transaction tx2 = dbservice.beginTx();
    int hop = 2;
    while (hop <= MAX_HOPNUM) {
      Util.println(String.format("construct %d hop", hop));
      FileWriter writer2 = new FileWriter(new File(getPNFilePath(PNPathAndPreffix, hop)));


      long curHopTime = 0;
      index = 0;
      for (long nodeID : containIDMap.keySet()) {
        index++;
        if (index % 10000 == 0) {
          LOGGER.info("" + index);
        }
        writer2.write(nodeID + "\n");
        long start = System.currentTimeMillis();
        Node node = dbservice.getNodeById(nodeID);
        Map<String, Object> properties = node.getAllProperties();

        for (String key : properties.keySet()) {
          if (RisoTreeUtil.isPNProperty(key) && StringUtils.countMatches(key, '_') == (hop - 1)) {
            int[] curPathNeighbors = (int[]) properties.get(key);
            TreeSet<Integer> nextPathNeighbors = getNextPathNeighborsInSet(curPathNeighbors, graph);

            HashMap<Integer, ArrayList<Integer>> pathLabelNeighbors =
                dividedByLabels(nextPathNeighbors, label_list);

            curHopTime += System.currentTimeMillis() - start;

            for (int pathEndLabel : pathLabelNeighbors.keySet()) {
              String propertyName = getAttachName(key, pathEndLabel, labelStringMap);
              ArrayList<Integer> arrayList = pathLabelNeighbors.get(pathEndLabel);
              int[] array = ArrayUtil.listToArrayInt(arrayList);
              node.setProperty(propertyName, array);
              node.setProperty(propertyName + "_size", array.length);

              writer2.write(String.format("%s,%s\n", propertyName, arrayList));
            }

            start = System.currentTimeMillis();
          }
        }
      }
      writer2.close();
      constructTime.put(hop, curHopTime);
      hop++;
    }
    tx2.success();
    tx2.close();
    dbservice.shutdown();
    return constructTime;
  }

  private static String getAttachName(String key, int pathEndLabel, String[] labelStringMap) {
    String labelStr = labelStringMap[pathEndLabel];
    return String.format("%s_%s", key, labelStr);

  }

  private static HashMap<Integer, ArrayList<Integer>> dividedByLabels(
      TreeSet<Integer> nextPathNeighbors, ArrayList<ArrayList<Integer>> label_list) {
    HashMap<Integer, ArrayList<Integer>> pathLabelNeighbors =
        new HashMap<Integer, ArrayList<Integer>>();
    for (int neighborID : nextPathNeighbors) {
      for (int label : label_list.get(neighborID)) {
        if (pathLabelNeighbors.containsKey(label))
          pathLabelNeighbors.get(label).add(neighborID);
        else {
          ArrayList<Integer> arrayList = new ArrayList<Integer>();
          arrayList.add(neighborID);
          pathLabelNeighbors.put(label, arrayList);
        }
      }
    }
    return pathLabelNeighbors;
  }

  /**
   * Get the neighbors for the next hop from a set of vertexes.
   *
   * @param curPathNeighbors
   * @param graph
   * @return
   */
  private static TreeSet<Integer> getNextPathNeighborsInSet(int[] curPathNeighbors,
      ArrayList<ArrayList<Integer>> graph) {
    TreeSet<Integer> nextPathNeighbors = new TreeSet<>();
    for (int curNeighborID : curPathNeighbors)
      for (int id : graph.get(curNeighborID))
        nextPathNeighbors.add(id);
    return nextPathNeighbors;
  }

  /**
   * One hop is directly loaded into db. Two hops is constructed based on one hop loaded, and it is
   * written to file for future batchinsert. Such function has to be run separately for one hop and
   * two hops.
   */
  public void constructPNTime(String containIDPath, String db_path, String graph_path,
      String label_list_path, int MAX_HOPNUM, String PNPathAndPreffix) {
    try {
      HashMap<Integer, Long> constructTime = new HashMap<>();
      Util.println("read contain map from " + containIDPath);
      HashMap<Long, ArrayList<Integer>> containIDMap = readContainIDMap(containIDPath);
      Util.println("read graph from " + graph_path);
      ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graph_path);
      Util.println("read label list from " + label_list_path);
      ArrayList<Integer> labelList = OwnMethods.readIntegerArray(label_list_path);
      GraphDatabaseService dbservice = Neo4jGraphUtility.getDatabaseService(db_path);
      // for one hop omit because already constructed in RTree construction phase. Refer to
      // ConstructPN for source code.

      // more than one hop
      Transaction tx2 = dbservice.beginTx();
      int hop = 2;
      while (hop <= MAX_HOPNUM) {
        Util.println(String.format("construct %d hop", hop));
        FileWriter writer2 = new FileWriter(new File(PNPathAndPreffix + "_" + hop + ".txt"));
        String regex = "PN";
        for (int i = 0; i < hop - 1; i++)
          regex += "_\\d+";
        regex += "$";

        long curHopTime = 0;
        for (long nodeID : containIDMap.keySet()) {
          writer2.write(nodeID + "\n");
          long start = System.currentTimeMillis();
          Node node = dbservice.getNodeById(nodeID);
          Map<String, Object> properties = node.getAllProperties();

          for (String key : properties.keySet()) {
            if (key.matches(regex)) {
              int[] curPathNeighbors = (int[]) properties.get(key);
              TreeSet<Integer> nextPathNeighbors = new TreeSet<Integer>();
              for (int curNeighborID : curPathNeighbors)
                for (int id : graph.get(curNeighborID))
                  nextPathNeighbors.add(id);

              HashMap<Integer, ArrayList<Integer>> pathLabelNeighbors =
                  new HashMap<Integer, ArrayList<Integer>>();
              for (int neighborID : nextPathNeighbors) {
                int label = labelList.get(neighborID);
                if (pathLabelNeighbors.containsKey(label))
                  pathLabelNeighbors.get(label).add(neighborID);
                else {
                  ArrayList<Integer> arrayList = new ArrayList<Integer>();
                  arrayList.add(neighborID);
                  pathLabelNeighbors.put(label, arrayList);
                }
              }

              curHopTime += System.currentTimeMillis() - start;

              for (int pathEndLabel : pathLabelNeighbors.keySet()) {
                String propertyName = String.format("%s_%d", key, pathEndLabel);
                ArrayList<Integer> arrayList = pathLabelNeighbors.get(pathEndLabel);
                int[] array = new int[arrayList.size()];
                for (int i = 0; i < arrayList.size(); i++)
                  array[i] = arrayList.get(i);

                writer2.write(String.format("%s,%s\n", propertyName, arrayList));
              }

              start = System.currentTimeMillis();
            }
          }
        }
        writer2.close();
        constructTime.put(hop, curHopTime);
        hop++;
      }
      tx2.success();
      tx2.close();
      dbservice.shutdown();
      Util.println(constructTime);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static String getPNFilePath(String PNPathAndPreffix, int hop) {
    return String.format("%s_%d.txt", PNPathAndPreffix, hop);
  }

  public static void wikiGenerateContainSpatialID(String db_path, String dataset,
      String containIDPath) {
    try {
      GraphDatabaseService dbservice =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      HashMap<Long, TreeSet<Long>> containIDMap = new HashMap<>();
      Transaction tx = dbservice.beginTx();
      List<Node> nodes = RTreeUtility.getRTreeLeafLevelNodes(dbservice, dataset);
      for (Node node : nodes) {
        long parentID = node.getId();
        Iterable<Relationship> rels =
            node.getRelationships(RTreeRel.RTREE_REFERENCE, Direction.OUTGOING);
        TreeSet<Long> containIDs = new TreeSet<>();
        for (Relationship relationship : rels) {
          // Use the id() rather .id to identify a node because wikidata .id is QId.
          long childID = relationship.getEndNode().getId();
          containIDs.add(childID);
        }
        containIDMap.put(parentID, containIDs);
      }
      tx.success();
      tx.close();
      dbservice.shutdown();

      FileWriter writer = new FileWriter(new File(containIDPath));
      for (long id : containIDMap.keySet())
        writer.write(String.format("%d,%s\n", id, containIDMap.get(id).toString()));
      writer.close();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Generate the map of neo4j RisoTree leaf level id and spatial objects graph id.
   */
  public static void generateContainSpatialID() {
    try {
      GraphDatabaseService dbservice = Neo4jGraphUtility.getDatabaseService(db_path);
      HashMap<Long, TreeSet<Integer>> containIDMap = new HashMap<Long, TreeSet<Integer>>();
      Transaction tx = dbservice.beginTx();
      List<Node> nodes = RTreeUtility.getRTreeLeafLevelNodes(dbservice, dataset);
      for (Node node : nodes) {
        long parentID = node.getId();
        Iterable<Relationship> rels =
            node.getRelationships(RTreeRel.RTREE_REFERENCE, Direction.OUTGOING);
        TreeSet<Integer> containIDs = new TreeSet<Integer>();
        for (Relationship relationship : rels) {
          int childID = (Integer) relationship.getEndNode().getProperty("id");
          containIDs.add(childID);
        }
        containIDMap.put(parentID, containIDs);
      }
      tx.success();
      tx.close();
      dbservice.shutdown();

      FileWriter writer = new FileWriter(new File(containIDPath));
      for (long id : containIDMap.keySet())
        writer.write(String.format("%d,%s\n", id, containIDMap.get(id).toString()));
      writer.close();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Generate the map of neo4j RisoTree leaf level id and spatial objects graph id.
   */
  public void generateContainSpatialID(String db_path, String dataset, String containIDPath) {
    try {
      GraphDatabaseService dbservice =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      HashMap<Long, TreeSet<Integer>> containIDMap = new HashMap<Long, TreeSet<Integer>>();
      Transaction tx = dbservice.beginTx();
      List<Node> nodes = RTreeUtility.getRTreeLeafLevelNodes(dbservice, dataset);
      for (Node node : nodes) {
        long parentID = node.getId();
        Iterable<Relationship> rels =
            node.getRelationships(RTreeRel.RTREE_REFERENCE, Direction.OUTGOING);
        TreeSet<Integer> containIDs = new TreeSet<Integer>();
        for (Relationship relationship : rels) {
          int childID = (Integer) relationship.getEndNode().getProperty("id");
          containIDs.add(childID);
        }
        containIDMap.put(parentID, containIDs);
      }
      tx.success();
      tx.close();
      dbservice.shutdown();

      FileWriter writer = new FileWriter(new File(containIDPath));
      for (long id : containIDMap.keySet())
        writer.write(String.format("%d,%s\n", id, containIDMap.get(id).toString()));
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  // public static void set_NL_list_label_DeepestNonLeaf(int max_hop_num, ArrayList<Integer> labels)
  // {
  // HashMap<String, String> map = OwnMethods.ReadMap(graph_node_map_path);
  // HashMap<Integer, Integer> graph_node_map = new HashMap<Integer, Integer>();
  // for (String key : map.keySet()) {
  // String value = map.get(key);
  // int graph_id = Integer.parseInt(key);
  // int pos_id = Integer.parseInt(value);
  // graph_node_map.put(graph_id, pos_id);
  // }
  //
  // GraphDatabaseService dbservice =
  // new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
  // try {
  // Transaction tx = dbservice.beginTx();
  //
  // List<Node> nodes = RTreeUtility.getRTreeLeafLevelNodes(dbservice, dataset);
  // Queue<Long> queue = new LinkedList<Long>();
  // for (Node node : nodes)
  // queue.add(node.getId());
  // tx.success();
  // tx.close();
  //
  // int index = 0, count = 0, commit_time = 0;
  // tx = dbservice.beginTx();
  // while (queue.isEmpty() == false) {
  // Utility.print("index:" + index);
  // Utility.print("count:" + count);
  // Long pos = queue.poll();
  // Node node = dbservice.getNodeById(pos);
  // if (count < 4250) {
  // count++;
  // continue;
  // }
  //
  // int rtree_id = (Integer) node.getProperty("rtree_id");
  // for (int i = 1; i <= max_hop_num; i++) {
  // for (int label : labels) {
  // String property_name = String.format("NL_%d_%d_list", i, label);
  // if (node.hasProperty(property_name)) {
  // int[] NL_list_label = (int[]) node.getProperty(property_name);
  // String label_name = String.format("%d_NL_%d_%d_list", rtree_id, i, label);
  // // OwnMethods.Print(label_name);
  // Label sub_label = DynamicLabel.label(label_name);
  // for (int id : NL_list_label) {
  // int pos_id = graph_node_map.get(id);
  // Node graph_node = dbservice.getNodeById(pos_id);
  // graph_node.addLabel(sub_label);
  // // OwnMethods.Print(graph_node.getAllProperties());
  // // break;
  // }
  // }
  // }
  // }
  // if (index == 19) {
  // long start = System.currentTimeMillis();
  // tx.success();
  // tx.close();
  // OwnMethods.WriteFile(log_path, true, (System.currentTimeMillis() - start) + "\n");
  // index = 0;
  // count++;
  // dbservice.shutdown();
  //
  // commit_time++;
  // // if(commit_time == 2)
  // // break;
  //
  // dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
  // tx = dbservice.beginTx();
  // continue;
  // }
  // index++;
  // count++;
  // // break;
  // }
  // if (index != 0) {
  // tx.success();
  // tx.close();
  // }
  // dbservice.shutdown();
  //
  // } catch (Exception e) {
  // e.printStackTrace();
  // }
  // }

  // public static void set_NL_list_label(int max_hop_num, ArrayList<Integer> labels) {
  // HashMap<String, String> map = OwnMethods.ReadMap(graph_node_map_path);
  // HashMap<Integer, Integer> graph_node_map = new HashMap<Integer, Integer>();
  // for (String key : map.keySet()) {
  // String value = map.get(key);
  // int graph_id = Integer.parseInt(key);
  // int pos_id = Integer.parseInt(value);
  // graph_node_map.put(graph_id, pos_id);
  // }
  //
  // GraphDatabaseService dbservice =
  // new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
  // try {
  // Transaction tx = dbservice.beginTx();
  // Node root = RTreeUtility.getRTreeRoot(dbservice, dataset);
  // long rootID = root.getId();
  // tx.success();
  // tx.close();
  //
  // Queue<Long> queue = new LinkedList<Long>();
  // queue.add(rootID);
  // int index = 0, count = 0;
  // tx = dbservice.beginTx();
  // while (queue.isEmpty() == false) {
  // Utility.print("index:" + index);
  // Utility.print("count:" + count);
  // Long pos = queue.poll();
  // Node node = dbservice.getNodeById(pos);
  // Iterable<Relationship> rels =
  // node.getRelationships(Direction.OUTGOING, RTreeRel.RTREE_CHILD);
  // for (Relationship relationship : rels)
  // queue.add(relationship.getEndNode().getId());
  // if (count < 2890) {
  // count++;
  // continue;
  // }
  //
  // int rtree_id = (Integer) node.getProperty("rtree_id");
  // for (int i = 1; i <= max_hop_num; i++) {
  // for (int label : labels) {
  // String property_name = String.format("NL_%d_%d_list", i, label);
  // if (node.hasProperty(property_name)) {
  // int[] NL_list_label = (int[]) node.getProperty(property_name);
  // String label_name = String.format("%d_NL_%d_%d_list", rtree_id, i, label);
  // // OwnMethods.Print(label_name);
  // Label sub_label = DynamicLabel.label(label_name);
  // for (int id : NL_list_label) {
  // int pos_id = graph_node_map.get(id);
  // Node graph_node = dbservice.getNodeById(pos_id);
  // graph_node.addLabel(sub_label);
  // // OwnMethods.Print(graph_node.getAllProperties());
  // // break;
  // }
  // }
  // }
  // }
  // if (index == 20) {
  // tx.success();
  // tx.close();
  // index = 0;
  // count++;
  // dbservice.shutdown();
  //
  // dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
  // tx = dbservice.beginTx();
  // continue;
  // }
  // index++;
  // count++;
  // // break;
  // }
  // if (index != 0) {
  // tx.success();
  // tx.close();
  // dbservice.shutdown();
  // }
  //
  // } catch (Exception e) {
  // e.printStackTrace();
  // }
  // }



  // public static void NL_size_Check() {
  // String output_path =
  // "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/treenode_NL_stast.txt";
  // GraphDatabaseService dbservice =
  // new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
  // try {
  // Transaction tx = dbservice.beginTx();
  // Node root = RTreeUtility.getRTreeRoot(dbservice, dataset);
  // Queue<Node> cur = new LinkedList<Node>();
  // Queue<Node> next = new LinkedList<Node>();
  // cur.add(root);
  //
  // int level_index = 0;
  // while (cur.isEmpty() == false) {
  // OwnMethods.WriteFile(output_path, true, String.format("level: %d\n", level_index));
  // level_index++;
  // Iterator<Node> iterator = cur.iterator();
  // while (iterator.hasNext()) {
  // Node node = iterator.next();
  // for (int i = 1; i <= MAX_HOPNUM; i++) {
  // for (int label : labels) {
  // String NL_size_property_name = String.format("NL_%d_%d_size", i, label);
  // int size = (Integer) node.getProperty(NL_size_property_name);
  // String line = String.format("%d %s size:%d", node.getProperty("rtree_id"),
  // NL_size_property_name, size);
  // Utility.print(line);
  // OwnMethods.WriteFile(output_path, true, line + "\n");
  // }
  // }
  // Iterable<Relationship> rels =
  // node.getRelationships(Direction.OUTGOING, RTreeRel.RTREE_CHILD);
  // for (Relationship relationship : rels)
  // next.add(relationship.getEndNode());
  // }
  // cur = next;
  // next = new LinkedList<Node>();
  // }
  // tx.success();
  // tx.close();
  // dbservice.shutdown();
  //
  // } catch (Exception e) {
  // e.printStackTrace();
  // }
  // }

  // public static void generateNL_size() {
  // Utility.print("Calculate NL size\n");
  // GraphDatabaseService dbservice =
  // new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
  // try {
  // Transaction tx = dbservice.beginTx();
  // Node root = RTreeUtility.getRTreeRoot(dbservice, dataset);
  // Queue<Node> cur = new LinkedList<Node>();
  // cur.add(root);
  //
  // while (cur.isEmpty() == false) {
  // Node node = cur.poll();
  // for (int i = 1; i <= MAX_HOPNUM; i++) {
  // for (int label : labels) {
  // String NL_list_property_name = String.format("NL_%d_%d_list", i, label);
  // String NL_size_property_name = String.format("NL_%d_%d_size", i, label);
  // if (node.hasProperty(NL_list_property_name)) {
  // int[] NL_list_label = (int[]) node.getProperty(NL_list_property_name);
  // int size = NL_list_label.length;
  // node.setProperty(NL_size_property_name, size);
  // // OwnMethods.Print(String.format("%s size:%d", NL_list_property_name, size));
  // } else
  // node.setProperty(NL_size_property_name, 0);
  // }
  // }
  // Iterable<Relationship> rels =
  // node.getRelationships(Direction.OUTGOING, RTreeRel.RTREE_CHILD);
  // for (Relationship relationship : rels)
  // cur.add(relationship.getEndNode());
  // }
  //
  // tx.success();
  // tx.close();
  // dbservice.shutdown();
  //
  // } catch (Exception e) {
  // e.printStackTrace();
  // System.exit(-1);
  // }
  // }

  // /**
  // * For the second hop Assume that the second hop NL has been built for all leaf nodes Not used
  // * anymore, replaced by ConstructNL() for two hop
  // */
  // public static void MergeLeafNL() {
  // GraphDatabaseService dbservice =
  // new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
  // try {
  // Transaction tx = dbservice.beginTx();
  // List<Node> leafNodes = RTreeUtility.getRTreeLeafLevelNodes(dbservice, dataset);
  // HashSet<Node> cur_level_nodes = new HashSet<>(leafNodes);
  //
  // HashSet<Node> up_level_nodes = new HashSet<Node>();
  //
  // while (cur_level_nodes.isEmpty() == false) {
  // for (Node node : cur_level_nodes) {
  // HashMap<Integer, TreeSet<Integer>> NL_list = new HashMap<Integer, TreeSet<Integer>>();
  // HashMap<Integer, String> property_name_map = new HashMap<Integer, String>();
  //
  // for (int label : labels) {
  // NL_list.put(label, new TreeSet<Integer>());
  // property_name_map.put(label, String.format("NL_%d_%d_list", MAX_HOPNUM, label));
  // }
  //
  // for (Relationship relationship : node.getRelationships(Direction.OUTGOING)) {
  // Node out_neighbor = relationship.getEndNode();
  // for (int label : labels) {
  // int[] neighbor_NL_label =
  // (int[]) out_neighbor.getProperty(property_name_map.get(label));
  // TreeSet<Integer> NL_label = NL_list.get(label);
  // for (int element : neighbor_NL_label)
  // NL_label.add(element);
  // }
  // }
  //
  // for (int label : labels) {
  // TreeSet<Integer> NL_label_set = NL_list.get(label);
  // int[] NL_label = new int[NL_label_set.size()];
  // int index = 0;
  // for (int element : NL_label_set) {
  // NL_label[index] = element;
  // index++;
  // }
  // node.setProperty(property_name_map.get(label), NL_label);
  // }
  //
  //
  //
  // Relationship relationship =
  // node.getSingleRelationship(RTreeRel.RTREE_CHILD, Direction.INCOMING);
  // if (relationship != null)
  // up_level_nodes.add(relationship.getStartNode());
  // }
  // cur_level_nodes = up_level_nodes;
  // up_level_nodes = new HashSet<Node>();
  // }
  //
  // tx.success();
  // tx.close();
  //
  // dbservice.shutdown();
  // } catch (Exception e) {
  // e.printStackTrace();
  // }
  // }

  // /**
  // * for the second hop
  // */
  // public static void LoadLeafNodeNList() {
  // String NL_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset +
  // "/2hop_neighbor_spa.txt";
  //
  // String line = "";
  // try {
  // Map<String, String> map_str = OwnMethods.ReadMap(geo_id_map_path);
  // Map<Long, Long> map = new HashMap<Long, Long>();
  // Set<Entry<String, String>> set = map_str.entrySet();
  // for (Entry<String, String> entry : set) {
  // Long key = Long.parseLong(entry.getKey());
  // Long value = Long.parseLong(entry.getValue());
  // map.put(value, key);
  // }
  //
  // ArrayList<Integer> label_list = OwnMethods.readIntegerArray(label_list_path);
  // // ArrayList<Integer> labels = new ArrayList<Integer>(Arrays.asList(0,1));
  //
  // BufferedReader reader = new BufferedReader(new FileReader(new File(NL_path)));
  // line = reader.readLine();
  // int node_count = Integer.parseInt(line);
  //
  // GraphDatabaseService dbservice =
  // new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
  //
  // int tx_index = 0;
  //
  // while ((line = reader.readLine()) != null) {
  // Utility.print(tx_index);
  // tx_index++;
  //
  // Transaction tx = dbservice.beginTx();
  //
  // String[] str_list = line.split(",");
  // long graph_id = Long.parseLong(str_list[0]);
  // long pos_id = map.get(graph_id);
  //
  // int line_count = Integer.parseInt(str_list[1]);
  //
  // HashMap<Integer, ArrayList<Integer>> NL_list = new HashMap<Integer, ArrayList<Integer>>();
  // for (int label : labels)
  // NL_list.put(label, new ArrayList<Integer>(line_count));
  //
  // for (int i = 0; i < line_count; i++) {
  // int neighbor = Integer.parseInt(str_list[i + 2]);
  // NL_list.get(label_list.get(neighbor)).add(neighbor);
  // }
  //
  // Node node = dbservice.getNodeById(pos_id);
  // // OwnMethods.Print(osm_node.getAllProperties().toString());
  // for (int label : labels) {
  // if (NL_list.get(label).size() == 0)
  // continue;
  // else {
  // ArrayList<Integer> label_neighbors = NL_list.get(label);
  // int[] NL_array = new int[label_neighbors.size()];
  // int i = 0;
  // Iterator<Integer> iterator = label_neighbors.iterator();
  // while (iterator.hasNext()) {
  // NL_array[i] = iterator.next();
  // i++;
  // }
  // String property_name = String.format("NL_%d_%d_list", MAX_HOPNUM, label);
  // node.setProperty(property_name, NL_array);
  // }
  // }
  //
  // tx.success();
  // tx.close();
  //
  // }
  // reader.close();
  // dbservice.shutdown();
  //
  // } catch (Exception e) {
  // Utility.print(line);
  // e.printStackTrace();
  // }
  //
  //
  // }

  // /**
  // * load first hop neighbor list with distinguishing labels for all non-leaf nodes in the RTree
  // the
  // * graph data will be used directly
  // */
  // public static void ConstructNL() {
  // Utility.print("Construct the first hop NL list\n");
  // try {
  // ArrayList<ArrayList<Integer>> graph = OwnMethods.ReadGraph(graph_path);
  // ArrayList<Integer> labelList = OwnMethods.readIntegerArray(label_list_path);
  // HashMap<Integer, String> NLListPropertyName = new HashMap<Integer, String>();
  // for (int label : labels)
  // NLListPropertyName.put(label, String.format("NL_1_%d_list", label));
  //
  // GraphDatabaseService dbservice =
  // new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
  // Transaction tx = dbservice.beginTx();
  //
  // // get all the deepest non-leaf nodes
  // List<Node> leafNodes = RTreeUtility.getRTreeLeafLevelNodes(dbservice, dataset);
  // Set<Node> cur_level_nodes = new HashSet<>(leafNodes);
  //
  // for (Node node : cur_level_nodes) {
  // TreeSet<Integer> NL = new TreeSet<Integer>();
  // Iterable<Relationship> rels =
  // node.getRelationships(Direction.OUTGOING, Labels.RTreeRel.RTREE_REFERENCE);
  // for (Relationship rel : rels) {
  // Node child = rel.getEndNode();
  // int graph_id = (Integer) child.getProperty("id");
  // for (int neighbor : graph.get(graph_id))
  // NL.add(neighbor);
  // }
  //
  // HashMap<Integer, ArrayList<Integer>> labelID_list =
  // new HashMap<Integer, ArrayList<Integer>>();
  // for (int label : labels)
  // labelID_list.put(label, new ArrayList<Integer>(NL.size()));
  //
  // Iterator<Integer> iterator = NL.iterator();
  // while (iterator.hasNext()) {
  // int neighborID = iterator.next();
  // int label = labelList.get(neighborID);
  // labelID_list.get(label).add(neighborID);
  // }
  //
  // for (int label : labels)
  // if (labelID_list.get(label).size() > 0) {
  // ArrayList<Integer> neighborList = labelID_list.get(label);
  // int[] neighborArray = new int[neighborList.size()];
  // for (int i = 0; i < neighborList.size(); i++)
  // neighborArray[i] = neighborList.get(i);
  // node.setProperty(NLListPropertyName.get(label), neighborArray);
  // }
  // }
  //
  // // upper level
  // Set<Node> next_level_nodes = new HashSet<Node>();
  // while (cur_level_nodes.isEmpty() == false) {
  // for (Node node : cur_level_nodes) {
  // Relationship relationship =
  // node.getSingleRelationship(RTreeRel.RTREE_CHILD, Direction.INCOMING);
  // if (relationship != null) {
  // Node parent = relationship.getStartNode();
  // next_level_nodes.add(parent);
  // }
  // }
  //
  // for (Node node : next_level_nodes) {
  // HashMap<Integer, TreeSet<Integer>> labelID_neighborSet =
  // new HashMap<Integer, TreeSet<Integer>>();
  // for (int label : labels)
  // labelID_neighborSet.put(label, new TreeSet<Integer>());
  //
  // Iterable<Relationship> rels =
  // node.getRelationships(Direction.OUTGOING, Labels.RTreeRel.RTREE_CHILD);
  // for (Relationship rel : rels) {
  // Node child = rel.getEndNode();
  //
  // for (int label : labels) {
  // if (child.hasProperty(NLListPropertyName.get(label))) {
  // int[] list = (int[]) child.getProperty(NLListPropertyName.get(label));
  // for (int neighbor : list)
  // labelID_neighborSet.get(label).add(neighbor);
  // }
  // }
  // }
  //
  // for (int label : labels) {
  // TreeSet<Integer> neighborSet = labelID_neighborSet.get(label);
  // if (neighborSet.size() > 0) {
  // int[] NL_array = new int[neighborSet.size()];
  // Iterator<Integer> iterator = neighborSet.iterator();
  // int i = 0;
  // while (iterator.hasNext()) {
  // NL_array[i] = iterator.next();
  // i++;
  // }
  // node.setProperty(NLListPropertyName.get(label), NL_array);
  // }
  // }
  // }
  //
  // cur_level_nodes = next_level_nodes;
  // next_level_nodes = new HashSet<Node>();
  // }
  // tx.success();
  // tx.close();
  // dbservice.shutdown();
  //
  // } catch (Exception e) {
  // e.printStackTrace();
  // System.exit(-1);
  // }
  //
  // }

  // /**
  // * load specific hop neighbor list from file
  // *
  // * @param hop_num
  // * @param labels
  // * @param label_list
  // */
  // public static void ConstructNL(String neighborListPath, int hop_num, ArrayList<Integer> labels,
  // ArrayList<Integer> label_list) {
  // int offset = 196591;
  // try {
  // ArrayList<ArrayList<Integer>> graph = OwnMethods.ReadGraph(neighborListPath);
  // Map<String, String> map_str = OwnMethods.ReadMap(geo_id_map_path);
  // Map<Long, Long> map = new HashMap<Long, Long>();
  // Set<Entry<String, String>> set = map_str.entrySet();
  // for (Entry<String, String> entry : set) {
  // Long key = Long.parseLong(entry.getKey());
  // Long value = Long.parseLong(entry.getValue());
  // map.put(key, value);
  // }
  //
  // GraphDatabaseService dbservice =
  // new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
  // Transaction tx = dbservice.beginTx();
  //
  // List<Node> leafNodes = RTreeUtility.getRTreeLeafLevelNodes(dbservice, dataset);
  // Set<Node> cur_level_nodes = new HashSet<>(leafNodes);
  //
  // for (Node node : cur_level_nodes) {
  // HashMap<Integer, TreeSet<Integer>> NL = new HashMap<Integer, TreeSet<Integer>>();
  // for (int i = 0; i < labels.size(); i++)
  // NL.put(labels.get(i), new TreeSet<Integer>());
  //
  // Iterable<Relationship> rels =
  // node.getRelationships(Direction.OUTGOING, Labels.RTreeRel.RTREE_REFERENCE);
  // for (Relationship rel : rels) {
  // Node child = rel.getEndNode();
  // long pos_id = child.getId();
  // long graph_id = map.get(pos_id) - offset;
  // if (graph_id > Integer.MAX_VALUE)
  // throw new Exception("graph id out of int range!");
  // int id = (int) graph_id;
  // for (int neighbor : graph.get(id)) {
  // int label = label_list.get(neighbor);
  // NL.get(label).add(neighbor);
  // }
  // }
  //
  //
  // for (int label : labels) {
  // TreeSet<Integer> hop_neighbors = NL.get(label);
  // int[] NL_array = new int[hop_neighbors.size()];
  // int i = 0;
  // Iterator<Integer> iterator = hop_neighbors.iterator();
  // while (iterator.hasNext()) {
  // NL_array[i] = iterator.next();
  // i++;
  // }
  // String property_name = String.format("NL_%d_%d_list", hop_num, label);
  // node.setProperty(property_name, NL_array);
  // }
  //
  // }
  //
  // Set<Node> next_level_nodes = new HashSet<Node>();
  // while (cur_level_nodes.isEmpty() == false) {
  // for (Node node : cur_level_nodes) {
  // Relationship relationship =
  // node.getSingleRelationship(RTreeRel.RTREE_CHILD, Direction.INCOMING);
  // if (relationship != null) {
  // Node parent = relationship.getStartNode();
  // next_level_nodes.add(parent);
  // }
  // }
  //
  // for (Node node : next_level_nodes) {
  // HashMap<Integer, TreeSet<Integer>> NL = new HashMap<Integer, TreeSet<Integer>>();
  // for (int i = 0; i < labels.size(); i++)
  // NL.put(labels.get(i), new TreeSet<Integer>());
  // Iterable<Relationship> rels =
  // node.getRelationships(Direction.OUTGOING, Labels.RTreeRel.RTREE_CHILD);
  // for (Relationship rel : rels) {
  // Node child = rel.getEndNode();
  //
  // for (int label : labels) {
  // String property_name = String.format("NL_%d_%d_list", hop_num, label);
  // if (child.hasProperty(property_name)) {
  // int[] child_NL_list = (int[]) child.getProperty(property_name);
  // for (int neighbor : child_NL_list)
  // NL.get(property_name).add(neighbor);
  // }
  // }
  // }
  //
  // for (int label : labels) {
  // int[] NL_array = new int[NL.get(label).size()];
  // int i = 0;
  // Iterator<Integer> iterator = NL.get(label).iterator();
  // while (iterator.hasNext()) {
  // NL_array[i] = iterator.next();
  // i++;
  // }
  // String property_name = String.format("NL_%d_%d_list", hop_num, label);
  // node.setProperty(property_name, NL_array);
  // }
  //
  // }
  //
  // cur_level_nodes = next_level_nodes;
  // next_level_nodes = new HashSet<Node>();
  // }
  // tx.success();
  // tx.close();
  // dbservice.shutdown();
  //
  // } catch (Exception e) {
  // e.printStackTrace();
  // }
  //
  // }

  // public static void ReplaceNL() {
  // String clear_property_name = "NL_1_list";
  // String replace_property_name = "NL_1_0_list";
  // try {
  // GraphDatabaseService dbservice =
  // new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
  // Transaction tx = dbservice.beginTx();
  //
  // Node rtree_root_node = RTreeUtility.getRTreeRoot(dbservice, dataset);
  //
  // Queue<Node> queue = new LinkedList<Node>();
  // queue.add(rtree_root_node);
  // while (queue.isEmpty() == false) {
  // Node node = queue.poll();
  // if (node.hasProperty(clear_property_name)) {
  // int[] property = (int[]) node.removeProperty(clear_property_name);
  // node.setProperty(replace_property_name, property);
  // }
  // Iterable<Relationship> rels =
  // node.getRelationships(Direction.OUTGOING, RTreeRel.RTREE_CHILD);
  // for (Relationship rel : rels) {
  // Node neighbor = rel.getEndNode();
  // queue.add(neighbor);
  // }
  // }
  //
  // tx.success();
  // tx.close();
  // dbservice.shutdown();
  // } catch (Exception e) {
  // e.printStackTrace();
  // }
  // }

  // public static void ClearNL() {
  // String clear_property_name = "NL_1_list";
  // try {
  // GraphDatabaseService dbservice =
  // new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
  // Transaction tx = dbservice.beginTx();
  //
  // Node rtree_root_node = RTreeUtility.getRTreeRoot(dbservice, dataset);
  //
  // Queue<Node> queue = new LinkedList<Node>();
  // queue.add(rtree_root_node);
  // while (queue.isEmpty() == false) {
  // Node node = queue.poll();
  // if (node.hasProperty(clear_property_name)) {
  // int[] property = (int[]) node.removeProperty(clear_property_name);
  // Utility.print(property.toString());
  // }
  // Iterable<Relationship> rels =
  // node.getRelationships(Direction.OUTGOING, RTreeRel.RTREE_CHILD);
  // for (Relationship rel : rels) {
  // Node neighbor = rel.getEndNode();
  // queue.add(neighbor);
  // }
  // }
  //
  // tx.success();
  // tx.close();
  // dbservice.shutdown();
  // } catch (Exception e) {
  // e.printStackTrace();
  // }
  // }

  public static HashMap<Long, ArrayList<Integer>> readContainIDMap(String filePath)
      throws Exception {
    LOGGER.info("read contain map from " + filePath);
    HashMap<Long, ArrayList<Integer>> containIDMap = new HashMap<Long, ArrayList<Integer>>();
    BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)));
    String line = null;
    while ((line = reader.readLine()) != null) {
      String[] lineList = line.split(",", 2);
      if (lineList.length != 2) {
        reader.close();
        throw new Exception("Contain ID Map format error at line " + line);
      }
      long id = Long.parseLong(lineList[0]);

      String[] idStrList = lineList[1].substring(1, lineList[1].length() - 1).split(",");
      ArrayList<Integer> idList = new ArrayList<Integer>(idStrList.length);
      for (String idString : idStrList)
        idList.add(Integer.parseInt(idString.trim()));

      containIDMap.put(id, idList);
    }
    reader.close();
    return containIDMap;
  }


}
