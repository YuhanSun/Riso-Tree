package graph;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.management.relation.Relation;
import javax.sound.sampled.LineUnavailableException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import commons.Config;
import commons.Labels;
import commons.OwnMethods;
import commons.RTreeUtility;
import commons.Util;
import commons.Labels.GraphLabel;
import commons.Labels.GraphRel;
import commons.Labels.OSMLabel;
import commons.Labels.OSMRelation;
import commons.Config.system;
import commons.GraphUtil;
import osm.OSM_Utility;

/**
 * this class switch spatial vertices in the graph to leaf nodes in R-tree in order to avoid one
 * level of graph traversal from R-tree leaf node to osm nodes So the GRAPH_1 label will be
 * transferred among nodes and GRAPH_LINK relationships will be transferred as well the attribute
 * id, lon and lat will be transfered as well
 * 
 * @author yuhansun
 *
 */
public class Graph_Switch {

  static Config config = new Config();
  static String dataset = config.getDatasetName();
  static String version = config.GetNeo4jVersion();
  static system systemName = config.getSystemName();
  static String lon_name = config.GetLongitudePropertyName();
  static String lat_name = config.GetLatitudePropertyName();
  static String graphLinkLabelName = Labels.GraphRel.GRAPH_LINK.name();

  static String db_path;
  static String label_list_path;

  static int max_hop_num = 2;

  static void initParameters() {
    switch (systemName) {
      case Ubuntu:
        db_path = String.format(
            "/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version,
            dataset);
        label_list_path =
            String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/label.txt", dataset);
        break;
      case Windows:
        db_path = String.format(
            "D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset,
            version, dataset);
        label_list_path =
            String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/label.txt", dataset);
      default:
        break;
    }
  }

  public static void main(String[] args) {
    initParameters();
    // transferProperty();

    // getNodesMapRTree();
    // addLabel();
    // removeLabel();
    // removeNLLabelOnGraph_0();

    // labelTest();

    // deleteRelationship();
    createNewRelationship();

    // transferLabel();
    // addLabel();

  }

  public static void createNewRelationship() {
    BatchInserter inserter = null;
    try {
      String map_path =
          String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map_RTree.txt", dataset);
      String graph_path =
          String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);

      Map<String, String> id_map = OwnMethods.ReadMap(map_path);
      ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graph_path);

      Map<String, String> config = new HashMap<String, String>();
      config.put("dbms.pagecache.memory", "6g");
      inserter = BatchInserters.inserter(new File(db_path).getAbsoluteFile(), config);

      for (int i = 0; i < graph.size(); i++) {
        // OwnMethods.Print(i);
        ArrayList<Integer> neighbors = graph.get(i);
        long start_neo4j_id = Long.parseLong(id_map.get(String.valueOf(i)));
        for (int j = 0; j < neighbors.size(); j++) {
          int neighbor = neighbors.get(j);
          if (i < neighbor) {
            long end_neo4j_id = Long.parseLong(id_map.get(String.valueOf(neighbor)));
            Util.println(String.format("%d\t%d", start_neo4j_id, end_neo4j_id));
            inserter.createRelationship(start_neo4j_id, end_neo4j_id, GraphRel.GRAPH_LINK, null);
          }
        }
      }
      inserter.shutdown();

    } catch (Exception e) {
      if (inserter != null)
        inserter.shutdown();
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * remove edges from nonspatial to spatial vertices (osm nodes)
   */
  public static void deleteRelationship() {
    try {
      GraphDatabaseService dbservice =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      // Transaction tx = dbservice.beginTx();
      // Iterable<Node> leafNodes = OSM_Utility.getAllGeometries(dbservice, dataset);
      // for ( Node leafNode : leafNodes)
      // {
      // Node osmNode = leafNode.getSingleRelationship(OSMRelation.GEOM, Direction.INCOMING)
      // .getStartNode();
      //
      // //transfer relationship
      // Iterable<Relationship> rels = osmNode.getRelationships(Direction.INCOMING,
      // GraphRel.GRAPH_LINK);
      // for ( Relationship relationship : rels)
      // relationship.delete();
      //
      // }
      // tx.success();
      // tx.close();
      dbservice.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static void labelTest() {
    try {
      GraphDatabaseService dbservice =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = dbservice.beginTx();

      // ResourceIterator<Node> nodes = dbservice.findNodes(GraphLabel.GRAPH_1);
      // Node node;
      // while(nodes.hasNext())
      // {
      // node = nodes.next();
      // OwnMethods.Print(node.getId());
      // OwnMethods.Print(node.getAllProperties());
      // }

      String query = "match (n:GRAPH_1) return n limit 10";
      Result result = dbservice.execute(query);
      while (result.hasNext()) {
        Map<String, Object> map = result.next();
        Node node = (Node) map.get("n");
        Util.println(node.getAllProperties());
      }

      tx.success();
      tx.close();
      dbservice.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * remove label of osm nodes, including on original GRAPH_0 nodes original GRAPH_0 label is
   * retained
   */
  public static void removeNLLabelOnGraph_0() {
    BatchInserter inserter = null;
    Map<String, String> config = new HashMap<String, String>();
    config.put("dbms.pagecache.memory", "10g");
    try {

      ArrayList<Integer> label_list = OwnMethods.readIntegerArray(label_list_path);
      Util.println(String.format("label_list size %d", label_list.size()));

      TreeSet<Integer> nonSpaIDs = new TreeSet<Integer>();
      for (int i = 0; i < label_list.size(); i++)
        if (label_list.get(i).equals(1) == false)
          nonSpaIDs.add(i);
      Util.println(String.format("spaIDs size %d", nonSpaIDs.size()));

      String map_path =
          String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map.txt", dataset);
      Map<String, String> map_str = OwnMethods.ReadMap(map_path);
      Map<Long, Long> map = new TreeMap<Long, Long>();
      for (String key : map_str.keySet())
        map.put(Long.parseLong(key), Long.parseLong(map_str.get(key)));
      Util.println(String.format("map size %d", map.size()));

      inserter = BatchInserters.inserter(new File(db_path), config);
      for (int id : nonSpaIDs) {
        Util.println(id);
        long pos_id = map.get((long) id);
        inserter.setNodeLabels(pos_id, GraphLabel.GRAPH_0);
      }
      inserter.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
      if (inserter != null)
        inserter.shutdown();
      System.exit(-1);
    }
  }

  /**
   * remove label of osm nodes, including on original GRAPH_1 nodes
   */
  public static void removeLabel() {
    BatchInserter inserter = null;
    Map<String, String> config = new HashMap<String, String>();
    config.put("dbms.pagecache.memory", "10g");
    try {

      ArrayList<Integer> label_list = OwnMethods.readIntegerArray(label_list_path);
      Util.println(String.format("label_list size %d", label_list.size()));

      TreeSet<Integer> spaIDs = new TreeSet<Integer>();
      for (int i = 0; i < label_list.size(); i++)
        if (label_list.get(i).equals(1))
          spaIDs.add(i);
      Util.println(String.format("spaIDs size %d", spaIDs.size()));

      String map_path =
          String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map.txt", dataset);
      Map<String, String> map_str = OwnMethods.ReadMap(map_path);
      Map<Long, Long> map = new TreeMap<Long, Long>();
      for (String key : map_str.keySet())
        map.put(Long.parseLong(key), Long.parseLong(map_str.get(key)));
      Util.println(String.format("map size %d", map.size()));

      inserter = BatchInserters.inserter(new File(db_path), config);
      for (int id : spaIDs) {
        Util.println(id);
        long pos_id = map.get((long) id);
        inserter.setNodeLabels(pos_id, OSMLabel.OSM_NODE);
      }
      inserter.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
      if (inserter != null)
        inserter.shutdown();
      System.exit(-1);
    }
  }

  /**
   * add label to R-tree leaf nodes
   */
  public static void addLabel() {
    BatchInserter inserter = null;
    Map<String, String> config = new HashMap<String, String>();
    config.put("dbms.pagecache.memory", "10g");
    try {

      ArrayList<Integer> label_list = OwnMethods.readIntegerArray(label_list_path);
      Util.println(String.format("label_list size %d", label_list.size()));

      TreeSet<Integer> spaIDs = new TreeSet<Integer>();
      for (int i = 0; i < label_list.size(); i++)
        if (label_list.get(i).equals(1))
          spaIDs.add(i);
      Util.println(String.format("spaIDs size %d", spaIDs.size()));

      String map_path =
          String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map_RTree.txt", dataset);
      Map<String, String> map_str = OwnMethods.ReadMap(map_path);
      Map<Long, Long> map = new TreeMap<Long, Long>();
      for (String key : map_str.keySet())
        map.put(Long.parseLong(key), Long.parseLong(map_str.get(key)));
      Util.println(String.format("map size %d", map.size()));

      inserter = BatchInserters.inserter(new File(db_path), config);
      for (int id : spaIDs) {
        Util.println(id);
        long pos_id = map.get((long) id);
        inserter.setNodeLabels(pos_id, GraphLabel.GRAPH_1);
      }
      inserter.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
      if (inserter != null)
        inserter.shutdown();
      System.exit(-1);
    }
  }

  public static void transferGraph() {
    GraphDatabaseService dbservice =
        new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
    try {
      Transaction tx = dbservice.beginTx();
      Iterable<Node> leafNodes = RTreeUtility.getAllGeometries(dbservice, dataset);
      for (Node leafNode : leafNodes) {
        Node osmNode =
            leafNode.getSingleRelationship(OSMRelation.GEOM, Direction.INCOMING).getStartNode();

        // transfer location property
        if (osmNode.hasProperty(lon_name) == false)
          throw new Exception(String.format("%s does not has attribute %s", osmNode, lon_name));
        leafNode.setProperty(lon_name, osmNode.getProperty(lon_name));
        leafNode.setProperty(lat_name, osmNode.getProperty(lat_name));

        // transfer node_osm_id property
        if (osmNode.hasProperty("node_osm_id") == false)
          throw new Exception(String.format("%s does not has attribute node_osm_id", osmNode));
        leafNode.setProperty("node_osm_id", osmNode.getProperty("node_osm_id"));

        // transfer label
        Iterable<Label> labels = osmNode.getLabels();
        boolean has = false;
        for (Label label : labels)
          if (label.name().equals(GraphLabel.GRAPH_1.name())) {
            has = true;
            osmNode.removeLabel(label);
            break;
          }
        if (has == false)
          throw new Exception(
              String.format("%s does not has label %s", osmNode, GraphLabel.GRAPH_1));
        else
          leafNode.addLabel(GraphLabel.GRAPH_1);

        // transfer relationship
        Iterable<Relationship> rels =
            osmNode.getRelationships(Direction.INCOMING, GraphRel.GRAPH_LINK);
        for (Relationship relationship : rels) {
          Node startNode = relationship.getStartNode();
          relationship.delete();
          startNode.createRelationshipTo(leafNode, GraphRel.GRAPH_LINK);
        }

      }
      tx.success();
      tx.close();
      dbservice.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * generate the map file of graph node id (R-tree version) to neo4j pos both spatial and
   * non-spatial vertices are included
   */
  public static void getNodesMapRTree() {
    try {
      Map<Object, Object> map = new TreeMap<Object, Object>();

      GraphDatabaseService dbservice =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = dbservice.beginTx();

      int index = 0;

      // for graph vertices
      ResourceIterator<Node> nonspatialNodes = dbservice.findNodes(GraphLabel.GRAPH_0);
      while (nonspatialNodes.hasNext()) {
        Util.println(index);
        index++;
        Node node = nonspatialNodes.next();
        map.put((long) (Integer) node.getProperty("id"), node.getId());
      }
      tx.success();
      tx.close();

      tx = dbservice.beginTx();
      Iterable<Node> leafNodes = RTreeUtility.getAllGeometries(dbservice, dataset);
      for (Node leafNode : leafNodes) {
        Util.println(index);
        index++;
        map.put(leafNode.getProperty("id"), leafNode.getId());
      }

      String map_path =
          String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map_RTree.txt", dataset);
      OwnMethods.WriteMap(map_path, true, map);

      tx.success();
      tx.close();
      dbservice.shutdown();

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }

  }

  public static void transferProperty() {
    GraphDatabaseService dbservice =
        new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
    try {
      Transaction tx = dbservice.beginTx();
      Iterable<Node> leafNodes = RTreeUtility.getAllGeometries(dbservice, dataset);
      int index = 0;
      for (Node leafNode : leafNodes) {
        Util.println(index);
        index++;
        Node osmNode =
            leafNode.getSingleRelationship(OSMRelation.GEOM, Direction.INCOMING).getStartNode();

        // transfer location property
        if (osmNode.hasProperty(lon_name) == false)
          throw new Exception(String.format("%s does not has attribute %s", osmNode, lon_name));
        leafNode.setProperty(lon_name, osmNode.getProperty(lon_name));
        leafNode.setProperty(lat_name, osmNode.getProperty(lat_name));

        // transfer node_osm_id property
        if (osmNode.hasProperty("node_osm_id") == false)
          throw new Exception(String.format("%s does not has attribute node_osm_id", osmNode));
        leafNode.setProperty("id", osmNode.getProperty("node_osm_id"));
      }
      tx.success();
      tx.close();
      dbservice.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

}
