package graph;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import org.geotools.geometry.jts.Geometries;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import commons.*;
import commons.Config.system;
import commons.Labels.GraphLabel;
import commons.Labels.GraphRel;
import commons.Labels.OSMLabel;
import commons.Labels.OSMRelation;
import commons.Labels.RTreeRel;
import osm.OSM_Utility;

/**
 * not used
 * 
 * @author yuhansun
 *
 */
public class LoadData {

  static Config config = new Config();
  static system systemName = config.getSystemName();
  static String version = config.GetNeo4jVersion();
  static String dataset = config.getDatasetName();
  static String lon_name = config.GetLongitudePropertyName();
  static String lat_name = config.GetLatitudePropertyName();

  static String db_path, map_path, graph_path;


  static void initParameters() {
    switch (systemName) {
      case Ubuntu:
        db_path = String.format(
            "/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version,
            dataset);
        // static String map_path =
        // String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map.txt", dataset);
        /**
         * use this because osm node are not seen as spatial graph but directly use RTree leaf node
         * as the spatial vertices in the graph
         */
        map_path =
            String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map_RTree.txt", dataset);
        graph_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
        // label_list_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/label.txt",
        // dataset);
        break;
      case Windows:
        db_path = String.format(
            "D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset,
            version, dataset);
        // label_list_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/label.txt",
        // dataset);
      default:
        break;
    }
  }

  public static void main(String[] args) {
    initParameters();

    // LoadNonSpatialEntity();
    GetSpatialNodeMap();

    set_Spatial_Label();
    LoadGraphEdges();
    setRTreeID();
    // check();
    CalculateCount();
    getRTreeMap();

    // get_Geometry_OSMID_Map();
  }

  /**
   * get map for geometry pos id to graph id but not useful in our new graph structure because osm
   * spatial node is not used as graph vertices
   */
  public static void get_Geometry_OSMID_Map() {
    try {
      GraphDatabaseService databaseService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = databaseService.beginTx();
      Iterable<Node> geometry_nodes = RTreeUtility.getAllGeometries(databaseService, dataset);
      Map<Object, Object> map = new TreeMap<Object, Object>();
      for (Node node : geometry_nodes) {
        Long geom_id = node.getId();
        Util.println(geom_id);
        long osm_id = (Long) node.getSingleRelationship(OSMRelation.GEOM, Direction.INCOMING)
            .getStartNode().getProperty("node_osm_id");
        map.put(geom_id, osm_id);
      }
      String map_path =
          String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/geom_osmid_map.txt", dataset);
      OwnMethods.WriteMap(map_path, true, map);
      tx.success();
      databaseService.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * map each rtree_id to pos_id output file is a map format
   */
  public static void getRTreeMap() {
    try {
      GraphDatabaseService databaseService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = databaseService.beginTx();
      Node rtree_root_node = RTreeUtility.getRTreeRoot(databaseService, dataset);
      TraversalDescription td = databaseService.traversalDescription().depthFirst()
          .relationships(RTreeRel.RTREE_CHILD, Direction.OUTGOING)
          .evaluator(Evaluators.includeWhereLastRelationshipTypeIs(RTreeRel.RTREE_CHILD));
      Iterable<Node> rtree_nodes = td.traverse(rtree_root_node).nodes();

      Map<Object, Object> rtree_map = new TreeMap<Object, Object>();
      rtree_map.put(0, rtree_root_node.getId());

      for (Node node : rtree_nodes)
        rtree_map.put(node.getProperty("rtree_id"), node.getId());

      String rtree_map_path =
          String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/rtree_map.txt", dataset);
      OwnMethods.WriteMap(rtree_map_path, true, rtree_map);

      tx.success();
      databaseService.shutdown();

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * calculate count of spatial vertices enclosed by the MBR for each non-leaf R-Tree node. This is
   * important in the query algorithm
   */
  public static void CalculateCount() {
    try {
      GraphDatabaseService databaseService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = databaseService.beginTx();

      Iterable<Node> geometry_nodes = RTreeUtility.getAllGeometries(databaseService, dataset);
      Set<Node> set = new HashSet<Node>();
      for (Node node : geometry_nodes) {
        Node parent =
            node.getSingleRelationship(RTreeRel.RTREE_REFERENCE, Direction.INCOMING).getStartNode();
        if (parent != null) {
          if (parent.hasProperty("count"))
            parent.setProperty("count", (Integer) parent.getProperty("count") + 1);
          else
            parent.setProperty("count", 1);
          set.add(parent);
        }
      }

      Set<Node> next_level_set = new HashSet<Node>();

      while (set.isEmpty() == false) {
        for (Node node : set) {
          Relationship relationship =
              node.getSingleRelationship(RTreeRel.RTREE_CHILD, Direction.INCOMING);
          if (relationship != null) {
            Node parent = relationship.getStartNode();
            if (parent.hasProperty("count"))
              parent.setProperty("count",
                  (Integer) parent.getProperty("count") + (Integer) node.getProperty("count"));
            else
              parent.setProperty("count", (Integer) node.getProperty("count"));
            next_level_set.add(parent);
          }
        }

        set = next_level_set;
        next_level_set = new HashSet<Node>();
      }

      tx.success();
      tx.close();
      databaseService.shutdown();

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static void check() {
    try {
      GraphDatabaseService databaseService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = databaseService.beginTx();
      Node check_node = databaseService.getNodeById(3849874);
      Util.println(check_node.getAllProperties());

      tx.success();
      tx.close();
      databaseService.shutdown();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * set each R-tree non-leaf node a unique id which starts from root with id 0 follow a bfs
   * sequence
   */
  public static void setRTreeID() {
    try {
      GraphDatabaseService databaseService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = databaseService.beginTx();
      // Node rtree_root_node = databaseService.getNodeById(3849874);
      Node rtree_root_node = RTreeUtility.getRTreeRoot(databaseService, dataset);
      Queue<Node> queue = new LinkedList<Node>();
      queue.add(rtree_root_node);
      int cur_id = 0;

      while (queue.isEmpty() == false) {
        Node node = queue.poll();

        Node working_node = databaseService.getNodeById(node.getId());
        working_node.setProperty("rtree_id", cur_id);
        // tx.success();
        // tx.close();
        // OwnMethods.Print(node.getId());
        // OwnMethods.Print(node.getAllProperties());
        cur_id++;

        // tx = databaseService.beginTx();
        Iterable<Relationship> edges =
            node.getRelationships(RTreeRel.RTREE_CHILD, Direction.OUTGOING);
        for (Relationship edge : edges)
          queue.add(edge.getEndNode());
        // tx.success();
      }
      tx.success();
      tx.close();
      databaseService.shutdown();

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }


  public static void LoadGraphEdges() {
    BatchInserter inserter = null;
    try {
      Map<String, String> id_map = OwnMethods.ReadMap(map_path);
      Map<String, String> config = new HashMap<String, String>();
      config.put("dbms.pagecache.memory", "6g");
      inserter = BatchInserters.inserter(new File(db_path).getAbsoluteFile(), config);

      ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graph_path);
      for (int i = 0; i < graph.size(); i++) {
        ArrayList<Integer> neighbors = graph.get(i);
        int start_neo4j_id = Integer.parseInt(id_map.get(String.valueOf(i)));
        for (int j = 0; j < neighbors.size(); j++) {
          int neighbor = neighbors.get(j);
          if (i < neighbor) {
            int end_neo4j_id = Integer.parseInt(id_map.get(String.valueOf(neighbor)));
            inserter.createRelationship(start_neo4j_id, end_neo4j_id, GraphRel.GRAPH_LINK, null);
          }
        }
      }
      inserter.shutdown();

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * set all spatial vertices (osm_node) with label GRAPH_1
   */
  public static void set_Spatial_Label() {
    GraphDatabaseService dbservice =
        new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
    try {
      Transaction tx = dbservice.beginTx();
      Iterable<Node> Geometries = RTreeUtility.getAllGeometries(dbservice, dataset);
      for (Node node : Geometries) {
        // OwnMethods.Print(node.getAllProperties().toString());
        node.addLabel(GraphLabel.GRAPH_1);


        // OwnMethods.Print(node.getAllProperties());
        // Iterable<Label> labels = node.getLabels();
        // for ( Label label : labels)
        // OwnMethods.Print(label.toString());
        // break;
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
   * attach spatial node (osm nodes) map to file node_map.txt and move property from osm node to
   * RTree leaf node, including id and location
   */
  public static void GetSpatialNodeMap() {
    try {
      Map<Object, Object> id_map = new TreeMap<Object, Object>();

      GraphDatabaseService databaseService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = databaseService.beginTx();
      Node osm_node = OSM_Utility.getOSMDatasetNode(databaseService, dataset);
      Iterable<Node> spatial_nodes = OSM_Utility.getAllPointNodes(databaseService, osm_node);
      ArrayList<Long> ids = new ArrayList<Long>();
      for (Node point : spatial_nodes) {
        // long entity_id = (Long) point.getProperty("node_osm_id");
        // long neo4j_id = point.getId();
        long id = point.getId();
        ids.add(id);
      }
      tx.success();
      tx.close();

      tx = databaseService.beginTx();
      for (int i = 0; i < ids.size(); i++) {
        Util.println(i);
        long id = ids.get(i);
        Node point = databaseService.getNodeById(id);
        long entity_id = (Long) point.getProperty("node_osm_id");
        Relationship relationship =
            point.getSingleRelationship(Labels.OSMRelation.GEOM, Direction.OUTGOING);
        Node leafNode = relationship.getEndNode();
        long neo4j_id = leafNode.getId();
        id_map.put(entity_id, neo4j_id);

        leafNode.setProperty("id", entity_id);
        double lon = (Double) point.getProperty(lon_name);
        double lat = (Double) point.getProperty(lat_name);
        leafNode.setProperty(lon_name, lon);
        leafNode.setProperty(lat_name, lat);
        if (i % 10000 == 0) {
          tx.success();
          tx.close();
          tx = databaseService.beginTx();
        }
      }
      tx.success();
      tx.close();
      databaseService.shutdown();

      OwnMethods.WriteMap(map_path, true, id_map);

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * Load non-spatial graph vertices and write the map to file node_map.txt All non-spatial vertices
   * are with label GRAPH_0
   */
  public static void LoadNonSpatialEntity() {
    String entity_path =
        String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
    Util.println(String.format("LoadNonSpatialEntity\n from %s to %s", entity_path, db_path));
    LoadNonSpatialEntity(entity_path, db_path, map_path);
  }

  /**
   * Load non-spatial graph vertices and write the map to map_path All non-spatial vertices are with
   * label GRAPH_0
   * 
   * @param entity_path
   * @param db_path
   * @param map_path
   */
  public static void LoadNonSpatialEntity(String entity_path, String db_path, String map_path) {
    try {
      ArrayList<Entity> entities = OwnMethods.ReadEntity(entity_path);
      Map<Object, Object> id_map = new TreeMap<Object, Object>();

      Map<String, String> config = new HashMap<String, String>();
      config.put("dbms.pagecache.memory", "6g");
      BatchInserter inserter = BatchInserters.inserter(new File(db_path).getAbsoluteFile(), config);

      for (int i = 0; i < entities.size(); i++) {
        Entity entity = entities.get(i);
        if (entity.IsSpatial == false) {
          Map<String, Object> properties = new HashMap<String, Object>();
          properties.put("id", i);
          Long pos_id = inserter.createNode(properties, GraphLabel.GRAPH_0);
          id_map.put((long) i, pos_id);
        }
      }
      inserter.shutdown();

      OwnMethods.WriteMap(map_path, true, id_map);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
