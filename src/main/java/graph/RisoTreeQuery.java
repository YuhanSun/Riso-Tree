package graph;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import commons.Config;
import commons.Config.Explain_Or_Profile;
import commons.Config.system;
import commons.Labels;
import commons.MyRectangle;
import commons.OwnMethods;
import commons.Query_Graph;
import commons.RTreeUtility;
import commons.Util;

/**
 * The class use NL_hopnum_label to organize neighbors. Not used any more.
 * 
 * @author ysun138
 *
 */
public class RisoTreeQuery {

  public GraphDatabaseService dbservice;
  public String dataset;
  public long[] graph_pos_map_list;

  public static Config config = new Config();
  public String lon_name = config.GetLongitudePropertyName();
  public String lat_name = config.GetLatitudePropertyName();
  public String graphLinkLabelName = Labels.GraphRel.GRAPH_LINK.name();
  public int MAX_HOPNUM = config.getMaxHopNum();
  // HMBR
  public int MAX_HMBRHOPNUM = config.getMaxHMBRHopNum();
  String minx_name = config.GetRectCornerName()[0];
  String miny_name = config.GetRectCornerName()[1];
  String maxx_name = config.GetRectCornerName()[2];
  String maxy_name = config.GetRectCornerName()[3];

  // query statistics
  public long range_query_time;
  public long get_iterator_time;
  public long iterate_time;
  public long result_count;
  public long page_hit_count;
  public String logPath;

  // test control variables
  public static boolean outputLevelInfo = false;
  public static boolean outputQuery = true;
  public static boolean outputExecutionPlan = true;
  public static boolean outputResult = false;

  /**
   * initialize
   * 
   * @param db_path database path ./graph.db
   * @param p_dataset dataset name
   * @param p_graph_pos_map the map from graph id to neo4j pos id
   */
  public RisoTreeQuery(String db_path, String p_dataset, long[] p_graph_pos_map) {
    dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
    dataset = p_dataset;
    graph_pos_map_list = p_graph_pos_map;
    // logPath = String.format("/mnt/hgfs/Experiment_Result/Riso-Tree/%s/query.log", dataset);
    logPath =
        String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s\\query.log", dataset);
  }

  public static int[][] Ini_Minhop(Query_Graph query_Graph) {
    int query_node_count = query_Graph.graph.size();
    int[][] minhop_index = new int[query_node_count][];

    for (int i = 0; i < query_node_count; i++) {
      if (query_Graph.spa_predicate[i] == null)
        minhop_index[i] = null;
      else
        minhop_index[i] = new int[query_node_count];
    }

    for (int i = 0; i < query_node_count; i++) {
      if (query_Graph.spa_predicate[i] != null) {
        boolean[] visited = new boolean[query_node_count];
        visited[i] = true;
        minhop_index[i][i] = -1;

        Queue<Integer> queue = new LinkedList<Integer>();
        queue.add(i);
        int pre_level_count = 1;
        int cur_level_count = 0;
        int level_index = 1;

        while (queue.isEmpty() == false) {
          for (int j = 0; j < pre_level_count; j++) {
            int node = queue.poll();
            for (int k = 0; k < query_Graph.graph.get(node).size(); k++) {
              int neighbor = query_Graph.graph.get(node).get(k);
              if (visited[neighbor] == false) {
                minhop_index[i][neighbor] = level_index;
                visited[neighbor] = true;
                cur_level_count += 1;
                queue.add(neighbor);
              }
            }
          }
          level_index++;
          pre_level_count = cur_level_count;
          cur_level_count = 0;
        }
      }
    }

    // minhop_index[2][0] = -1; minhop_index[2][3] = -1;
    return minhop_index;
  }

  /**
   * form the cypher query
   * 
   * @param query_Graph
   * @param limit -1 is no limit
   * @param Explain_Or_Profile
   * @param spa_predicates spatial predicates except for the min_pos spatial predicate
   * @param pos query graph node id with the trigger spatial predicate
   * @param id corresponding spatial graph node id (neo4j pos id)
   * @param NL_hopnum shrunk query node <query_graph_id, hop_num>
   * @param node the rtree node stores NL_list information
   * @return
   */
  public String formSubgraphQuery(Query_Graph query_Graph, int limit,
      Explain_Or_Profile explain_Or_Profile, HashMap<Integer, MyRectangle> spa_predicates, int pos,
      ArrayList<Long> ids) {
    String query = "";
    switch (explain_Or_Profile) {
      case Profile:
        query += "profile match ";
        break;
      case Explain:
        query += "explain match ";
        break;
      case Nothing:
        query += "match ";
        break;
    }

    // label
    query += String.format("(a0:GRAPH_%d)", query_Graph.label_list[0]);
    for (int i = 1; i < query_Graph.graph.size(); i++) {
      query += String.format(",(a%d:GRAPH_%d)", i, query_Graph.label_list[i]);
    }

    // edge
    for (int i = 0; i < query_Graph.graph.size(); i++) {
      for (int j = 0; j < query_Graph.graph.get(i).size(); j++) {
        int neighbor = query_Graph.graph.get(i).get(j);
        if (neighbor > i)
          query += String.format(",(a%d)-[:%s]-(a%d)", i, graphLinkLabelName, neighbor);
      }
    }

    query += " \n where ";

    // spatial predicate
    for (int key : spa_predicates.keySet()) {
      MyRectangle qRect = spa_predicates.get(key);
      query += String.format(" %f <= a%d.%s <= %f ", qRect.min_x, key, lon_name, qRect.max_x);
      query +=
          String.format("and %f <= a%d.%s <= %f and ", qRect.min_y, key, lat_name, qRect.max_y);
    }

    // id
    query += String.format("\n(id(a%d) = %d ", pos, ids.get(0));
    if (ids.size() > 1)
      for (int i = 1; i < ids.size(); i++)
        query += String.format("or id(a%d) = %d ", pos, ids.get(i));
    query += ")";

    // NL_id_list
    // for ( int key : NL_hopnum.keySet())
    // {
    // String id_list_property_name = String.format("NL_%d_%d_list", NL_hopnum.get(key),
    // query_Graph.label_list[key]);
    //
    // if ( node.hasProperty(id_list_property_name) == false)
    // {
    // query = "match (n) where false return n";
    // return query;
    // }
    //
    // int[] graph_id_list = (int[]) node.getProperty(id_list_property_name);
    // ArrayList<Long> pos_id_list = new ArrayList<Long>(graph_id_list.length);
    // for ( int i = 0; i < graph_id_list.length; i++)
    // pos_id_list.add(graph_pos_map_list[graph_id_list[i]]);
    //
    // query += String.format(" and ( id(a%d) = %d", key, pos_id_list.get(0));
    // for ( int i = 1; i < pos_id_list.size(); i++)
    // query += String.format(" or id(a%d) = %d", key, pos_id_list.get(i));
    // query += " )\n";
    //// query += String.format(" and id(a%d) in %s", key, pos_id_list.toString());
    // }

    // return
    query += "\nreturn id(a0)";
    for (int i = 1; i < query_Graph.graph.size(); i++)
      query += String.format(",id(a%d)", i);

    if (limit != -1)
      query += String.format(" limit %d", limit);

    return query;
  }

  /**
   * form the cypher query for MBR block
   * 
   * @param query_Graph
   * @param limit -1 is no limit
   * @param Explain_Or_Profile -1 is Explain; 1 is Profile; the rest is nothing
   * @param spa_predicates spatial predicates except the min_pos spatial predicate
   * @param pos query graph node id with the trigger spatial predicate
   * @param ids corresponding spatial graph node ids that in this MBR (neo4j pos id)
   * @param NL_hopnum shrunk query node <query_graph_id, hop_num>
   * @param node the rtree node stores NL_list information
   * @return
   */
  public String formSubgraphQuery_ForSpatialFirst_Block(Query_Graph query_Graph, int limit,
      Explain_Or_Profile explain_Or_Profile, HashMap<Integer, MyRectangle> spa_predicates, int pos,
      ArrayList<Long> ids, HashMap<Integer, Integer> NL_hopnum, Node node) {
    String query = "";
    switch (explain_Or_Profile) {
      case Profile:
        query += "profile match ";
        break;
      case Explain:
        query += "explain match ";
        break;
      case Nothing:
        query += "match ";
        break;
    }

    // label
    if (pos == 0 || NL_hopnum.containsKey(0))
      query += "(a0)";
    else
      query += String.format("(a0:GRAPH_%d)", query_Graph.label_list[0]);
    for (int i = 1; i < query_Graph.graph.size(); i++) {
      if (pos == i || NL_hopnum.containsKey(i))
        query += String.format(",(a%d)", i);
      else
        query += String.format(",(a%d:GRAPH_%d)", i, query_Graph.label_list[i]);
    }

    // edge
    for (int i = 0; i < query_Graph.graph.size(); i++) {
      for (int j = 0; j < query_Graph.graph.get(i).size(); j++) {
        int neighbor = query_Graph.graph.get(i).get(j);
        if (neighbor > i)
          query += String.format(",(a%d)-[:%s]-(a%d)", i, graphLinkLabelName, neighbor);
      }
    }

    query += " where\n";

    // spatial predicate
    for (int key : spa_predicates.keySet()) {
      if (key != pos) {
        MyRectangle qRect = spa_predicates.get(key);
        query += String.format(" %f <= a%d.%s <= %f ", qRect.min_x, key, lon_name, qRect.max_x);
        query +=
            String.format("and %f <= a%d.%s <= %f and", qRect.min_y, key, lat_name, qRect.max_y);
      }
    }

    query += "\n";

    // id
    query += String.format(" (id(a%d)=%d", pos, ids.get(0));
    if (ids.size() > 1)
      for (int i = 1; i < ids.size(); i++)
        query += String.format(" or id(a%d)=%d", pos, ids.get(i));
    // query += String.format(" id(a%d) in %s\n", pos, ids.toString());

    query += ")\n";
    Util.println(String.format("spa_ids size: %d", ids.size()));

    // NL_id_list
    for (int key : NL_hopnum.keySet()) {
      String id_list_size_property_name =
          String.format("NL_%d_%d_size", NL_hopnum.get(key), query_Graph.label_list[key]);
      int id_list_size = (Integer) node.getProperty(id_list_size_property_name);
      if (id_list_size > 0) // whether to use the shrunk label
        query = query.replaceFirst(String.format("a%d", key),
            String.format("a%d:GRAPH_%d", key, query_Graph.label_list[key]));
      else {
        String id_list_property_name =
            String.format("NL_%d_%d_list", NL_hopnum.get(key), query_Graph.label_list[key]);

        if (node.hasProperty(id_list_property_name) == false)
          return "match (n) where false return n";

        int[] graph_id_list = (int[]) node.getProperty(id_list_property_name);
        ArrayList<Long> pos_id_list = new ArrayList<Long>(graph_id_list.length);
        for (int i = 0; i < graph_id_list.length; i++)
          pos_id_list.add(graph_pos_map_list[graph_id_list[i]]);

        query += String.format(" and ( id(a%d) = %d", key, pos_id_list.get(0));
        for (int i = 1; i < pos_id_list.size(); i++)
          query += String.format(" or id(a%d) = %d", key, pos_id_list.get(i));
        query += " )\n";
        // query += String.format(" and id(a%d) in %s\n", key, pos_id_list.toString());
        Util.println(String.format("%s size is %d", id_list_property_name, pos_id_list.size()));
      }
    }

    // return
    query += " return id(a0)";
    for (int i = 1; i < query_Graph.graph.size(); i++)
      query += String.format(",id(a%d)", i);

    if (limit != -1)
      query += String.format(" limit %d", limit);

    return query;
  }

  // /**
  // * Does not consider condition that area of query rectangle is 0.
  // * If so, such function cannot make the correct dicision.
  // * @param query_Graph
  // * @param limit
  // */
  // public void Query(Query_Graph query_Graph, int limit)
  // {
  // try {
  // range_query_time = 0;
  // get_iterator_time = 0;
  // iterate_time = 0;
  // result_count = 0;
  // page_hit_count = 0;
  // String logWriteLine = "";
  //
  // //<spa_id, rectangle>
  // HashMap<Integer, MyRectangle> spa_predicates = new HashMap<Integer, MyRectangle>();
  //
  // //<spa_id, <neighbor_id, hop_num>>
  // HashMap<Integer, HashMap<Integer, Integer>> min_hop = new HashMap<Integer, HashMap<Integer,
  // Integer>>();
  // //<spa_id, <neighbor_id, size_property_name>>
  // HashMap<Integer, HashMap<Integer, String>> NL_size_propertyname = new HashMap<Integer,
  // HashMap<Integer, String>>();
  // //<spa_id, <neighbor_id, list_property_name>>
  // HashMap<Integer, HashMap<Integer, String>> NL_list_propertyname = new HashMap<Integer,
  // HashMap<Integer,String>>();
  // //<spa_id, <neighbor_id, NL_list>>
  // HashMap<Integer, HashMap<Integer, HashSet<Integer>>> NL_list = new HashMap<Integer,
  // HashMap<Integer, HashSet<Integer>>>();
  //
  // //Construct min_hop_array for the query graph
  // int[][] min_hop_array = Ini_Minhop(query_Graph);
  //
  // //Construct min_hop (compact structure for min_hop_array
  // //and NL_size_propertyname and NL_list_propertyname
  // for (int i = 0; i < query_Graph.Has_Spa_Predicate.length; i++)
  // if(query_Graph.Has_Spa_Predicate[i])
  // {
  // spa_predicates.put(i, query_Graph.spa_predicate[i]);
  // min_hop.put(i, new HashMap<Integer, Integer>());
  // NL_size_propertyname.put(i, new HashMap<Integer, String>());
  // NL_list_propertyname.put(i, new HashMap<Integer, String>());
  // for ( int j = 0; j < min_hop_array[i].length; j++)
  // {
  // if(min_hop_array[i][j] <= MAX_HOPNUM && min_hop_array[i][j] != -1)
  // {
  // min_hop.get(i).put(j, min_hop_array[i][j]);
  // int label = query_Graph.label_list[j];
  // NL_size_propertyname.get(i).put(j, String.format("NL_%d_%d_size", min_hop_array[i][j], label));
  // NL_list_propertyname.get(i).put(j, String.format("NL_%d_%d_list", min_hop_array[i][j], label));
  // }
  // }
  // }
  //
  // logWriteLine = String.format("min_hop: %s\nNL_property: %s", min_hop, NL_size_propertyname);
  // if ( outputLevelInfo)
  // {
  // OwnMethods.Print(logWriteLine);
  // OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
  // }
  //
  // long start = System.currentTimeMillis();
  // Transaction tx = dbservice.beginTx();
  // LinkedList<Node> cur_list = new LinkedList<Node>();
  // Node root_node = OSM_Utility.getRTreeRoot(dbservice, dataset);
  // cur_list.add(root_node);
  // LinkedList<Node> next_list = new LinkedList<Node>();
  //
  // int level_index = 0;
  // while(cur_list.isEmpty() == false)
  // {
  // //<spa_id, card>
  // HashMap<Integer, Double> spa_cards = new HashMap<Integer, Double>();
  // for (int key : spa_predicates.keySet())
  // spa_cards.put(key, 0.0);
  //
  // //<spa_id, <neighbor_id, card>>
  // HashMap<Integer, HashMap<Integer, Double>> NL_cards = new HashMap<Integer, HashMap<Integer,
  // Double>>();
  // for (int spa_id : min_hop.keySet())
  // {
  // NL_cards.put(spa_id, new HashMap<Integer, Double>());
  // HashMap<Integer, Integer> min_hop_vector = min_hop.get(spa_id);
  // for ( int neighbor_pos : min_hop_vector.keySet())
  // NL_cards.get(spa_id).put(neighbor_pos, 0.0);
  // }
  //
  // //<spa_id, overlap_nodes_list>
  // // HashMap<Integer, LinkedList<Node>> overlap_MBR_list = new HashMap<>();
  // LinkedList<Node>overlap_MBR_list = new LinkedList<Node>(); //just support one spatial predicate
  //
  // Iterator<Node> iterator = cur_list.iterator();
  //
  // while(iterator.hasNext())
  // {
  // Node node = iterator.next();
  // if(node.hasProperty("bbox"))
  // {
  // double[] bbox = (double[]) node.getProperty("bbox");
  // MyRectangle MBR = new MyRectangle(bbox[0], bbox[1], bbox[2], bbox[3]);
  //// OwnMethods.Print(MBR);
  // double MBR_area = MBR.area();
  //
  // int spa_count = (Integer) node.getProperty("count");
  // for ( int key : spa_predicates.keySet())
  // {
  // MyRectangle queryRectangle = spa_predicates.get(key);
  //
  // MyRectangle intersect = MBR.intersect(queryRectangle);
  // if(intersect != null)
  // {
  // // overlap_MBR_list.get(key).add(node);
  // overlap_MBR_list.add(node);
  //
  // double ratio;
  // if(MBR_area == 0)
  // ratio = 1;
  // else
  // ratio = intersect.area() / MBR_area;
  // spa_cards.put(key, (spa_cards.get(key) + ratio * spa_count));
  //
  // HashMap<Integer, Double> NL_cards_vector = NL_cards.get(key);
  // for ( int neighbor_id : NL_cards_vector.keySet())
  // {
  // int NL_label_size = (Integer) node.getProperty(NL_size_propertyname.get(key).get(neighbor_id));
  // NL_cards_vector.put(neighbor_id, (NL_cards_vector.get(neighbor_id) + ratio * NL_label_size));
  // }
  //
  // Iterable<Relationship> rels = node.getRelationships(RTreeRelationshipTypes.RTREE_CHILD,
  // Direction.OUTGOING);
  // for ( Relationship relationship : rels)
  // next_list.add(relationship.getEndNode());
  // }
  // }
  // }
  // else
  // throw new Exception(String.format("node %d does not has \"bbox\" property", node));
  // }
  //
  // logWriteLine = String.format("level %d", level_index);
  // if ( outputLevelInfo)
  // {
  // OwnMethods.Print(logWriteLine);
  // OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
  // }
  //
  // //find the query node with the minimum cardinality
  // double min_spa_card = Double.MAX_VALUE, min_NL_card = Double.MAX_VALUE;
  // int min_NL_spa_id = 0, min_NL_neighbor_id = 0;
  //
  // //now spa_cards has only one key
  // for (int key : spa_cards.keySet())
  // {
  // double spa_card = spa_cards.get(key);
  // if(spa_card < min_spa_card)
  // min_spa_card = spa_card;
  //
  // logWriteLine = String.format("spa_card %d %f", key, spa_card);
  // if ( outputLevelInfo)
  // {
  // OwnMethods.Print(logWriteLine);
  // OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
  // }
  //
  // HashMap<Integer, Double> NL_cards_vector = NL_cards.get(key);
  // for ( int neighbor_id : NL_cards_vector.keySet())
  // {
  // double NL_card = NL_cards_vector.get(neighbor_id);
  // if(NL_card < min_NL_card)
  // {
  // min_NL_spa_id = key; min_NL_neighbor_id = neighbor_id;
  // min_NL_card = NL_card;
  // }
  // logWriteLine = String.format("NL_size %d %d %s", key, neighbor_id,
  // NL_cards_vector.get(neighbor_id).toString());
  // if ( outputLevelInfo)
  // {
  // OwnMethods.Print(logWriteLine);
  // OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
  // }
  // }
  // }
  //
  // logWriteLine = String.format("level %d min card : %f", level_index, Math.min(min_spa_card,
  // min_NL_card));
  // if ( outputLevelInfo)
  // {
  // OwnMethods.Print(logWriteLine);
  // OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
  // }
  //
  // if (overlap_MBR_list.isEmpty() == true)
  // {
  // OwnMethods.Print("No result satisfy the query.");
  // return;
  // }
  //
  // //construct the NL_list with the highest selectivity
  // long start1 = System.currentTimeMillis();
  // // if ( min_NL_card < min_spa_card)
  //// {
  // String property_name = NL_list_propertyname.get(min_NL_spa_id).get(min_NL_neighbor_id);
  //
  // HashSet<Integer> min_NL_list = new HashSet<Integer>();
  // // NL_list.put(min_NL_spa_id, new HashMap<Integer, HashSet<Integer>>());
  // // NL_list.get(min_NL_spa_id).put(min_NL_neighbor_id, new HashSet<Integer>());
  // for ( Node node : overlap_MBR_list)
  // {
  // if ( node.hasProperty(property_name))
  // {
  // int[] NL_list_label = ( int[] ) node.getProperty(property_name);
  // for ( int node_id : NL_list_label)
  // min_NL_list.add(node_id);
  // }
  // }
  // logWriteLine = String.format("min_NL_list size is %d\n", min_NL_list.size());
  // if ( min_NL_list.size() < min_spa_card)
  // logWriteLine += "NL_list is more selective";
  // else
  // logWriteLine += "spa predicate is more selective";
  // if ( outputLevelInfo)
  // {
  // OwnMethods.Print(logWriteLine);
  // OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
  // }
  //// }
  //
  // logWriteLine = String.format("NL_serialize time: %d\n", System.currentTimeMillis() - start1);
  // logWriteLine += String.format("level %d time: %d\n", level_index, System.currentTimeMillis() -
  // start);
  // if ( outputLevelInfo)
  // {
  // OwnMethods.Print(logWriteLine);
  // OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
  // }
  //
  // range_query_time += System.currentTimeMillis() - start;
  // start = System.currentTimeMillis();
  //
  // //traverse to the second deepest level and start to form the cypher query
  //// int located_in_count = 0;
  // if( overlap_MBR_list.isEmpty() == false && next_list.isEmpty())
  // {
  // //get located in nodes
  //// TreeSet<Long> ids = new TreeSet<Long>();
  //// start = System.currentTimeMillis();
  //// for ( Node node : overlap_MBR_list)
  //// for ( Relationship relationship : node.getRelationships(
  //// Direction.OUTGOING, RTreeRelationshipTypes.RTREE_REFERENCE))
  //// {
  //// Node geom = relationship.getEndNode();
  //// double[] bbox = (double[]) geom.getProperty("bbox");
  //// MyRectangle rectangle = new MyRectangle(bbox);
  //// for ( int key : spa_predicates.keySet())
  //// if ( rectangle.intersect(spa_predicates.get(key)) != null)
  //// {
  //// ids.add(geom.getId());
  //// located_in_count++;
  //// }
  //// }
  //// OwnMethods.Print(String.format("Located in nodes: %d", located_in_count));
  //// level_index++;
  //// OwnMethods.Print(String.format("level %d time: %d", level_index, System.currentTimeMillis() -
  // start));
  //// return ids;
  //
  // int index = 0;
  // ArrayList<Long> id_pos_list = new ArrayList<Long>();
  // for ( int id : min_NL_list)
  // {
  // //graph id to neo4j pos id
  // id_pos_list.add(graph_pos_map_list[id]);
  // index ++;
  // if ( index == 500)
  // {
  // String query = formSubgraphQuery(query_Graph, -1, Explain_Or_Profile.Profile, spa_predicates,
  // min_NL_neighbor_id, id_pos_list);
  //
  // if ( outputQuery)
  // {
  // OwnMethods.Print(query);
  // OwnMethods.WriteFile(logPath, true, query + "\n");
  // }
  //
  // start = System.currentTimeMillis();
  // Result result = dbservice.execute(query);
  // get_iterator_time += System.currentTimeMillis() - start;
  //
  // start = System.currentTimeMillis();
  // int cur_count = 0;
  // while (result.hasNext())
  // {
  // cur_count++;
  // result.next();
  // }
  // iterate_time += System.currentTimeMillis() - start;
  //
  // if ( cur_count!= 0)
  // {
  // ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
  // ExecutionPlanDescription.ProfilerStatistics profile = planDescription.getProfilerStatistics();
  // result_count += profile.getRows();
  // page_hit_count += OwnMethods.GetTotalDBHits(planDescription);
  // if ( outputExecutionPlan)
  // {
  // OwnMethods.Print(planDescription);
  // OwnMethods.WriteFile(logPath, true, planDescription.toString() + "\n");
  // }
  // }
  // index = 0; id_pos_list = new ArrayList<Long>();
  // }
  // }
  //
  // logWriteLine = "id list size: " + id_pos_list.size();
  //
  // if ( outputLevelInfo)
  // {
  // OwnMethods.Print(logWriteLine);
  // OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
  // }
  //
  // if ( id_pos_list.size() != 0)
  // {
  // String query = formSubgraphQuery(query_Graph, -1, Explain_Or_Profile.Profile, spa_predicates,
  // min_NL_neighbor_id, id_pos_list);
  //
  // if ( outputQuery)
  // {
  // OwnMethods.Print(query);
  // OwnMethods.WriteFile(logPath, true, query + "\n");
  // }
  //
  // start = System.currentTimeMillis();
  // Result result = dbservice.execute(query);
  // get_iterator_time += System.currentTimeMillis() - start;
  //
  // start = System.currentTimeMillis();
  // int cur_count = 0;
  // while (result.hasNext())
  // {
  // cur_count++;
  // result.next();
  // }
  // iterate_time += System.currentTimeMillis() - start;
  //
  // if ( cur_count!= 0)
  // {
  // ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
  // ExecutionPlanDescription.ProfilerStatistics profile = planDescription.getProfilerStatistics();
  // result_count += profile.getRows();
  // page_hit_count += OwnMethods.GetTotalDBHits(planDescription);
  //
  // if ( outputExecutionPlan)
  // {
  // OwnMethods.Print(planDescription);
  // OwnMethods.WriteFile(logPath, true, planDescription.toString() + "\n");
  // }
  // }
  // }
  // }
  //
  // cur_list = next_list;
  // next_list = new LinkedList<Node>();
  // level_index++;
  // }
  // tx.success();
  // tx.close();
  //
  // } catch (Exception e) {
  // e.printStackTrace(); System.exit(-1);
  // }
  //// return null;
  // }

  /**
   * Does not consider condition that area of query rectangle is 0. If so, such function cannot make
   * the correct dicision.
   * 
   * @param query_Graph
   * @param limit
   */
  public void Query(Query_Graph query_Graph, int limit) {
    try {
      range_query_time = 0;
      get_iterator_time = 0;
      iterate_time = 0;
      result_count = 0;
      page_hit_count = 0;
      String logWriteLine = "";

      // <spa_id, rectangle> all query rectangles
      HashMap<Integer, MyRectangle> spa_predicates = new HashMap<Integer, MyRectangle>();

      // <spa_id, <neighbor_id, hop_num>> hop num of each node in the query graph
      // with regard to each query node with spatial predicate
      HashMap<Integer, HashMap<Integer, Integer>> min_hop =
          new HashMap<Integer, HashMap<Integer, Integer>>();
      // <spa_id, <neighbor_id, size_property_name>>
      HashMap<Integer, HashMap<Integer, String>> NL_size_propertyname =
          new HashMap<Integer, HashMap<Integer, String>>();
      // <spa_id, <neighbor_id, list_property_name>>
      HashMap<Integer, HashMap<Integer, String>> NL_list_propertyname =
          new HashMap<Integer, HashMap<Integer, String>>();
      // <spa_id, <neighbor_id, NL_list>>
      HashMap<Integer, HashMap<Integer, HashSet<Integer>>> NL_list =
          new HashMap<Integer, HashMap<Integer, HashSet<Integer>>>();

      // Construct min_hop_array for the query graph
      int[][] min_hop_array = Ini_Minhop(query_Graph);

      // Construct min_hop (compact structure for min_hop_array
      // and NL_size_propertyname and NL_list_propertyname
      for (int i = 0; i < query_Graph.Has_Spa_Predicate.length; i++)
        if (query_Graph.Has_Spa_Predicate[i]) {
          spa_predicates.put(i, query_Graph.spa_predicate[i]);
          min_hop.put(i, new HashMap<Integer, Integer>());
          NL_size_propertyname.put(i, new HashMap<Integer, String>());
          NL_list_propertyname.put(i, new HashMap<Integer, String>());
          for (int j = 0; j < min_hop_array[i].length; j++) {
            if (min_hop_array[i][j] <= MAX_HOPNUM && min_hop_array[i][j] != -1) {
              min_hop.get(i).put(j, min_hop_array[i][j]);
              int label = query_Graph.label_list[j];
              NL_size_propertyname.get(i).put(j,
                  String.format("NL_%d_%d_size", min_hop_array[i][j], label));
              NL_list_propertyname.get(i).put(j,
                  String.format("NL_%d_%d_list", min_hop_array[i][j], label));
            }
          }
        }

      if (outputLevelInfo) {
        logWriteLine = String.format("min_hop: %s\nNL_property: %s", min_hop, NL_size_propertyname);
        Util.println(logWriteLine);
        OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
      }

      Transaction tx = dbservice.beginTx();
      LinkedList<Node> cur_list = new LinkedList<Node>();
      Node root_node = RTreeUtility.getRTreeRoot(dbservice, dataset);
      cur_list.add(root_node);
      LinkedList<Node> next_list = new LinkedList<Node>();

      int level_index = 0;
      while (cur_list.isEmpty() == false) {
        long startLevel = System.currentTimeMillis();// for the level time
        // <spa_id, card>
        HashMap<Integer, Double> spa_cards = new HashMap<Integer, Double>();
        for (int key : spa_predicates.keySet())
          spa_cards.put(key, 0.0);

        // <spa_id, <neighbor_id, card>>
        HashMap<Integer, HashMap<Integer, Double>> NL_cards =
            new HashMap<Integer, HashMap<Integer, Double>>();
        for (int spa_id : min_hop.keySet()) {
          NL_cards.put(spa_id, new HashMap<Integer, Double>());
          HashMap<Integer, Integer> min_hop_vector = min_hop.get(spa_id);
          for (int neighbor_pos : min_hop_vector.keySet())
            NL_cards.get(spa_id).put(neighbor_pos, 0.0);
        }

        // <spa_id, overlap_nodes_list>
        // HashMap<Integer, LinkedList<Node>> overlap_MBR_list = new HashMap<>();
        LinkedList<Node> overlap_MBR_list = new LinkedList<Node>(); // just support one spatial
                                                                    // predicate

        Iterator<Node> iterator = cur_list.iterator();
        while (iterator.hasNext()) {
          Node node = iterator.next();
          if (node.hasProperty("bbox")) {
            double[] bbox = (double[]) node.getProperty("bbox");
            MyRectangle MBR = new MyRectangle(bbox[0], bbox[1], bbox[2], bbox[3]);
            // OwnMethods.Print(MBR);
            double MBR_area = MBR.area();

            int spa_count = (Integer) node.getProperty("count");
            for (int key : spa_predicates.keySet()) {
              MyRectangle queryRectangle = spa_predicates.get(key);

              MyRectangle intersect = MBR.intersect(queryRectangle);
              if (intersect != null) {
                // all overlapped nodes
                // overlap_MBR_list.get(key).add(node);
                overlap_MBR_list.add(node);

                // calculate overlapped ratio compared to the MBR area
                double ratio;
                if (MBR_area == 0)
                  ratio = 1;
                else
                  ratio = intersect.area() / MBR_area;
                // estimate spatial predicate cardinality
                spa_cards.put(key, (spa_cards.get(key) + ratio * spa_count));

                // estimate NL cardinality
                HashMap<Integer, Double> NL_cards_vector = NL_cards.get(key);
                for (int neighbor_id : NL_cards_vector.keySet()) {
                  int NL_label_size =
                      (Integer) node.getProperty(NL_size_propertyname.get(key).get(neighbor_id));
                  NL_cards_vector.put(neighbor_id,
                      (NL_cards_vector.get(neighbor_id) + ratio * NL_label_size));
                }

                // record the next level tree nodes
                Iterable<Relationship> rels =
                    node.getRelationships(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING);
                for (Relationship relationship : rels)
                  next_list.add(relationship.getEndNode());
              }
            }
          } else
            throw new Exception(String.format("node %d does not has \"bbox\" property", node));
        }

        if (outputLevelInfo) {
          logWriteLine = String.format("level %d", level_index);
          Util.println(logWriteLine);
          OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
        }

        // find the query node with the minimum cardinality
        double min_spa_card = Double.MAX_VALUE, min_NL_card = Double.MAX_VALUE;
        int minSpaID = 0;// the spatial predicate with minimum cardinality
        int min_NL_spa_id = 0, min_NL_neighbor_id = 0;// min NL spatial id and neighbor id

        // now spa_cards has only one key
        for (int key : spa_cards.keySet()) {
          double spa_card = spa_cards.get(key);
          if (spa_card < min_spa_card) {
            minSpaID = key;
            min_spa_card = spa_card;
          }

          if (outputLevelInfo) {
            logWriteLine = String.format("spa_card %d %f", key, spa_card);
            Util.println(logWriteLine);
            OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
          }

          HashMap<Integer, Double> NL_cards_vector = NL_cards.get(key);
          for (int neighbor_id : NL_cards_vector.keySet()) {
            double NL_card = NL_cards_vector.get(neighbor_id);
            if (NL_card < min_NL_card) {
              min_NL_spa_id = key;
              min_NL_neighbor_id = neighbor_id;
              min_NL_card = NL_card;
            }
            if (outputLevelInfo) {
              logWriteLine = String.format("NL_size %d %d %s", key, neighbor_id,
                  NL_cards_vector.get(neighbor_id).toString());
              Util.println(logWriteLine);
              OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
            }
          }
        }

        if (outputLevelInfo) {
          logWriteLine = String.format("level %d min card : %f", level_index,
              Math.min(min_spa_card, min_NL_card));
          Util.println(logWriteLine);
          OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
        }

        if (overlap_MBR_list.isEmpty() == true) {
          Util.println("No result satisfy the query.");
          return;
        }

        // construct the NL_list with the highest selectivity
        long start1 = System.currentTimeMillis();
        String minPropertyName = NL_list_propertyname.get(min_NL_spa_id).get(min_NL_neighbor_id);

        HashSet<Integer> min_NL_list = new HashSet<Integer>();
        // NL_list.put(min_NL_spa_id, new HashMap<Integer, HashSet<Integer>>());
        // NL_list.get(min_NL_spa_id).put(min_NL_neighbor_id, new HashSet<Integer>());
        for (Node node : overlap_MBR_list) {
          if (node.hasProperty(minPropertyName)) {
            int[] NL_list_label = (int[]) node.getProperty(minPropertyName);
            for (int node_id : NL_list_label)
              min_NL_list.add(node_id);
          }
        }
        if (outputLevelInfo) {
          logWriteLine = String.format("min_NL_list size is %d\n", min_NL_list.size());
          if (min_NL_list.size() < min_spa_card)
            logWriteLine += "NL_list is more selective\n";
          else
            logWriteLine += "spa predicate is more selective\n";
          logWriteLine +=
              String.format("NL_serialize time: %d\n", System.currentTimeMillis() - start1);
          logWriteLine += String.format("level %d time: %d\n", level_index,
              System.currentTimeMillis() - startLevel);
          Util.println(logWriteLine);
          OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
        }
        range_query_time += System.currentTimeMillis() - startLevel;

        // traverse to the second deepest level and start to form the cypher query
        if (overlap_MBR_list.isEmpty() == false && next_list.isEmpty()) {
          // if NL is more selective than spatial predicate
          if (min_NL_list.size() < min_spa_card) {
            int index = 0;
            ArrayList<Long> id_pos_list = new ArrayList<Long>();
            for (int id : min_NL_list) {
              // graph id to neo4j pos id
              id_pos_list.add(graph_pos_map_list[id]);
              index++;
              if (index == 500) {
                String query = formSubgraphQuery(query_Graph, -1, Explain_Or_Profile.Profile,
                    spa_predicates, min_NL_neighbor_id, id_pos_list);
                if (outputQuery) {
                  Util.println(query);
                  OwnMethods.WriteFile(logPath, true, query + "\n");
                }

                long start = System.currentTimeMillis();
                Result result = dbservice.execute(query);
                get_iterator_time += System.currentTimeMillis() - start;

                start = System.currentTimeMillis();
                int cur_count = 0;
                while (result.hasNext()) {
                  cur_count++;
                  Map<String, Object> row = result.next();
                  if (outputResult)
                    Util.println(row);
                }
                iterate_time += System.currentTimeMillis() - start;

                result_count += cur_count;
                ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
                page_hit_count += OwnMethods.GetTotalDBHits(planDescription);
                if (outputExecutionPlan) {
                  Util.println(planDescription);

                  Util
                      .println("OwnMethods.gethits: " + OwnMethods.GetTotalDBHits(planDescription));
                  if (planDescription.hasProfilerStatistics())
                    Util.println("cypher statistics: "
                        + planDescription.getProfilerStatistics().getDbHits());
                  else
                    throw new Exception("planDescription has no statistics!!");

                  OwnMethods.WriteFile(logPath, true, planDescription.toString() + "\n");
                }
                index = 0;
                id_pos_list = new ArrayList<Long>();
              }
            }

            if (outputLevelInfo) {
              logWriteLine = "id list size: " + id_pos_list.size();
              Util.println(logWriteLine);
              OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
            }

            if (id_pos_list.size() != 0) {
              String query = formSubgraphQuery(query_Graph, -1, Explain_Or_Profile.Profile,
                  spa_predicates, min_NL_neighbor_id, id_pos_list);

              if (outputQuery) {
                Util.println(query);
                OwnMethods.WriteFile(logPath, true, query + "\n");
              }

              long start = System.currentTimeMillis();
              Result result = dbservice.execute(query);
              get_iterator_time += System.currentTimeMillis() - start;

              start = System.currentTimeMillis();
              int cur_count = 0;
              while (result.hasNext()) {
                cur_count++;
                Map<String, Object> row = result.next();
                if (outputResult)
                  Util.println(row);
              }
              iterate_time += System.currentTimeMillis() - start;

              result_count += cur_count;
              ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
              page_hit_count += OwnMethods.GetTotalDBHits(planDescription);
              if (outputExecutionPlan) {
                Util.println(planDescription);

                Util.println("OwnMethods.gethits: " + OwnMethods.GetTotalDBHits(planDescription));
                if (planDescription.hasProfilerStatistics())
                  Util.println(
                      "cypher statistics: " + planDescription.getProfilerStatistics().getDbHits());
                else
                  throw new Exception("planDescription has no statistics!!");

                OwnMethods.WriteFile(logPath, true, planDescription.toString() + "\n");
              }
            }
            return;
          } else // if spatial predicate is more selective
          {
            MyRectangle queryRect = spa_predicates.get(minSpaID);
            // get located in nodes
            int located_in_count = 0;
            int levelTime = 0;
            level_index++;
            for (Node node : overlap_MBR_list) {
              long start = System.currentTimeMillis();
              Iterable<Relationship> rels =
                  node.getRelationships(Direction.OUTGOING, Labels.RTreeRel.RTREE_REFERENCE);

              ArrayList<Long> ids = new ArrayList<Long>(100);
              for (Relationship relationship : rels) {
                Node geom = relationship.getEndNode();
                double[] bbox = (double[]) geom.getProperty("bbox");
                MyRectangle rectangle = new MyRectangle(bbox);
                if (rectangle.intersect(queryRect) != null) {
                  ids.add(geom.getId());
                  located_in_count++;
                }
              }
              long time = System.currentTimeMillis() - start;
              range_query_time += time;
              levelTime += time;

              if (ids.size() != 0) {
                start = System.currentTimeMillis();
                String query = formSubgraphQuery_ForSpatialFirst_Block(query_Graph, limit,
                    Explain_Or_Profile.Profile, spa_predicates, minSpaID, ids,
                    min_hop.get(minSpaID), node);
                if (outputQuery) {
                  Util.println(query);
                  OwnMethods.WriteFile(logPath, true, query + "\n");
                }

                Result result = dbservice.execute(query);
                get_iterator_time += System.currentTimeMillis() - start;

                start = System.currentTimeMillis();
                int cur_count = 0;
                while (result.hasNext()) {
                  cur_count++;
                  Map<String, Object> row = result.next();
                  if (outputResult)
                    Util.println(row);
                }
                iterate_time += System.currentTimeMillis() - start;

                result_count += cur_count;
                ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
                page_hit_count += OwnMethods.GetTotalDBHits(planDescription);
                if (outputExecutionPlan) {
                  Util.println(planDescription);

                  Util
                      .println("OwnMethods.gethits: " + OwnMethods.GetTotalDBHits(planDescription));
                  if (planDescription.hasProfilerStatistics())
                    Util.println("cypher statistics: "
                        + planDescription.getProfilerStatistics().getDbHits());
                  else
                    throw new Exception("planDescription has no statistics!!");

                  OwnMethods.WriteFile(logPath, true, planDescription.toString() + "\n");
                }
              }
            }
            if (outputLevelInfo) {
              logWriteLine = String.format("level %d\n", level_index);
              logWriteLine += String.format("Located in nodes: %d\n", located_in_count);
              logWriteLine += String.format("level %d time: %d", level_index, levelTime);
              Util.println(logWriteLine);
              OwnMethods.WriteFile(logPath, true, logWriteLine + "\n\n");
            }
            return;
          }
        }

        cur_list = next_list;
        next_list = new LinkedList<Node>();
        level_index++;
      }
      tx.success();
      tx.close();

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
    // return null;
  }

  /**
   * form the cypher query for MBR block
   * 
   * @param query_Graph
   * @param limit -1 is no limit
   * @param Explain_Or_Profile -1 is Explain; 1 is Profile; the rest is nothing
   * @param spa_predicates spatial predicates except the min_pos spatial predicate
   * @param pos query graph node id with the trigger spatial predicate
   * @param ids corresponding spatial graph node ids that in this MBR (neo4j pos id)
   * @param NL_hopnum shrunk query node <query_graph_id, hop_num>
   * @param node the rtree node stores NL_list information
   * @return
   */
  public String formSubgraphQuery_ForSpatialFirst_Block_HMBR(Query_Graph query_Graph, int limit,
      Explain_Or_Profile explain_Or_Profile, HashMap<Integer, MyRectangle> spa_predicates, int pos,
      ArrayList<Long> ids, HashMap<Integer, Integer> NL_hopnum, Node node, int[][] min_hop_array) {
    String query = "";
    switch (explain_Or_Profile) {
      case Profile:
        query += "profile match ";
        break;
      case Explain:
        query += "explain match ";
        break;
      case Nothing:
        query += "match ";
        break;
    }

    // label
    if (pos == 0 || NL_hopnum.containsKey(0))
      query += "(a0)";
    else
      query += String.format("(a0:GRAPH_%d)", query_Graph.label_list[0]);
    for (int i = 1; i < query_Graph.graph.size(); i++) {
      if (pos == i || NL_hopnum.containsKey(i))
        query += String.format(",(a%d)", i);
      else
        query += String.format(",(a%d:GRAPH_%d)", i, query_Graph.label_list[i]);
    }

    // edge
    for (int i = 0; i < query_Graph.graph.size(); i++) {
      for (int j = 0; j < query_Graph.graph.get(i).size(); j++) {
        int neighbor = query_Graph.graph.get(i).get(j);
        if (neighbor > i)
          query += String.format(",(a%d)-[:%s]-(a%d)", i, graphLinkLabelName, neighbor);
      }
    }

    query += " where\n";

    // spatial predicate
    for (int key : spa_predicates.keySet()) {
      if (key != pos) {
        MyRectangle qRect = spa_predicates.get(key);
        query += String.format(" %f <= a%d.%s <= %f ", qRect.min_x, key, lon_name, qRect.max_x);
        query +=
            String.format("and %f <= a%d.%s <= %f and", qRect.min_y, key, lat_name, qRect.max_y);
      }
    }

    query += "\n";

    // id
    query += String.format(" (id(a%d)=%d", pos, ids.get(0));
    if (ids.size() > 1)
      for (int i = 1; i < ids.size(); i++)
        query += String.format(" or id(a%d)=%d", pos, ids.get(i));
    // query += String.format(" id(a%d) in %s\n", pos, ids.toString());

    query += ")\n";
    Util.println(String.format("spa_ids size: %d", ids.size()));

    // HMBR
    // for ( int key : spa_predicates.keySet())
    // {
    // MyRectangle cur_rect = spa_predicates.get(key);
    // for ( int queryNodeID = 0; queryNodeID < query_Graph.graph.size(); queryNodeID++)
    // {
    // int minhop = min_hop_array[key][queryNodeID];
    // if ( minhop <= MAX_HMBRHOPNUM && minhop != -1)
    // {
    // query += String.format(" and a%d.HMBR_%d_%s <= %f", queryNodeID, minhop, minx_name,
    // cur_rect.max_x);
    // query += String.format(" and a%d.HMBR_%d_%s <= %f", queryNodeID, minhop, miny_name,
    // cur_rect.max_y);
    // query += String.format(" and a%d.HMBR_%d_%s >= %f", queryNodeID, minhop, maxx_name,
    // cur_rect.min_x);
    // query += String.format(" and a%d.HMBR_%d_%s >= %f", queryNodeID, minhop, maxy_name,
    // cur_rect.min_y);
    // }
    // }
    // }

    // NL_id_list
    for (int key : NL_hopnum.keySet()) {
      String id_list_size_property_name =
          String.format("NL_%d_%d_size", NL_hopnum.get(key), query_Graph.label_list[key]);
      int id_list_size = (Integer) node.getProperty(id_list_size_property_name);
      if (id_list_size > 0) // whether to use the shrunk label
        query = query.replaceFirst(String.format("a%d", key),
            String.format("a%d:GRAPH_%d", key, query_Graph.label_list[key]));
      else {
        String id_list_property_name =
            String.format("NL_%d_%d_list", NL_hopnum.get(key), query_Graph.label_list[key]);

        if (node.hasProperty(id_list_property_name) == false)
          return "match (n) where false return n";

        int[] graph_id_list = (int[]) node.getProperty(id_list_property_name);
        ArrayList<Long> pos_id_list = new ArrayList<Long>(graph_id_list.length);
        for (int i = 0; i < graph_id_list.length; i++)
          pos_id_list.add(graph_pos_map_list[graph_id_list[i]]);

        query += String.format(" and ( id(a%d) = %d", key, pos_id_list.get(0));
        for (int i = 1; i < pos_id_list.size(); i++)
          query += String.format(" or id(a%d) = %d", key, pos_id_list.get(i));
        query += " )\n";
        // query += String.format(" and id(a%d) in %s\n", key, pos_id_list.toString());
        Util.println(String.format("%s size is %d", id_list_property_name, pos_id_list.size()));
      }
    }

    // return
    query += " return id(a0)";
    for (int i = 1; i < query_Graph.graph.size(); i++)
      query += String.format(",id(a%d)", i);

    if (limit != -1)
      query += String.format(" limit %d", limit);

    return query;
  }

  /**
   * form the cypher query
   * 
   * @param query_Graph
   * @param limit -1 is no limit
   * @param Explain_Or_Profile
   * @param spa_predicates spatial predicates except for the min_pos spatial predicate
   * @param pos query graph node id with the trigger spatial predicate
   * @param id corresponding spatial graph node id (neo4j pos id)
   * @param NL_hopnum shrunk query node <query_graph_id, hop_num>
   * @param node the rtree node stores NL_list information
   * @return
   */
  public String formSubgraphQuery_HMBR(Query_Graph query_Graph, int limit,
      Explain_Or_Profile explain_Or_Profile, HashMap<Integer, MyRectangle> spa_predicates, int pos,
      ArrayList<Long> ids, int[][] min_hop_array) {
    String query = "";
    switch (explain_Or_Profile) {
      case Profile:
        query += "profile match ";
        break;
      case Explain:
        query += "explain match ";
        break;
      case Nothing:
        query += "match ";
        break;
    }

    // label
    query += String.format("(a0:GRAPH_%d)", query_Graph.label_list[0]);
    for (int i = 1; i < query_Graph.graph.size(); i++) {
      query += String.format(",(a%d:GRAPH_%d)", i, query_Graph.label_list[i]);
    }

    // edge
    for (int i = 0; i < query_Graph.graph.size(); i++) {
      for (int j = 0; j < query_Graph.graph.get(i).size(); j++) {
        int neighbor = query_Graph.graph.get(i).get(j);
        if (neighbor > i)
          query += String.format(",(a%d)-[:%s]-(a%d)", i, graphLinkLabelName, neighbor);
      }
    }

    query += " \n where ";

    // spatial predicate
    for (int key : spa_predicates.keySet()) {
      MyRectangle qRect = spa_predicates.get(key);
      query += String.format(" %f <= a%d.%s <= %f ", qRect.min_x, key, lon_name, qRect.max_x);
      query +=
          String.format("and %f <= a%d.%s <= %f and ", qRect.min_y, key, lat_name, qRect.max_y);
    }

    // id
    query += String.format("\n(id(a%d) = %d ", pos, ids.get(0));
    if (ids.size() > 1)
      for (int i = 1; i < ids.size(); i++)
        query += String.format("or id(a%d) = %d ", pos, ids.get(i));
    query += ")\n";

    // HMBR
    for (int key : spa_predicates.keySet()) {
      MyRectangle cur_rect = spa_predicates.get(key);
      for (int queryNodeID = 0; queryNodeID < query_Graph.graph.size(); queryNodeID++) {
        int minhop = min_hop_array[key][queryNodeID];
        if (minhop <= MAX_HMBRHOPNUM && minhop != -1) {
          query += String.format(" and a%d.HMBR_%d_%s <= %f", queryNodeID, minhop, minx_name,
              cur_rect.max_x);
          query += String.format(" and a%d.HMBR_%d_%s <= %f", queryNodeID, minhop, miny_name,
              cur_rect.max_y);
          query += String.format(" and a%d.HMBR_%d_%s >= %f", queryNodeID, minhop, maxx_name,
              cur_rect.min_x);
          query += String.format(" and a%d.HMBR_%d_%s >= %f", queryNodeID, minhop, maxy_name,
              cur_rect.min_y);
        }
      }
    }

    // NL_id_list
    // for ( int key : NL_hopnum.keySet())
    // {
    // String id_list_property_name = String.format("NL_%d_%d_list", NL_hopnum.get(key),
    // query_Graph.label_list[key]);
    //
    // if ( node.hasProperty(id_list_property_name) == false)
    // {
    // query = "match (n) where false return n";
    // return query;
    // }
    //
    // int[] graph_id_list = (int[]) node.getProperty(id_list_property_name);
    // ArrayList<Long> pos_id_list = new ArrayList<Long>(graph_id_list.length);
    // for ( int i = 0; i < graph_id_list.length; i++)
    // pos_id_list.add(graph_pos_map_list[graph_id_list[i]]);
    //
    // query += String.format(" and ( id(a%d) = %d", key, pos_id_list.get(0));
    // for ( int i = 1; i < pos_id_list.size(); i++)
    // query += String.format(" or id(a%d) = %d", key, pos_id_list.get(i));
    // query += " )\n";
    //// query += String.format(" and id(a%d) in %s", key, pos_id_list.toString());
    // }

    // return
    query += "\nreturn id(a0)";
    for (int i = 1; i < query_Graph.graph.size(); i++)
      query += String.format(",id(a%d)", i);

    if (limit != -1)
      query += String.format(" limit %d", limit);

    return query;
  }

  /**
   * Does not consider condition that area of query rectangle is 0. If so, such function cannot make
   * the correct dicision.
   * 
   * @param query_Graph
   * @param limit
   */
  public void QueryHMBR(Query_Graph query_Graph, int limit) {
    try {
      range_query_time = 0;
      get_iterator_time = 0;
      iterate_time = 0;
      result_count = 0;
      page_hit_count = 0;
      String logWriteLine = "";

      // <spa_id, rectangle> all query rectangles
      HashMap<Integer, MyRectangle> spa_predicates = new HashMap<Integer, MyRectangle>();

      // <spa_id, <neighbor_id, hop_num>> hop num of each node in the query graph
      // with regard to each query node with spatial predicate
      HashMap<Integer, HashMap<Integer, Integer>> min_hop =
          new HashMap<Integer, HashMap<Integer, Integer>>();
      // <spa_id, <neighbor_id, size_property_name>>
      HashMap<Integer, HashMap<Integer, String>> NL_size_propertyname =
          new HashMap<Integer, HashMap<Integer, String>>();
      // <spa_id, <neighbor_id, list_property_name>>
      HashMap<Integer, HashMap<Integer, String>> NL_list_propertyname =
          new HashMap<Integer, HashMap<Integer, String>>();
      // <spa_id, <neighbor_id, NL_list>>
      HashMap<Integer, HashMap<Integer, HashSet<Integer>>> NL_list =
          new HashMap<Integer, HashMap<Integer, HashSet<Integer>>>();

      // Construct min_hop_array for the query graph
      int[][] min_hop_array = Ini_Minhop(query_Graph);

      // Construct min_hop (compact structure for min_hop_array
      // and NL_size_propertyname and NL_list_propertyname
      for (int i = 0; i < query_Graph.Has_Spa_Predicate.length; i++)
        if (query_Graph.Has_Spa_Predicate[i]) {
          spa_predicates.put(i, query_Graph.spa_predicate[i]);
          min_hop.put(i, new HashMap<Integer, Integer>());
          NL_size_propertyname.put(i, new HashMap<Integer, String>());
          NL_list_propertyname.put(i, new HashMap<Integer, String>());
          for (int j = 0; j < min_hop_array[i].length; j++) {
            if (min_hop_array[i][j] <= MAX_HOPNUM && min_hop_array[i][j] != -1) {
              min_hop.get(i).put(j, min_hop_array[i][j]);
              int label = query_Graph.label_list[j];
              NL_size_propertyname.get(i).put(j,
                  String.format("NL_%d_%d_size", min_hop_array[i][j], label));
              NL_list_propertyname.get(i).put(j,
                  String.format("NL_%d_%d_list", min_hop_array[i][j], label));
            }
          }
        }

      if (outputLevelInfo) {
        logWriteLine = String.format("min_hop: %s\nNL_property: %s", min_hop, NL_size_propertyname);
        Util.println(logWriteLine);
        OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
      }

      Transaction tx = dbservice.beginTx();
      LinkedList<Node> cur_list = new LinkedList<Node>();
      Node root_node = RTreeUtility.getRTreeRoot(dbservice, dataset);
      cur_list.add(root_node);
      LinkedList<Node> next_list = new LinkedList<Node>();

      int level_index = 0;
      while (cur_list.isEmpty() == false) {
        long startLevel = System.currentTimeMillis();// for the level time
        // <spa_id, card>
        HashMap<Integer, Double> spa_cards = new HashMap<Integer, Double>();
        for (int key : spa_predicates.keySet())
          spa_cards.put(key, 0.0);

        // <spa_id, <neighbor_id, card>>
        HashMap<Integer, HashMap<Integer, Double>> NL_cards =
            new HashMap<Integer, HashMap<Integer, Double>>();
        for (int spa_id : min_hop.keySet()) {
          NL_cards.put(spa_id, new HashMap<Integer, Double>());
          HashMap<Integer, Integer> min_hop_vector = min_hop.get(spa_id);
          for (int neighbor_pos : min_hop_vector.keySet())
            NL_cards.get(spa_id).put(neighbor_pos, 0.0);
        }

        // <spa_id, overlap_nodes_list>
        // HashMap<Integer, LinkedList<Node>> overlap_MBR_list = new HashMap<>();
        LinkedList<Node> overlap_MBR_list = new LinkedList<Node>(); // just support one spatial
                                                                    // predicate

        Iterator<Node> iterator = cur_list.iterator();
        while (iterator.hasNext()) {
          Node node = iterator.next();
          if (node.hasProperty("bbox")) {
            double[] bbox = (double[]) node.getProperty("bbox");
            MyRectangle MBR = new MyRectangle(bbox[0], bbox[1], bbox[2], bbox[3]);
            // OwnMethods.Print(MBR);
            double MBR_area = MBR.area();

            int spa_count = (Integer) node.getProperty("count");
            for (int key : spa_predicates.keySet()) {
              MyRectangle queryRectangle = spa_predicates.get(key);

              MyRectangle intersect = MBR.intersect(queryRectangle);
              if (intersect != null) {
                // all overlapped nodes
                // overlap_MBR_list.get(key).add(node);
                overlap_MBR_list.add(node);

                // calculate overlapped ratio compared to the MBR area
                double ratio;
                if (MBR_area == 0)
                  ratio = 1;
                else
                  ratio = intersect.area() / MBR_area;
                // estimate spatial predicate cardinality
                spa_cards.put(key, (spa_cards.get(key) + ratio * spa_count));

                // estimate NL cardinality
                HashMap<Integer, Double> NL_cards_vector = NL_cards.get(key);
                for (int neighbor_id : NL_cards_vector.keySet()) {
                  int NL_label_size =
                      (Integer) node.getProperty(NL_size_propertyname.get(key).get(neighbor_id));
                  NL_cards_vector.put(neighbor_id,
                      (NL_cards_vector.get(neighbor_id) + ratio * NL_label_size));
                }

                // record the next level tree nodes
                Iterable<Relationship> rels =
                    node.getRelationships(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING);
                for (Relationship relationship : rels)
                  next_list.add(relationship.getEndNode());
              }
            }
          } else
            throw new Exception(String.format("node %d does not has \"bbox\" property", node));
        }

        if (outputLevelInfo) {
          logWriteLine = String.format("level %d", level_index);
          Util.println(logWriteLine);
          OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
        }

        // find the query node with the minimum cardinality
        double min_spa_card = Double.MAX_VALUE, min_NL_card = Double.MAX_VALUE;
        int minSpaID = 0;// the spatial predicate with minimum cardinality
        int min_NL_spa_id = 0, min_NL_neighbor_id = 0;// min NL spatial id and neighbor id

        // now spa_cards has only one key
        for (int key : spa_cards.keySet()) {
          double spa_card = spa_cards.get(key);
          if (spa_card < min_spa_card) {
            minSpaID = key;
            min_spa_card = spa_card;
          }

          if (outputLevelInfo) {
            logWriteLine = String.format("spa_card %d %f", key, spa_card);
            Util.println(logWriteLine);
            OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
          }

          HashMap<Integer, Double> NL_cards_vector = NL_cards.get(key);
          for (int neighbor_id : NL_cards_vector.keySet()) {
            double NL_card = NL_cards_vector.get(neighbor_id);
            if (NL_card < min_NL_card) {
              min_NL_spa_id = key;
              min_NL_neighbor_id = neighbor_id;
              min_NL_card = NL_card;
            }
            if (outputLevelInfo) {
              logWriteLine = String.format("NL_size %d %d %s", key, neighbor_id,
                  NL_cards_vector.get(neighbor_id).toString());
              Util.println(logWriteLine);
              OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
            }
          }
        }

        if (outputLevelInfo) {
          logWriteLine = String.format("level %d min card : %f", level_index,
              Math.min(min_spa_card, min_NL_card));
          Util.println(logWriteLine);
          OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
        }

        if (overlap_MBR_list.isEmpty() == true) {
          Util.println("No result satisfy the query.");
          return;
        }

        // construct the NL_list with the highest selectivity
        long start1 = System.currentTimeMillis();
        String minPropertyName = NL_list_propertyname.get(min_NL_spa_id).get(min_NL_neighbor_id);

        HashSet<Integer> min_NL_list = new HashSet<Integer>();
        // NL_list.put(min_NL_spa_id, new HashMap<Integer, HashSet<Integer>>());
        // NL_list.get(min_NL_spa_id).put(min_NL_neighbor_id, new HashSet<Integer>());
        for (Node node : overlap_MBR_list) {
          if (node.hasProperty(minPropertyName)) {
            int[] NL_list_label = (int[]) node.getProperty(minPropertyName);
            for (int node_id : NL_list_label)
              min_NL_list.add(node_id);
          }
        }
        if (outputLevelInfo) {
          logWriteLine = String.format("min_NL_list size is %d\n", min_NL_list.size());
          if (min_NL_list.size() < min_spa_card)
            logWriteLine += "NL_list is more selective\n";
          else
            logWriteLine += "spa predicate is more selective\n";
          logWriteLine +=
              String.format("NL_serialize time: %d\n", System.currentTimeMillis() - start1);
          logWriteLine += String.format("level %d time: %d\n", level_index,
              System.currentTimeMillis() - startLevel);
          Util.println(logWriteLine);
          OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
        }
        range_query_time += System.currentTimeMillis() - startLevel;

        // traverse to the second deepest level and start to form the cypher query
        if (overlap_MBR_list.isEmpty() == false && next_list.isEmpty()) {
          // if NL is more selective than spatial predicate
          if (min_NL_list.size() < min_spa_card) {
            int index = 0;
            ArrayList<Long> id_pos_list = new ArrayList<Long>();
            for (int id : min_NL_list) {
              // graph id to neo4j pos id
              id_pos_list.add(graph_pos_map_list[id]);
              index++;
              if (index == 500) {
                String query = formSubgraphQuery_HMBR(query_Graph, -1, Explain_Or_Profile.Profile,
                    spa_predicates, min_NL_neighbor_id, id_pos_list, min_hop_array);
                if (outputQuery) {
                  Util.println(query);
                  OwnMethods.WriteFile(logPath, true, query + "\n");
                }

                long start = System.currentTimeMillis();
                Result result = dbservice.execute(query);
                get_iterator_time += System.currentTimeMillis() - start;

                start = System.currentTimeMillis();
                int cur_count = 0;
                while (result.hasNext()) {
                  cur_count++;
                  Map<String, Object> row = result.next();
                  if (outputResult)
                    Util.println(row);
                }
                iterate_time += System.currentTimeMillis() - start;

                result_count += cur_count;
                ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
                page_hit_count += OwnMethods.GetTotalDBHits(planDescription);
                if (outputExecutionPlan) {
                  Util.println(planDescription);
                  OwnMethods.WriteFile(logPath, true, planDescription.toString() + "\n");
                }
                index = 0;
                id_pos_list = new ArrayList<Long>();
              }
            }

            if (outputLevelInfo) {
              logWriteLine = "id list size: " + id_pos_list.size();
              Util.println(logWriteLine);
              OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
            }

            if (id_pos_list.size() != 0) {
              String query = formSubgraphQuery_HMBR(query_Graph, -1, Explain_Or_Profile.Profile,
                  spa_predicates, min_NL_neighbor_id, id_pos_list, min_hop_array);

              if (outputQuery) {
                Util.println(query);
                OwnMethods.WriteFile(logPath, true, query + "\n");
              }

              long start = System.currentTimeMillis();
              Result result = dbservice.execute(query);
              get_iterator_time += System.currentTimeMillis() - start;

              start = System.currentTimeMillis();
              int cur_count = 0;
              while (result.hasNext()) {
                cur_count++;
                Map<String, Object> row = result.next();
                if (outputResult)
                  Util.println(row);
              }
              iterate_time += System.currentTimeMillis() - start;

              result_count += cur_count;
              ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
              page_hit_count += OwnMethods.GetTotalDBHits(planDescription);
              if (outputExecutionPlan) {
                Util.println(planDescription);
                OwnMethods.WriteFile(logPath, true, planDescription.toString() + "\n");
              }
            }
            return;
          } else // if spatial predicate is more selective
          {
            MyRectangle queryRect = spa_predicates.get(minSpaID);
            // get located in nodes
            int located_in_count = 0;
            int levelTime = 0;
            level_index++;
            for (Node node : overlap_MBR_list) {
              long start = System.currentTimeMillis();
              Iterable<Relationship> rels =
                  node.getRelationships(Direction.OUTGOING, Labels.RTreeRel.RTREE_REFERENCE);

              ArrayList<Long> ids = new ArrayList<Long>(100);
              for (Relationship relationship : rels) {
                Node geom = relationship.getEndNode();
                double[] bbox = (double[]) geom.getProperty("bbox");
                MyRectangle rectangle = new MyRectangle(bbox);
                if (rectangle.intersect(queryRect) != null) {
                  ids.add(geom.getId());
                  located_in_count++;
                }
              }
              long time = System.currentTimeMillis() - start;
              range_query_time += time;
              levelTime += time;

              if (ids.size() != 0) {
                start = System.currentTimeMillis();
                String query = formSubgraphQuery_ForSpatialFirst_Block_HMBR(query_Graph, limit,
                    Explain_Or_Profile.Profile, spa_predicates, minSpaID, ids,
                    min_hop.get(minSpaID), node, min_hop_array);
                if (outputQuery) {
                  Util.println(query);
                  OwnMethods.WriteFile(logPath, true, query + "\n");
                }

                Result result = dbservice.execute(query);
                get_iterator_time += System.currentTimeMillis() - start;

                start = System.currentTimeMillis();
                int cur_count = 0;
                while (result.hasNext()) {
                  cur_count++;
                  Map<String, Object> row = result.next();
                  if (outputResult)
                    Util.println(row);
                }
                iterate_time += System.currentTimeMillis() - start;

                result_count += cur_count;
                ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
                page_hit_count += OwnMethods.GetTotalDBHits(planDescription);
                if (outputExecutionPlan) {
                  Util.println(planDescription);
                  OwnMethods.WriteFile(logPath, true, planDescription.toString() + "\n");
                }
              }
            }
            if (outputLevelInfo) {
              logWriteLine = String.format("level %d\n", level_index);
              logWriteLine += String.format("Located in nodes: %d\n", located_in_count);
              logWriteLine += String.format("level %d time: %d", level_index, levelTime);
              Util.println(logWriteLine);
              OwnMethods.WriteFile(logPath, true, logWriteLine + "\n\n");
            }
            return;
          }
        }

        cur_list = next_list;
        next_list = new LinkedList<Node>();
        level_index++;
      }
      tx.success();
      tx.close();

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
    // return null;
  }

  public static void queryTest() {
    String db_path = "", querygraphDir = "", querygraph_path = "", graph_pos_map_path = "",
        log_path = "";
    int queryNodeCount = 3;
    int query_id = 0;
    Config config = new Config();
    String dataset = config.getDatasetName();
    system systemName = config.getSystemName();
    String neo4jVersion = config.GetNeo4jVersion();
    switch (systemName) {
      case Ubuntu:
        db_path = String.format(
            "/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db",
            neo4jVersion, dataset);
        querygraphDir =
            String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph/%s/", dataset);
        querygraph_path = String.format("%s%d.txt", querygraphDir, queryNodeCount);
        graph_pos_map_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/node_map.txt";
        log_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/test.log";
        break;
      case Windows:
        db_path = String.format(
            "D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset,
            neo4jVersion, dataset);
        querygraphDir =
            String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\query_graph\\%s\\", dataset);
        querygraph_path = String.format("%s%d.txt", querygraphDir, queryNodeCount);
        graph_pos_map_path = "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset + "\\node_map.txt";
        log_path = "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset + "\\test.log";
      default:
        break;
    }

    ArrayList<Query_Graph> queryGraphs = Util.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
    Query_Graph query_Graph = queryGraphs.get(query_id);
    query_Graph.spa_predicate[2] = new MyRectangle(-84.521118, 33.827220, -84.375996, 33.972342);// 1000,
                                                                                                 // 2
    // query_Graph.spa_predicate[2] = new MyRectangle(-84.468680, 33.879658, -84.428434,
    // 33.919904);//100
    // query_Graph.spa_predicate[1] = new MyRectangle(9.523183, 46.839041, 9.541593,
    // 46.857451);//100
    // query_Graph.spa_predicate[1] = new MyRectangle(-98.025157, 29.953977, -97.641747,
    // 30.337387);/10,000
    // query_Graph.spa_predicate[1] = new MyRectangle(-91.713778, 14.589395, -68.517838,
    // 37.785335);//100,000
    // query_Graph.spa_predicate[1] = new MyRectangle(-179.017757, -135.408325, 207.362849,
    // 250.972281);//1,000,000
    // query_Graph.spa_predicate[1] = new MyRectangle(-91.713778, 14.589395, -68.517838, 37.785335);
    // query_Graph.spa_predicate[3] = new MyRectangle(-98.025157, 29.953977, -97.641747, 30.337387);

    HashMap<String, String> graph_pos_map = OwnMethods.ReadMap(graph_pos_map_path);
    long[] graph_pos_map_list = new long[graph_pos_map.size()];
    for (String key_str : graph_pos_map.keySet()) {
      int key = Integer.parseInt(key_str);
      int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
      graph_pos_map_list[key] = pos_id;
    }

    try {
      RisoTreeQuery risoTreeQuery = new RisoTreeQuery(db_path, dataset, graph_pos_map_list);
      long start = System.currentTimeMillis();
      risoTreeQuery.Query(query_Graph, -1);
      Util.println("Time:" + (System.currentTimeMillis() - start));
      Util.println(String.format("result count:%d", risoTreeQuery.result_count));
      risoTreeQuery.dbservice.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    queryTest();

    // SpatialFirst spatialFirst = new SpatialFirst(db_path, dataset);
    // TreeSet<Long> result2 = spatialFirst.Query(query_Graph);
    // spatialFirst.shutdown();
    //
    // LinkedList<Long> counter1 = new LinkedList<Long>();
    // for ( long id : result1)
    // if(result2.contains(id) == false)
    // counter1.add(id);
    //
    // for ( long id : counter1)
    // OwnMethods.Print(id);
    // OwnMethods.Print(counter1.size());

    // LinkedList<Long> counter2 = new LinkedList<Long>();
    // for ( long id : result2)
    // if(result1.contains(id) == false)
    // counter2.add(id);
    //
    // for ( long id : counter2)
    // OwnMethods.Print(id);
    // OwnMethods.Print(counter2.size());

    // dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
    // tx = dbservice.beginTx();
    // for ( long id : counter1)
    // {
    // double[] bbox = (double[])dbservice.getNodeById(id).getProperty("bbox");
    // MyRectangle rectangle = new MyRectangle(bbox);
    //// for ( double element : bbox)
    //// OwnMethods.Print(element);
    // OwnMethods.Print(rectangle.toString());
    //// break;
    // }
  }

}
