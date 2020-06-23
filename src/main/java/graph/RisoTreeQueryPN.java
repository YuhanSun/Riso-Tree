package graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.logging.Logger;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import commons.ArrayUtil;
import commons.Config;
import commons.Enums;
import commons.Enums.Explain_Or_Profile;
import commons.Enums.QueryStatistic;
import commons.Enums.QueryType;
import commons.Labels;
import commons.Labels.RTreeRel;
import commons.MyPoint;
import commons.MyRectangle;
import commons.Neo4jGraphUtility;
import commons.OwnMethods;
import commons.QueryUtil;
import commons.Query_Graph;
import commons.Query_Graph.LabelType;
import commons.RTreeUtility;
import commons.RisoTreeUtil;
import commons.Util;
import cypher.middleware.CypherDecoder;
import cypher.middleware.CypherEncoder;
import knn.Element;
import knn.KNNComparator;
import knn.NodeAndRec;

/**
 * Use path neighbors to organize reachable subgraphs.
 * 
 * @author ysun138
 *
 */
public class RisoTreeQueryPN {

  private static final Logger LOGGER = Logger.getLogger(RisoTreeQueryPN.class.getName());

  public GraphDatabaseService dbservice;
  public String dataset;
  public long[] graph_pos_map_list;

  public static Config config = new Config();
  public static String lon_name = config.GetLongitudePropertyName();
  public static String lat_name = config.GetLatitudePropertyName();
  public static String graphLinkLabelName = Labels.GraphRel.GRAPH_LINK.name();
  public final static String PNPrefix = Config.PNPrefix;
  public final static String PNSizePrefix = Config.PNSizePrefix;

  public int MAX_HOPNUM;
  // HMBR
  public int MAX_HMBRHOPNUM = config.getMaxHMBRHopNum();
  String minx_name = config.GetRectCornerName()[0];
  String miny_name = config.GetRectCornerName()[1];
  String maxx_name = config.GetRectCornerName()[2];
  String maxy_name = config.GetRectCornerName()[3];

  // shared query statistics for range, join, knn queries
  public long run_time;
  public long get_iterator_time;
  public long iterate_time;
  public long result_count;
  public long page_hit_count;
  public long check_paths_time;

  // LAGAQ-Range
  public long range_query_time;
  public long set_label_time;
  public long remove_label_time;

  public long overlap_leaf_node_count;
  public long candidate_count;

  public String logPath;
  public ExecutionPlanDescription planDescription;

  // for knn query track
  public long queue_time;
  public int visit_spatial_object_count;

  // for join track
  public long join_result_count;
  public long join_time;
  public long check_overlap_time;

  public Map<QueryStatistic, Object> queryStatisticMap = new HashMap<>();

  // test control variables
  public static final boolean outputLevelInfo = true;
  public static final boolean outputQuery = true;
  public static final boolean outputExecutionPlan = false;
  public static final boolean outputResult = false;

  // method control
  public static boolean forceGraphFirst = true;
  public final static boolean completeStrategyUsed = true;// whether to use the complete approach
  public final static boolean selectivityEstimate = true;
  public static Boolean candidateComplete = null;
  public static Map<Integer, MutableBoolean[]> queryNodesComplete = null;
  public final static int candidateSetsSizeLimit = 2000;
  public final static boolean joinBatch = true;
  public final static int joinBatchSize = 50;

  public RisoTreeQueryPN(String db_path, String p_dataset, long[] p_graph_pos_map, int pMAXHOPNUM,
      boolean forceGraphFirst) {

    dbservice = Neo4jGraphUtility.getDatabaseService(db_path);
    dataset = p_dataset;
    graph_pos_map_list = p_graph_pos_map;
    // logPath = String.format("/mnt/hgfs/Experiment_Result/Riso-Tree/%s/query.log", dataset);
    logPath =
        String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s\\query.log", dataset);
    // if (!OwnMethods.pathExist(logPath))
    // {
    // OwnMethods.Print(logPath + " does not exist");
    // System.exit(-1);
    // }
    MAX_HOPNUM = pMAXHOPNUM;
    this.forceGraphFirst = forceGraphFirst;
  }

  /**
   * initialize
   * 
   * @param db_path database path ./graph.db
   * @param p_dataset dataset name
   * @param p_graph_pos_map the map from graph id to neo4j pos id
   */
  public RisoTreeQueryPN(String db_path, String p_dataset, long[] p_graph_pos_map, int pMAXHOPNUM) {

    dbservice = Neo4jGraphUtility.getDatabaseService(db_path);
    dataset = p_dataset;
    graph_pos_map_list = p_graph_pos_map;
    // logPath = String.format("/mnt/hgfs/Experiment_Result/Riso-Tree/%s/query.log", dataset);
    logPath =
        String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s\\query.log", dataset);
    // if (!OwnMethods.pathExist(logPath))
    // {
    // OwnMethods.Print(logPath + " does not exist");
    // System.exit(-1);
    // }
    MAX_HOPNUM = pMAXHOPNUM;
  }

  public RisoTreeQueryPN(GraphDatabaseService service, String p_dataset, long[] p_graph_pos_map,
      int pMAXHOPNUM) {
    this.dbservice = service;
    dataset = p_dataset;
    graph_pos_map_list = p_graph_pos_map;
    logPath =
        String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s\\query.log", dataset);
    MAX_HOPNUM = pMAXHOPNUM;
  }

  public RisoTreeQueryPN(GraphDatabaseService service, String dataset, int MAX_HOPNUM) {
    this.dbservice = service;
    this.dataset = dataset;
    this.MAX_HOPNUM = MAX_HOPNUM;
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
   * Generate the paths like _%s_%s if MAX_HOPNUM = 2. The paths are different for different
   * labelType of the query graph.
   *
   * @param visited
   * @param paths contain the paths for return
   * @param stack
   * @param queryGraph
   */
  private void traverse(boolean[] visited, HashMap<Integer, HashSet<String>> paths,
      Stack<Integer> stack, Query_Graph queryGraph) {
    if (queryGraph.labelType.equals(LabelType.INT)) {
      traverseInt(visited, paths, stack, queryGraph);
    } else {
      traverseString(visited, paths, stack, queryGraph);
    }
  }

  private void traverseString(boolean[] visited, HashMap<Integer, HashSet<String>> paths,
      Stack<Integer> stack, Query_Graph queryGraph) {
    // Does not reach the limit
    if (stack.size() <= MAX_HOPNUM) {
      int top = stack.peek();
      if (stack.size() >= 1) {
        // maintainPaths(stack, queryGraph, paths);
        String path = PNPrefix;
        for (int i = 0; i < stack.size(); i++) {
          int id = stack.get(i);
          path = RisoTreeUtil.getAttachName(path, queryGraph.getNodeLabelString(id));
          if (paths.containsKey(id))
            paths.get(id).add(path);
          else {
            paths.put(id, new HashSet<String>());
            paths.get(id).add(path);
          }
        }
      }

      ArrayList<Integer> topNeighbors = queryGraph.graph.get(top);
      for (int i = 0; i < topNeighbors.size(); i++) {
        int neighbor = topNeighbors.get(i);
        if (visited[neighbor] == false) {
          stack.push(neighbor);
          visited[neighbor] = true;
          traverse(visited, paths, stack, queryGraph);
        }
        if (i == topNeighbors.size() - 1) {
          stack.pop();
          visited[top] = false;
        }
      }
    } else if (stack.size() == MAX_HOPNUM + 1) {// just reach the limit
      String path = PNPrefix;
      for (int i = 0; i < stack.size(); i++) {
        int id = stack.get(i);
        path = RisoTreeUtil.getAttachName(path, queryGraph.getNodeLabelString(id));
      }
      int top = stack.pop();
      visited[top] = false;
      if (paths.containsKey(top))
        paths.get(top).add(path);
      else {
        paths.put(top, new HashSet<String>());
        paths.get(top).add(path);
      }
      return;
    }
  }

  private void maintainPaths(Stack<Integer> stack, Query_Graph queryGraph,
      HashMap<Integer, HashSet<String>> paths) {
    // TODO Auto-generated method stub

  }

  private String buildPath(Stack<Integer> stack, Query_Graph queryGraph) {
    // TODO Auto-generated method stub
    return null;
  }

  private void traverseInt(boolean[] visited, HashMap<Integer, HashSet<String>> paths,
      Stack<Integer> stack, Query_Graph queryGraph) {
    if (stack.size() <= MAX_HOPNUM) {
      int top = stack.peek();

      if (stack.size() >= 2) {
        String path = PNPrefix;
        for (int i = 1; i < stack.size(); i++) {
          int id = stack.get(i);
          path += String.format("_%d", queryGraph.label_list[id]);
          if (paths.containsKey(top))
            paths.get(top).add(path);
          else {
            paths.put(top, new HashSet<String>());
            paths.get(top).add(path);
          }
        }
      }

      ArrayList<Integer> topNeighbors = queryGraph.graph.get(top);
      for (int i = 0; i < topNeighbors.size(); i++) {
        int neighbor = topNeighbors.get(i);
        if (visited[neighbor] == false) {
          stack.push(neighbor);
          visited[neighbor] = true;
          traverse(visited, paths, stack, queryGraph);
        }
        if (i == topNeighbors.size() - 1) {
          stack.pop();
          visited[top] = false;
        }
      }
    } else if (stack.size() == MAX_HOPNUM + 1) {
      String path = PNPrefix;
      for (int i = 1; i < stack.size(); i++) {
        int id = stack.get(i);
        path += String.format("_%d", queryGraph.label_list[id]);
      }
      int top = stack.pop();
      visited[top] = false;
      if (paths.containsKey(top))
        paths.get(top).add(path);
      else {
        paths.put(top, new HashSet<String>());
        paths.get(top).add(path);
      }
      return;
    }
  }

  /**
   * Recognize paths in the query Graph. The paths are different for INT or STRING labe in the query
   * graph. It can handle the multiple-spatial predicate case.
   *
   * @param queryGraph
   * @return
   */
  public HashMap<Integer, HashMap<Integer, HashSet<String>>> recognizePaths(
      Query_Graph queryGraph) {
    HashMap<Integer, HashMap<Integer, HashSet<String>>> spaPathsMap =
        new HashMap<Integer, HashMap<Integer, HashSet<String>>>();

    for (int i = 0; i < queryGraph.graph.size(); i++) {
      // for each spatial node, generate the paths
      if (queryGraph.Has_Spa_Predicate[i]) {
        if (MAX_HOPNUM == 0)
          spaPathsMap.put(i, new HashMap<Integer, HashSet<String>>());
        else {
          HashMap<Integer, HashSet<String>> paths = new HashMap<Integer, HashSet<String>>();
          boolean[] visited = new boolean[queryGraph.graph.size()];
          for (int j = 0; j < visited.length; j++)
            visited[j] = false;
          Stack<Integer> stack = new Stack<Integer>();
          stack.push(i);
          visited[i] = true;
          traverse(visited, paths, stack, queryGraph);
          spaPathsMap.put(i, paths);
        }
      }
    }

    return spaPathsMap;
  }

  /**
   * form the cypher query
   * 
   * @param query_Graph
   * @param limit -1 is no limit
   * @param Explain_Or_Profile
   * @param spa_predicates spatial predicates except for the min_pos spatial predicate
   * @param pos query graph node id with the trigger spatial predicate
   * @param ids corresponding graph spatial vertex id (neo4j pos id)
   * @return
   */
  public String formSubgraphQuery(Query_Graph query_Graph, int limit,
      Enums.Explain_Or_Profile explain_Or_Profile, HashMap<Integer, MyRectangle> spa_predicates,
      int pos, ArrayList<Long> ids) {
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
      Enums.Explain_Or_Profile explain_Or_Profile, HashMap<Integer, MyRectangle> spa_predicates,
      int pos, ArrayList<Long> ids, HashMap<Integer, Integer> NL_hopnum, Node node) {
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
    // if ( pos == 0 || NL_hopnum.containsKey(0))
    if (pos == 0)
      query += "(a0)";
    else
      query += String.format("(a0:GRAPH_%d)", query_Graph.label_list[0]);
    for (int i = 1; i < query_Graph.graph.size(); i++) {
      // if ( pos == i || NL_hopnum.containsKey(i))
      if (pos == i)
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
    // for ( int key : NL_hopnum.keySet())
    // {
    // String id_list_size_property_name = String.format("NL_%d_%d_size", NL_hopnum.get(key),
    // query_Graph.label_list[key]);
    // int id_list_size = (Integer) node.getProperty(id_list_size_property_name);
    // if( id_list_size > 0) //whether to use the shrunk label
    // query = query.replaceFirst(String.format("a%d", key), String.format("a%d:GRAPH_%d", key,
    // query_Graph.label_list[key]));
    // else {
    // String id_list_property_name = String.format("NL_%d_%d_list", NL_hopnum.get(key),
    // query_Graph.label_list[key]);
    //
    // if ( node.hasProperty(id_list_property_name) == false)
    // return "match (n) where false return n";
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
    //// query += String.format(" and id(a%d) in %s\n", key, pos_id_list.toString());
    // OwnMethods.Print(String.format("%s size is %d", id_list_property_name, pos_id_list.size()));
    // }
    // }

    // return
    query += " return id(a0)";
    for (int i = 1; i < query_Graph.graph.size(); i++)
      query += String.format(",id(a%d)", i);

    if (limit != -1)
      query += String.format(" limit %d", limit);

    return query;
  }

  private Query_Graph query_Graph = null;


  /**
   * Determine the query plan according to the {@code completeStrategyUsed} value.
   *
   * @param query
   * @throws Exception
   */
  public void queryWithIgnore(String query) throws Exception {
    Query_Graph query_Graph = CypherDecoder.getQueryGraph(query, dbservice);
    this.query_Graph = query_Graph;
    candidateComplete = null;
    queryNodesComplete = new HashMap<>();
    for (int i = 0; i < query_Graph.Has_Spa_Predicate.length; i++) {
      if (query_Graph.Has_Spa_Predicate[i]) {
        queryNodesComplete.put(i, new MutableBoolean[query_Graph.graph.size()]);
      }
    }

    // queryWithIgnore(query, query_Graph);
    queryWithIgnoreNewLabel(query, query_Graph);
  }

  /**
   * Combined for complete strategy used or not.
   *
   * @param query
   * @param candidateSets
   * @param query_Graph
   * @return
   */
  public String formQueryWithIgnoreNewLabelCombined(String query,
      Map<Integer, Collection<Long>> candidateSets, Query_Graph query_Graph) {
    String queryAfterRewrite = null;
    if (completeStrategyUsed) {
      if (candidateComplete == null) {
        throw new RuntimeException("candidate complete is null!");
      } else if (candidateComplete) {
        queryAfterRewrite = formQueryWithIgnoreNewLabelComplete(query, candidateSets, query_Graph);
      } else {
        queryAfterRewrite = formQueryWithIgnoreNewLabel(query, candidateSets, query_Graph);
      }
    } else {
      queryAfterRewrite = formQueryWithIgnoreNewLabel(query, candidateSets, query_Graph);
    }

    return ("profile " + queryAfterRewrite);
  }

  /**
   * Generate a query without 'union all' and add all label constraints.
   *
   * @param query
   * @param candidateSets
   * @param query_Graph
   * @return
   */
  private String formQueryWithIgnoreNewLabelComplete(String query,
      Map<Integer, Collection<Long>> candidateSets, Query_Graph query_Graph) {
    for (int id : candidateSets.keySet()) {
      String variable = query_Graph.nodeVariables[id];
      String label = query_Graph.label_list_string[id];
      String replaceToken = String.format("%s:`%s`", variable, label);
      if (!query.contains(replaceToken)) {
        throw new RuntimeException(query + " does not have " + replaceToken);
      }
      query =
          query.replace(replaceToken, replaceToken + ":`" + query_Graph.nodeVariables[id] + "`");
    }
    return query;
  }

  /**
   * Use the id-in way to rewrite the query.
   *
   * @param query
   * @throws Exception
   */
  public void query(String query) throws Exception {
    clearTrackingVariables();
    Query_Graph query_Graph = CypherDecoder.getQueryGraph(query, dbservice);
    queryWithIgnore(query, query_Graph);
  }

  /**
   * Execute the query. Attach 'profile' to get profile statistics. Use the label way rather than id
   * way to rewrite.
   *
   * @param query
   * @param query_Graph
   * @throws Exception
   */
  public void queryWithIgnoreNewLabel(String query, Query_Graph query_Graph) throws Exception {
    clearTrackingVariables();
    long totalStart = System.currentTimeMillis();
    this.query_Graph = query_Graph;
    Transaction tx = dbservice.beginTx();
    Map<Integer, Collection<Long>> candidateSets = getCandidateSetWithIgnore(query_Graph);

    if (candidateSets.isEmpty()) {
      return;
    }

    Util.println("candidate complete: " + candidateComplete);
    printCandidateSets(candidateSets);

    if (candidateComplete == true && selectivityEstimate) {
      spatialFilterAllPredicates(candidateSets);
      if (candidate_count > candidateSetsSizeLimit) {
        pickup(candidateSets);
      }
    }

    printCandidateSets(candidateSets);

    setNewLabel(candidateSets, query_Graph.nodeVariables);
    String queryAfterRewrite =
        formQueryWithIgnoreNewLabelCombined(query, candidateSets, query_Graph);
    Util.println("query after rewrite: \n" + queryAfterRewrite);

    runAndTrackTime(queryAfterRewrite);

    recoverLabel(candidateSets, query_Graph.nodeVariables);
    tx.success();
    tx.close();
    run_time = System.currentTimeMillis() - totalStart;
    setQueryStatistics(QueryType.LAGAQ_RANGE);
  }

  /**
   * Filter the spatial objects using spatial predicate if |spatial_nodes|<2000. Allow Multiple
   * spatial predicates.
   *
   * @param candidateSets
   */
  private void spatialFilterAllPredicates(Map<Integer, Collection<Long>> candidateSets) {
    long start = System.currentTimeMillis();
    for (int i = 0; i < query_Graph.Has_Spa_Predicate.length; i++) {
      if (query_Graph.Has_Spa_Predicate[i]) {
        Collection<Long> ids = candidateSets.get(i);
        if (ids.size() > 2000) {
          continue;
        }
        Collection<Long> idsAfterFilter = spatialFilter(ids, query_Graph.spa_predicate[i]);
        candidateSets.put(i, idsAfterFilter);
      }
    }
    range_query_time += System.currentTimeMillis() - start;
  }

  private Collection<Long> spatialFilter(Collection<Long> ids, MyRectangle queryRectangle) {
    Collection<Long> idsAfterFilter = new ArrayList<>(ids.size());
    for (long id : ids) {
      if (isNodeOverlapRectangle(dbservice.getNodeById(id), queryRectangle)) {
        idsAfterFilter.add(id);
      }
    }
    return idsAfterFilter;
  }

  private void printCandidateSets(Map<Integer, Collection<Long>> candidateSets) {
    for (int key : candidateSets.keySet()) {
      Util.println(
          String.format("%s:%d", query_Graph.nodeVariables[key], candidateSets.get(key).size()));
    }
    Util.println("\n");
  }

  /**
   * If |{@code candidateSets}| larger than limit, remove from the largest candidate set until limit
   * or only one element exist.
   *
   * @param candidateSets
   * @return
   */
  private void pickup(Map<Integer, Collection<Long>> candidateSets) {
    List<Entry<Integer, Collection<Long>>> list = new ArrayList<>(candidateSets.entrySet());
    list.sort(Entry.comparingByValue(new Comparator<Collection<Long>>() {
      @Override
      public int compare(Collection<Long> o1, Collection<Long> o2) {
        return o2.size() - o1.size();
      }
    }));
    Iterator<Entry<Integer, Collection<Long>>> iterator = list.iterator();
    while (candidateSets.size() > 1 && candidate_count > 2000) {
      Entry<Integer, Collection<Long>> entry = iterator.next();
      candidateSets.remove(entry.getKey());
      candidate_count -= entry.getValue().size();
    }
  }

  private void runAndTrackTime(String queryAfterRewrite) {
    long start = System.currentTimeMillis();
    Result result = dbservice.execute(queryAfterRewrite);
    get_iterator_time += System.currentTimeMillis() - start;

    start = System.currentTimeMillis();
    int cur_count = 0;
    while (result.hasNext()) {
      cur_count++;
      Map<String, Object> row = result.next();
      if (outputResult) {
        Util.println(row);
      }
    }
    iterate_time += System.currentTimeMillis() - start;

    result_count += cur_count;
    planDescription = result.getExecutionPlanDescription();
    page_hit_count += OwnMethods.GetTotalDBHits(planDescription);
    if (outputExecutionPlan) {
      Util.println(planDescription);
      Util.println("OwnMethods.gethits: " + OwnMethods.GetTotalDBHits(planDescription));
      if (planDescription.hasProfilerStatistics()) {
        Util.println("cypher statistics: " + planDescription.getProfilerStatistics().getDbHits());
      } else {
        throw new RuntimeException("planDescription has no statistics!!");
      }

      OwnMethods.WriteFile(logPath, true, planDescription.toString() + "\n");
    }
  }

  /**
   * Form the query with union all. Used for the non-complete case.
   *
   * @param query
   * @param candidateSets
   * @param query_Graph
   * @return
   */
  private String formQueryWithIgnoreNewLabel(String query,
      Map<Integer, Collection<Long>> candidateSets, Query_Graph query_Graph) {
    List<String> subQueries = new ArrayList<>();
    for (int id : candidateSets.keySet()) {
      String variable = query_Graph.nodeVariables[id];
      String label = query_Graph.label_list_string[id];
      String replaceToken = String.format("%s:`%s`", variable, label);
      if (!query.contains(replaceToken)) {
        throw new RuntimeException(query + " does not have " + replaceToken);
      }
      subQueries.add(
          query.replace(replaceToken, replaceToken + ":`" + query_Graph.nodeVariables[id] + "`"));
    }

    String newQuery = null;
    for (String subQuery : subQueries) {
      if (newQuery == null) {
        newQuery = subQuery;
        continue;
      } else {
        newQuery += " UNION ALL " + subQuery;
      }
    }
    return newQuery;
  }

  /**
   * Set the new label for cypher query execution. For each <int, Collection> set its label with
   * nodeVariableName string, i.e., a0, a1...
   *
   * @param candidateSets
   * @param nodeVariables
   */
  private void setNewLabel(Map<Integer, Collection<Long>> candidateSets, String[] nodeVariables) {
    long start = System.currentTimeMillis();
    for (int queryNodeId : candidateSets.keySet()) {
      String nodeVariableName = nodeVariables[queryNodeId];
      for (long neo4jId : candidateSets.get(queryNodeId)) {
        Node node = dbservice.getNodeById(neo4jId);
        node.addLabel(Label.label(nodeVariableName));
      }
    }
    set_label_time += System.currentTimeMillis() - start;
  }

  private void recoverLabel(Map<Integer, Collection<Long>> candidateSets, String[] nodeVariables) {
    long start = System.currentTimeMillis();
    for (int queryNodeId : candidateSets.keySet()) {
      String nodeVariableName = nodeVariables[queryNodeId];
      for (long neo4jId : candidateSets.get(queryNodeId)) {
        Node node = dbservice.getNodeById(neo4jId);
        node.removeLabel(Label.label(nodeVariableName));
      }
    }
    remove_label_time += System.currentTimeMillis() - start;
  }

  /**
   * Execute the query. Attach 'profile' to get profile statistics. id(a_i) is used. Not use
   * setLabel.
   *
   * @param query
   * @param query_Graph
   * @throws Exception
   */
  public void queryWithIgnore(String query, Query_Graph query_Graph) throws Exception {
    clearTrackingVariables();
    Transaction tx = dbservice.beginTx();
    Map<Integer, Collection<Long>> candidateSets = getCandidateSetWithIgnore(query_Graph);

    if (candidateSets.isEmpty()) {
      return;
    }

    String queryAfterRewrite = formQueryWithIgnore(query, candidateSets, query_Graph.nodeVariables);
    queryAfterRewrite = "profile " + queryAfterRewrite;
    Util.println("query after rewrite: \n" + queryAfterRewrite);
    long start = System.currentTimeMillis();
    Result result = dbservice.execute(queryAfterRewrite);
    get_iterator_time += System.currentTimeMillis() - start;

    start = System.currentTimeMillis();
    int cur_count = 0;
    while (result.hasNext()) {
      cur_count++;
      Map<String, Object> row = result.next();
      if (outputResult) {
        Util.println(row);
      }
    }
    iterate_time += System.currentTimeMillis() - start;

    result_count += cur_count;
    planDescription = result.getExecutionPlanDescription();
    page_hit_count += OwnMethods.GetTotalDBHits(planDescription);
    if (outputExecutionPlan) {
      Util.println(planDescription);
      Util.println("OwnMethods.gethits: " + OwnMethods.GetTotalDBHits(planDescription));
      if (planDescription.hasProfilerStatistics()) {
        Util.println("cypher statistics: " + planDescription.getProfilerStatistics().getDbHits());
      } else {
        throw new Exception("planDescription has no statistics!!");
      }
    }
    tx.success();
    tx.close();
    setQueryStatistics(QueryType.LAGAQ_RANGE);
  }

  /**
   * Rewrite the query to include the candidateSets constraint. id(a_i) in [] is used.
   *
   * @param query
   * @param candidateSets
   * @param nodeVariables
   * @return
   */
  public static String formQueryWithIgnore(String query,
      Map<Integer, Collection<Long>> candidateSets, String[] nodeVariables) {
    String queryAfterRewrite = query.toString();

    String idConstraint = formIdConstraint(candidateSets, nodeVariables);
    queryAfterRewrite = insertIdConstraint(queryAfterRewrite, idConstraint);

    return queryAfterRewrite;
  }

  /**
   * Split the query into two parts. One is before where. Another is the rest.
   *
   * @param query
   * @return
   */
  public static String[] generateQueryCombineParts(String query) {
    String queryLowercase = query.toLowerCase();
    int splitPos = queryLowercase.indexOf("where") + 5;
    return new String[] {query.substring(0, splitPos), query.substring(splitPos, query.length())};
  }

  /**
   * Insert the id constraint string into the original query string.
   *
   * @param query
   * @param idConstraint
   * @return
   */
  private static String insertIdConstraint(String query, String idConstraint) {
    String[] queryParts = generateQueryCombineParts(query);
    return queryParts[0] + " (" + idConstraint + ") and" + queryParts[1];
  }

  /**
   * Form the string of like "id(a0) in [...] or id(a1) in [...]".
   *
   * @param candidateSets
   * @param nodeVariables
   * @return
   */
  public static String formIdConstraint(Map<Integer, Collection<Long>> candidateSets,
      String[] nodeVariables) {
    String string = "";
    for (int endId : candidateSets.keySet()) {
      Collection<Long> ids = candidateSets.get(endId);
      String variable = nodeVariables[endId];
      if (string.isEmpty()) {
        string += String.format("id(%s) in %s", variable, ids);
      } else {
        string += String.format(" or id(%s) in %s", variable, ids);
      }
    }
    return string;
  }

  /**
   * For a given query to get several query nodes and their candidate set. Set the complete
   * variables here. If complete, all complete query nodes will be kept and incomplete removed. If
   * not complete, all query nodes are kept and the union strategy will be used. Consider the
   * ignored PN with [] and PNSize is 0. Assume that only 1 spatial predicate exists.
   *
   * @param query_Graph use the String[] label_list
   * @return null means something wrong
   */
  public Map<Integer, Collection<Long>> getCandidateSetWithIgnore(Query_Graph query_Graph) {
    try {
      Map<Integer, Collection<Long>> candidateSets = new HashMap<>();

      // <spa_id, rectangle> all query rectangles
      Map<Integer, MyRectangle> spa_predicates = new HashMap<Integer, MyRectangle>();

      // <spa_id, <neighbor_id, size_property_name>>
      Map<Integer, Map<Integer, Set<String>>> PN_size_propertyname = new HashMap<>();
      // <spa_id, <neighbor_id, list_property_name>>
      Map<Integer, Map<Integer, Set<String>>> PN_list_propertyname = new HashMap<>();

      // <spa_id, <end_id, path_name>> (path_name: PN_a_endid)
      Map<Integer, HashMap<Integer, HashSet<String>>> spaPathsMap = recognizePaths(query_Graph);

      Util.println(spaPathsMap.toString());

      // Construct min_hop (compact structure for min_hop_array
      // and NL_size_propertyname and NL_list_propertyname
      for (int i = 0; i < query_Graph.Has_Spa_Predicate.length; i++)
        if (query_Graph.Has_Spa_Predicate[i]) {
          spa_predicates.put(i, query_Graph.spa_predicate[i]);
        }

      for (int spaID : spaPathsMap.keySet()) {
        PN_size_propertyname.put(spaID, new HashMap<Integer, Set<String>>());
        PN_list_propertyname.put(spaID, new HashMap<Integer, Set<String>>());
        for (int neighborID : spaPathsMap.get(spaID).keySet()) {
          PN_size_propertyname.get(spaID).put(neighborID, new HashSet<String>());
          PN_list_propertyname.get(spaID).put(neighborID, new HashSet<String>());
          for (String PNName : spaPathsMap.get(spaID).get(neighborID)) {
            PN_size_propertyname.get(spaID).get(neighborID).add(RisoTreeUtil.getPNSizeName(PNName));
            PN_list_propertyname.get(spaID).get(neighborID).add(PNName);
          }
        }
      }

      if (outputLevelInfo) {
        Util.println(String.format("PNSize_property: %s", PN_size_propertyname));
      }

      Node root_node = RTreeUtility.getRTreeRoot(dbservice, dataset);

      // long start = System.currentTimeMillis();
      Map<Integer, List<Node>> overlapLeafNodes =
          getOverlapLeafNodes(root_node, spa_predicates, PN_list_propertyname);
      // range_query_time += System.currentTimeMillis() - start;
      if (overlapLeafNodes == null) {
        Util.println("No result satisfy the query.");
        return candidateSets;
      }

      // current candidateComplete strategy only works for single spatial range predicate
      if (completeStrategyUsed) {
        candidateComplete = false;
        candidateSets = getCandidateSetWithIgnoreComplete(overlapLeafNodes, PN_list_propertyname);
        for (int spatialId : queryNodesComplete.keySet()) {
          for (MutableBoolean complete : queryNodesComplete.get(spatialId)) {
            if (complete.booleanValue()) {
              candidateComplete = true;
              break;
            }
          }
        }
        // If any query node is complete, we can use the complete strategy
        // which is to form the cypher query without union.
        // But the incomplete query nodes need to be removed from the candidateSet.
        if (candidateComplete) {
          for (int spatialId : queryNodesComplete.keySet()) {
            for (int i = 0; i < queryNodesComplete.get(spatialId).length; i++) {
              MutableBoolean complete = queryNodesComplete.get(spatialId)[i];
              if (complete == null || !complete.booleanValue()) {
                candidateSets.remove(i);
              }
            }
            break;
          }
        }
      } else {
        candidateSets =
            getCandidateSetWithIgnore(overlapLeafNodes, PN_list_propertyname, PN_size_propertyname);
      }

      for (Collection<Long> candidates : candidateSets.values()) {
        candidate_count += candidates.size();
      }

      return candidateSets;
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
    return null;
  }

  /**
   * Set the value of {@code candidateComplete} here.
   *
   * @param overlapLeafNodes
   * @param pN_list_propertyname
   * @return
   */
  private Map<Integer, Collection<Long>> getCandidateSetWithIgnoreComplete(
      Map<Integer, List<Node>> overlapLeafNodes,
      Map<Integer, Map<Integer, Set<String>>> pN_list_propertyname) {
    Map<Integer, Map<Integer, Collection<Long>>> pathNeighborMultiPredicates = new HashMap<>();
    for (int spatialId : overlapLeafNodes.keySet()) {
      Map<Integer, Collection<Long>> pathNeighbors = getPathNeighborsComplete(spatialId,
          overlapLeafNodes.get(spatialId), pN_list_propertyname.get(spatialId));
      pathNeighborMultiPredicates.put(spatialId, pathNeighbors);
    }

    Map<Integer, Collection<Long>> candidateSets =
        refineMultiPredicatesCandidateSets(pathNeighborMultiPredicates);

    return candidateSets;
  }

  /**
   * Set the {@code queryNodesComplete} here.
   *
   * @param spatialId
   * @param nodes
   * @param pN_list_propertyname
   * @return
   */
  private Map<Integer, Collection<Long>> getPathNeighborsComplete(int spatialId, List<Node> nodes,
      Map<Integer, Set<String>> pN_list_propertyname) {
    Map<Integer, Collection<Long>> pathNeighbors = new HashMap<>();
    for (int endId : pN_list_propertyname.keySet()) {
      Set<String> labelPaths = pN_list_propertyname.get(endId);
      MutableBoolean complete = new MutableBoolean(true);
      Collection<Long> candidates = getCadidates(nodes, labelPaths, complete);
      queryNodesComplete.get(spatialId)[endId] = complete;
      pathNeighbors.put(endId, candidates);
    }
    return pathNeighbors;
  }

  /**
   * Get the candidates for an end query node.
   *
   * @param nodes
   * @param labelPaths
   * @return
   */
  private Collection<Long> getCadidates(List<Node> nodes, Set<String> labelPaths,
      MutableBoolean complete) {
    List<Integer> candidates = new ArrayList<>();
    for (Node node : nodes) {
      List<Integer> curCandidates = new ArrayList<>();
      for (String path : labelPaths) {
        int[] pn = (int[]) node.getProperty(path, null);
        if (pn == null || pn.length == 0) {
          continue;
        } else if (curCandidates.size() == 0) {
          curCandidates = Util.intArrayToList(pn);
        } else {
          curCandidates = Util.sortedListIntersect(curCandidates, pn);
        }
      }
      if (curCandidates.size() == 0) {
        complete.setValue(false); // if any leaf node is [], the end query node is
                                  // incomplete
      }
      candidates = Util.sortedListMerge(candidates, curCandidates);
    }

    List<Long> res = new ArrayList<>();
    for (int id : candidates) {
      res.add((long) id);
    }
    return res;
  }

  private Map<Integer, Collection<Long>> getCandidateSetWithIgnore(
      Map<Integer, List<Node>> overlapLeafNodes,
      Map<Integer, Map<Integer, Set<String>>> pN_list_propertyname,
      Map<Integer, Map<Integer, Set<String>>> pN_size_propertyname) throws Exception {
    Map<Integer, Map<Integer, Collection<Long>>> pathNeighborMultiPredicates = new HashMap<>();
    for (int spatialId : overlapLeafNodes.keySet()) {
      // Map<Integer, Collection<Long>> pathNeighbors =
      // getPathNeighbors(overlapLeafNodes.get(spatialId), pN_list_propertyname.get(spatialId),
      // pN_size_propertyname.get(spatialId));
      Map<Integer, Collection<Long>> pathNeighbors = getPathNeighbors(spatialId,
          overlapLeafNodes.get(spatialId), pN_list_propertyname, pN_size_propertyname);
      pathNeighborMultiPredicates.put(spatialId, pathNeighbors);
    }

    Map<Integer, Collection<Long>> candidateSets =
        refineMultiPredicatesCandidateSets(pathNeighborMultiPredicates);

    return candidateSets;
  }

  /**
   * If multiple spatial predicates exist, choose the one with minimum sum pn size. Actually it is
   * not used now.
   *
   * @param pathNeighborMultiPredicates
   * @return
   */
  private Map<Integer, Collection<Long>> refineMultiPredicatesCandidateSets(
      Map<Integer, Map<Integer, Collection<Long>>> pathNeighborMultiPredicates) {
    int minPredicate = -1, minCard = Integer.MAX_VALUE;
    for (int spatialId : pathNeighborMultiPredicates.keySet()) {
      int card = 0;
      Map<Integer, Collection<Long>> candidateSetsPredicate =
          pathNeighborMultiPredicates.get(spatialId);
      for (int endId : candidateSetsPredicate.keySet()) {
        card += candidateSetsPredicate.get(endId).size();
      }
      if (card < minCard) {
        minPredicate = spatialId;
      }
    }
    return pathNeighborMultiPredicates.get(minPredicate);
  }

  // /**
  // * Get the candidateSet for one spatial predicate.
  // *
  // * @param list
  // * @param pN_list_propertyname
  // * @param pN_size_propertyname
  // * @return
  // * @throws Exception
  // */
  // private Map<Integer, Collection<Long>> getPathNeighbors(List<Node> list,
  // Map<Integer, Set<String>> pN_list_propertyname,
  // Map<Integer, Set<String>> pN_size_propertyname) throws Exception {
  // Map<Integer, Collection<Long>> candidateSets = new HashMap<>();
  //
  // for (Node node : list) {
  // int minEndId = getEndIdWithMinCard(node, pN_size_propertyname);
  // addPathNeighbors(node, minEndId, pN_list_propertyname, candidateSets);
  // }
  // return candidateSets;
  // }

  /**
   * Add the path neighbor of the minEndId into the corresponding node id of candidateSets.
   *
   * @param node
   * @param minEndId
   * @param pN_list_propertyname
   * @param candidateSets
   * @throws Exception
   */
  private void addPathNeighbors(Node node, int minEndId,
      Map<Integer, Set<String>> pN_list_propertyname,
      Map<Integer, Collection<Long>> candidateSets) {
    // Construct the candidateSet for a endId using different path neighbors ending at it.
    List<Integer> candidates = null;
    for (String path : pN_list_propertyname.get(minEndId)) {
      int[] pathNeighbors = (int[]) node.getProperty(path);
      if (pathNeighbors.length == 0) {
        throw new RuntimeException(
            String.format("%s has %s as minCard while it is dropped!", node, path));
      }
      if (candidates == null) {
        candidates = ArrayUtil.intArrayToList(pathNeighbors);
      } else {
        candidates = ArrayUtil.sortedListIntersect(candidates, pathNeighbors);
      }
    }

    Collection<Long> candidateSet = candidateSets.getOrDefault(minEndId, new HashSet<>());
    for (int candidateId : candidates) {
      candidateSet.add((long) candidateId);
    }
    candidateSets.put(minEndId, candidateSet);
  }

  /**
   * For each spatial predicate, get the pn. Pn is collected for each leaf node. In each node, only
   * the most selective pn will be returned. So it is not complete. Union is required.
   *
   * @param spatialId
   * @param overlapLeafNodes
   * @param pN_list_propertyname
   * @param pN_size_propertyname
   * @return
   * @throws Exception
   */
  private Map<Integer, Collection<Long>> getPathNeighbors(int spatialId,
      List<Node> overlapLeafNodes, Map<Integer, Map<Integer, Set<String>>> pN_list_propertyname,
      Map<Integer, Map<Integer, Set<String>>> pN_size_propertyname) throws Exception {
    Map<Integer, Collection<Long>> candidateSets = new HashMap<>();

    for (Node node : overlapLeafNodes) {
      int minEndId = getEndIdWithMinCard(node, pN_size_propertyname.get(spatialId));
      if (minEndId == -1) {
        addSpatialCandidate(node, spatialId, candidateSets);
      } else {
        addPathNeighbors(node, minEndId, pN_list_propertyname.get(spatialId), candidateSets);
      }
    }
    return candidateSets;
  }

  private void addSpatialCandidate(Node node, int spatialId,
      Map<Integer, Collection<Long>> candidateSets) {
    MyRectangle queryRect = query_Graph.spa_predicate[spatialId];
    Collection<Long> candidateSet = candidateSets.get(spatialId);
    if (candidateSet == null) {
      candidateSet = new HashSet<>();
    }
    Iterable<Relationship> rels =
        node.getRelationships(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.OUTGOING);
    for (Relationship relationship : rels) {
      Node geom = relationship.getEndNode();
      MyRectangle mbr = RTreeUtility.getNodeMBR(geom);
      if (queryRect.intersect(mbr) != null) {
        candidateSet.add(geom.getId());
      }
    }

    candidateSets.put(spatialId, candidateSet);
  }

  /**
   * Get the query node id with the smallest pnsize. If one query node appears as the end node in
   * more than one label path, the smallest one is considered.
   *
   * @param node
   * @param pN_size_propertyname
   * @return
   * @throws Exception
   */
  private int getEndIdWithMinCard(Node node, Map<Integer, Set<String>> pN_size_propertyname)
      throws Exception {
    int minCard = Integer.MAX_VALUE;
    int minEndId = -1;
    for (int endId : pN_size_propertyname.keySet()) {
      for (String pathSizeName : pN_size_propertyname.get(endId)) {
        Object curSizeObject = node.getProperty(pathSizeName, Integer.MAX_VALUE);
        // Object curSizeObject = node.getProperty(pathSizeName);
        // this case handle the removed pn because of a shorter path
        // if (curSizeObject == null) {
        // continue;
        // }
        int curSize = (int) curSizeObject;
        if (curSize != 0 && curSize < minCard) {
          minEndId = endId;
          minCard = curSize;
        }
      }
    }

    if (minEndId == -1) {
      LOGGER.info(String.format("Node %s's path neighbor has all been dropped!", node));
    }

    return minEndId;
  }

  /**
   * Get the overlapped leaf nodes for all spatial predicates.
   *
   * @param root_node
   * @param spa_predicates
   * @param pN_list_propertyname
   * @return a map of <spa_predicate_id, overlapped_nodes>
   * @throws Exception
   */
  public Map<Integer, List<Node>> getOverlapLeafNodes(Node root_node,
      Map<Integer, MyRectangle> spa_predicates,
      Map<Integer, Map<Integer, Set<String>>> pN_list_propertyname) throws Exception {
    long start = System.currentTimeMillis();
    Map<Integer, List<Node>> overlapLeafNodesMultiPredicate = new HashMap<>();
    // for each spatial predicate, get the overlapped leaf nodes.
    for (int spatialId : spa_predicates.keySet()) {
      List<Node> overlapLeafNodes = getOverlapLeafNodesSinglePredicate(root_node,
          spa_predicates.get(spatialId), pN_list_propertyname.get(spatialId));
      if (overlapLeafNodes.isEmpty()) {
        return null;
      }
      overlap_leaf_node_count += overlapLeafNodes.size();
      overlapLeafNodesMultiPredicate.put(spatialId, overlapLeafNodes);
    }
    range_query_time += System.currentTimeMillis() - start;
    return overlapLeafNodesMultiPredicate;
  }

  /**
   * Get the overlapped leaf nodes for a spatial predicate considering all the label path
   * constraint.
   *
   * @param root_node
   * @param myRectangle
   * @param pN_propertyname_single_predicate the label path prop related to this spatial predicate
   * @return
   * @throws Exception
   */
  private List<Node> getOverlapLeafNodesSinglePredicate(Node root_node, MyRectangle myRectangle,
      Map<Integer, Set<String>> pN_propertyname_single_predicate) throws Exception {
    List<Node> cur_list = new LinkedList<>();
    cur_list.add(root_node);
    List<Node> next_list = new LinkedList<>();

    Map<String, Set<String>> pathsAndShorterPaths = new HashMap<>();
    for (int endId : pN_propertyname_single_predicate.keySet()) {
      Set<String> pathsToSameNode = pN_propertyname_single_predicate.get(endId);
      for (String path : pathsToSameNode) {
        pathsAndShorterPaths.put(path, RisoTreeUtil.formIgnoreSearchSet(path));
      }
    }

    Util.println("paths and shorter paths: " + pathsAndShorterPaths.toString());

    int level_index = 0;
    boolean isLeafLevel = false;
    while (!cur_list.isEmpty()) {
      long startLevel = System.currentTimeMillis();// for the level time
      long expandTime = 0;
      if (!isLeafLevel) {
        Node firstNodeThisLevel = cur_list.get(0);
        isLeafLevel = RTreeUtility.isLeaf(firstNodeThisLevel);
      }

      List<Node> overlap_MBR_list = new LinkedList<>();

      for (Node node : cur_list) {
        if (isNodeOverlapRectangle(node, myRectangle)) {
          // if does not contain all the paths (currently only leaf nodes contain path info)
          if (isLeafLevel && !isNodeContainAllPathsIgnore(node, pathsAndShorterPaths)) {
            continue;
          }
          overlap_MBR_list.add(node);
          // record the next level tree nodes
          long start = System.currentTimeMillis();
          Iterable<Relationship> rels =
              node.getRelationships(RTreeRel.RTREE_CHILD, Direction.OUTGOING);
          for (Relationship relationship : rels) {
            next_list.add(relationship.getEndNode());
          }
          expandTime += System.currentTimeMillis() - start;
        }
      }
      int located_in_count = overlap_MBR_list.size();

      if (outputLevelInfo) {
        String logWriteLine = String.format("level %d\n", level_index);
        logWriteLine += String.format("cur list size: %d\n", cur_list.size());
        logWriteLine += String.format("Located in nodes: %d\n", located_in_count);
        logWriteLine += String.format("next list size: %d\n", next_list.size());
        logWriteLine += String.format("level %d time: %d\n", level_index,
            System.currentTimeMillis() - startLevel);
        logWriteLine += String.format("expand time: %d\n", expandTime);
        Util.println(logWriteLine);
      }

      if (overlap_MBR_list.isEmpty() == true) {
        Util.println("No result satisfy the query.");
        return overlap_MBR_list;
      }

      // traverse to the leaf node level and start to form the cypher query
      if (overlap_MBR_list.isEmpty() == false && next_list.isEmpty()) {
        return overlap_MBR_list;
      }

      cur_list = next_list;
      next_list = new LinkedList<Node>();
      level_index++;
    }

    return cur_list;
  }

  private boolean isNodeOverlapRectangle(Node node, MyRectangle queryRectangle) {
    if (node.hasProperty(Config.BBoxName)) {
      double[] bbox = (double[]) node.getProperty(Config.BBoxName);
      MyRectangle MBR = new MyRectangle(bbox[0], bbox[1], bbox[2], bbox[3]);
      MyRectangle intersect = MBR.intersect(queryRectangle);
      return intersect != null;
    } else {
      throw new RuntimeException(
          String.format("node %s does not has \"%s\" property", node, Config.BBoxName));
    }
  }

  /**
   * Decide whether a node contains all the paths required. PN can be ignored (stored with []).
   *
   * @param node
   * @param paths
   * @return
   */
  public boolean isNodeContainAllPathsIgnore(Node node,
      Map<String, Set<String>> pathsAndShortPaths) {
    long start = System.currentTimeMillis();
    for (String path : pathsAndShortPaths.keySet()) {
      if (!isNodeContainSinglePathIgnore(node, path, pathsAndShortPaths.get(path))) {
        check_paths_time += System.currentTimeMillis() - start;
        return false;
      }
    }
    check_paths_time += System.currentTimeMillis() - start;
    return true;
  }

  public boolean isNodeContainSinglePathIgnore(Node node, String path, Set<String> shorterPaths) {
    for (String key : node.getPropertyKeys()) {
      if (key.equals(path)) {
        return true;
      }
      if (shorterPaths.contains(key)) {
        int[] pn = (int[]) node.getProperty(key);
        if (pn.length == 0) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Decide whether a node contains all the paths required.
   *
   * @param node
   * @param paths
   * @return
   */
  public boolean isNodeContainAllPaths(Node node, Set<String> paths) {
    for (String path : paths) {
      if (!node.hasProperty(path)) {
        return false;
      }
    }
    return true;
  }

  /**
   * For a given query to get the most selective query node and its candidate set.
   *
   * @param query_Graph use the String[] label_list
   * @return null means something wrong
   */
  public HashMap<Integer, Collection<Long>> getCandidateSet(Query_Graph query_Graph) {
    try {
      HashMap<Integer, Collection<Long>> candidateSet = new HashMap<>();
      String logWriteLine = "";

      // <spa_id, rectangle> all query rectangles
      HashMap<Integer, MyRectangle> spa_predicates = new HashMap<Integer, MyRectangle>();

      // <spa_id, <neighbor_id, size_property_name>>
      HashMap<Integer, HashMap<Integer, HashSet<String>>> PN_size_propertyname =
          new HashMap<Integer, HashMap<Integer, HashSet<String>>>();
      // <spa_id, <neighbor_id, list_property_name>>
      HashMap<Integer, HashMap<Integer, HashSet<String>>> PN_list_propertyname =
          new HashMap<Integer, HashMap<Integer, HashSet<String>>>();

      // <spa_id, <end_id, path_name>> (path_name: PN_a_endid)
      HashMap<Integer, HashMap<Integer, HashSet<String>>> spaPathsMap = recognizePaths(query_Graph);

      // Construct min_hop (compact structure for min_hop_array
      // and NL_size_propertyname and NL_list_propertyname
      for (int i = 0; i < query_Graph.Has_Spa_Predicate.length; i++)
        if (query_Graph.Has_Spa_Predicate[i]) {
          spa_predicates.put(i, query_Graph.spa_predicate[i]);
        }

      for (int spaID : spaPathsMap.keySet()) {
        PN_size_propertyname.put(spaID, new HashMap<Integer, HashSet<String>>());
        PN_list_propertyname.put(spaID, new HashMap<Integer, HashSet<String>>());
        for (int neighborID : spaPathsMap.get(spaID).keySet()) {
          PN_size_propertyname.get(spaID).put(neighborID, new HashSet<String>());
          PN_list_propertyname.get(spaID).put(neighborID, new HashSet<String>());
          for (String PNName : spaPathsMap.get(spaID).get(neighborID)) {
            PN_size_propertyname.get(spaID).get(neighborID).add(RisoTreeUtil.getPNSizeName(PNName));
            PN_list_propertyname.get(spaID).get(neighborID).add(PNName);
          }
        }
      }

      if (outputLevelInfo) {
        logWriteLine = String.format("NL_property: %s", PN_size_propertyname);
        Util.println(logWriteLine);
      }

      Transaction tx = dbservice.beginTx();
      LinkedList<Node> cur_list = new LinkedList<Node>();
      Node root_node = RTreeUtility.getRTreeRoot(dbservice, dataset);
      cur_list.add(root_node);
      LinkedList<Node> next_list = new LinkedList<Node>();

      int level_index = 0;
      while (cur_list.isEmpty() == false) {
        long startLevel = System.currentTimeMillis();// for the level time

        // <spa_id, overlap_nodes_list>
        // HashMap<Integer, LinkedList<Node>> overlap_MBR_list = new HashMap<>();
        LinkedList<Node> overlap_MBR_list = new LinkedList<Node>(); // just support one spatial
                                                                    // predicate

        Iterator<Node> iterator = cur_list.iterator();
        while (iterator.hasNext()) {
          Node node = iterator.next();
          if (node.hasProperty(Config.BBoxName)) {
            double[] bbox = (double[]) node.getProperty(Config.BBoxName);
            MyRectangle MBR = new MyRectangle(bbox[0], bbox[1], bbox[2], bbox[3]);

            for (int key : spa_predicates.keySet()) {
              MyRectangle queryRectangle = spa_predicates.get(key);

              MyRectangle intersect = MBR.intersect(queryRectangle);
              if (intersect != null) {
                // all overlapped nodes
                // overlap_MBR_list.get(key).add(node);
                overlap_MBR_list.add(node);

                // record the next level tree nodes
                Iterable<Relationship> rels =
                    node.getRelationships(RTreeRel.RTREE_CHILD, Direction.OUTGOING);
                for (Relationship relationship : rels)
                  next_list.add(relationship.getEndNode());
              }
            }
          } else
            throw new Exception(String.format("node %d does not has \"bbox\" property", node));
        }

        if (outputLevelInfo) {
          logWriteLine = String.format("level %d time: %d", level_index,
              System.currentTimeMillis() - startLevel);
          Util.println(logWriteLine);
        }

        if (overlap_MBR_list.isEmpty() == true) {
          Util.println("No result satisfy the query.");
          tx.success();
          tx.close();
          return candidateSet;
        }

        // traverse to the leaf node level and start to form the cypher query
        if (overlap_MBR_list.isEmpty() == false && next_list.isEmpty()) {
          overlap_leaf_node_count = overlap_MBR_list.size();
          // <spa_id, card>
          HashMap<Integer, Double> spa_cards = new HashMap<Integer, Double>();
          for (int key : spa_predicates.keySet())
            spa_cards.put(key, 0.0);

          // <spa_id, <neighbor_id, card>>
          HashMap<Integer, HashMap<Integer, HashMap<String, Double>>> NL_cards =
              new HashMap<Integer, HashMap<Integer, HashMap<String, Double>>>();
          for (int spa_id : PN_size_propertyname.keySet()) {
            NL_cards.put(spa_id, new HashMap<Integer, HashMap<String, Double>>());
            for (int neighborID : PN_size_propertyname.get(spa_id).keySet()) {
              NL_cards.get(spa_id).put(neighborID, new HashMap<String, Double>());
              for (String propertyName : PN_size_propertyname.get(spa_id).get(neighborID))
                NL_cards.get(spa_id).get(neighborID).put(propertyName, 0.0);
            }
          }

          for (Node node : overlap_MBR_list) {
            double[] bbox = (double[]) node.getProperty(Config.BBoxName);
            MyRectangle MBR = new MyRectangle(bbox[0], bbox[1], bbox[2], bbox[3]);
            // OwnMethods.Print(MBR);
            double MBR_area = MBR.area();
            // int spa_count = (Integer) node.getProperty("count");
            int spa_count = 100;

            for (int key : spa_predicates.keySet()) {
              MyRectangle queryRectangle = spa_predicates.get(key);

              MyRectangle intersect = MBR.intersect(queryRectangle);
              // calculate overlapped ratio compared to the MBR area
              double ratio;
              if (MBR_area == 0)
                ratio = 1;
              else
                ratio = intersect.area() / MBR_area;
              // estimate spatial predicate cardinality
              spa_cards.put(key, (spa_cards.get(key) + ratio * spa_count));

              // estimate NL cardinality
              for (int neighborID : NL_cards.get(key).keySet())
                for (String propertyName : NL_cards.get(key).get(neighborID).keySet()) {
                  if (node.hasProperty(propertyName)) {
                    int PNSize = (Integer) node.getProperty(propertyName);
                    NL_cards.get(key).get(neighborID).put(propertyName,
                        NL_cards.get(key).get(neighborID).get(propertyName) + ratio * PNSize);
                  }
                }
            }
          }

          // find the query node with the minimum cardinality
          double min_spa_card = Double.MAX_VALUE, min_NL_card = Double.MAX_VALUE;
          int minSpaID = 0;// the spatial predicate with minimum cardinality
          int min_NL_spa_id = 0, min_NL_neighbor_id = 0;// min NL spatial id and neighbor id
          String minPNListPropertyname = null, minPNSizePropertyname = "";

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
            }

            for (int neighbor_id : PN_list_propertyname.get(key).keySet())
              for (String properName : PN_list_propertyname.get(key).get(neighbor_id)) {
                double card =
                    NL_cards.get(key).get(neighbor_id).get(RisoTreeUtil.getPNSizeName(properName));
                if (card < min_NL_card) {
                  min_NL_spa_id = key;
                  min_NL_neighbor_id = neighbor_id;
                  min_NL_card = card;
                  minPNListPropertyname = properName;
                  minPNSizePropertyname = RisoTreeUtil.getPNSizeName(properName);
                }
                if (outputLevelInfo) {
                  logWriteLine = String.format("%d %d %s estimate size: %f", key, neighbor_id,
                      properName, card);
                  Util.println(logWriteLine);
                }
              }
          }

          if (outputLevelInfo) {
            logWriteLine = String.format("level %d min card : %f", level_index,
                Math.min(min_spa_card, min_NL_card));
            Util.println(logWriteLine);
          }

          // construct the NL_list with the highest selectivity
          long start1 = System.currentTimeMillis();

          HashSet<Long> min_NL_list = null;
          int realMinPNSize = Integer.MAX_VALUE;
          if (minPNListPropertyname != null) {
            min_NL_list = new HashSet<>();
            for (Node node : overlap_MBR_list) {
              if (node.hasProperty(minPNListPropertyname)) {
                // here id is graph id rather than neo4j id
                int[] NL_list_label = (int[]) node.getProperty(minPNListPropertyname);
                for (int node_id : NL_list_label)
                  min_NL_list.add((long) node_id);
              }
            }
            realMinPNSize = min_NL_list.size();
          }

          range_query_time += System.currentTimeMillis() - startLevel;

          if (outputLevelInfo) {
            logWriteLine = String.format("min_NL_list size is %d\n", realMinPNSize);
            if (realMinPNSize < min_spa_card)
              logWriteLine += "NL_list is more selective\n";
            else
              logWriteLine += "spa predicate is more selective\n";
            logWriteLine +=
                String.format("NL_serialize time: %d\n", System.currentTimeMillis() - start1);
            logWriteLine += String.format("level %d time: %d\n", level_index,
                System.currentTimeMillis() - startLevel);
            Util.println(logWriteLine);
          }

          // set realMinPNSize to 0 to force start the neighbor
          if (forceGraphFirst) {
            Util.println("force graph first even min PN size is " + realMinPNSize);
            realMinPNSize = 0;
          }

          // if PN is more selective than spatial predicate
          if (realMinPNSize < min_spa_card) {
            Util.println("PN is more selective");
            candidateSet.put(min_NL_neighbor_id, min_NL_list);
          } else { // if spatial predicate is more selective
            MyRectangle queryRect = spa_predicates.get(minSpaID);
            // get located in nodes
            candidate_count = 0;
            int levelTime = 0;
            level_index++;
            double nodeIndex = 0;
            // construct a cypher query for each tree leaf node
            // which consists of several spatial objects
            Collection<Long> candidateIds = new LinkedList<>();
            for (Node node : overlap_MBR_list) {
              nodeIndex++;
              long start = System.currentTimeMillis();
              Iterable<Relationship> rels =
                  node.getRelationships(Direction.OUTGOING, Labels.RTreeRel.RTREE_REFERENCE);

              // default fan-out of neo4j spatial is 100
              for (Relationship relationship : rels) {
                Node geom = relationship.getEndNode();
                double[] bbox = (double[]) geom.getProperty(Config.BBoxName);
                MyRectangle rectangle = new MyRectangle(bbox);
                if (rectangle.intersect(queryRect) != null) {
                  candidateIds.add(geom.getId());
                  candidate_count++;
                }
              }
              long time = System.currentTimeMillis() - start;
              range_query_time += time;
              levelTime += time;

              Util.println("Execute percentage: " + nodeIndex / overlap_MBR_list.size());
              start = System.currentTimeMillis();
            }
            if (outputLevelInfo) {
              logWriteLine = String.format("level %d\n", level_index);
              logWriteLine += String.format("Located in nodes: %d\n", candidate_count);
              logWriteLine += String.format("level %d time: %d", level_index, levelTime);
              Util.println(logWriteLine);
            }
            tx.success();
            tx.close();
            candidateSet.put(minSpaID, candidateIds);
          }
          return candidateSet;
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
    return null;
  }



  /**
   * Does not consider condition that area of query rectangle is 0. If so, such function cannot make
   * the correct decision.
   *
   * @param query_Graph
   * @param limit
   */
  public void Query(Query_Graph query_Graph, int limit) {
    try {
      clearTrackingVariables();
      String logWriteLine = "";

      // <spa_id, rectangle> all query rectangles
      HashMap<Integer, MyRectangle> spa_predicates = new HashMap<Integer, MyRectangle>();

      // <spa_id, <neighbor_id, hop_num>> hop num of each node in the query graph
      // with regard to each query node with spatial predicate. Do not know why it is not used.
      HashMap<Integer, HashMap<Integer, Integer>> min_hop =
          new HashMap<Integer, HashMap<Integer, Integer>>();
      // <spa_id, <neighbor_id, size_property_name>>
      HashMap<Integer, HashMap<Integer, HashSet<String>>> PN_size_propertyname =
          new HashMap<Integer, HashMap<Integer, HashSet<String>>>();
      // <spa_id, <neighbor_id, list_property_name>>
      HashMap<Integer, HashMap<Integer, HashSet<String>>> PN_list_propertyname =
          new HashMap<Integer, HashMap<Integer, HashSet<String>>>();

      // <spa_id, <end_id, path_name>> (path_name: PN_a_endid)
      HashMap<Integer, HashMap<Integer, HashSet<String>>> spaPathsMap = recognizePaths(query_Graph);

      // Construct min_hop_array for the query graph
      // int[][] min_hop_array = Ini_Minhop(query_Graph);

      // Construct min_hop (compact structure for min_hop_array
      // and NL_size_propertyname and NL_list_propertyname
      for (int i = 0; i < query_Graph.Has_Spa_Predicate.length; i++)
        if (query_Graph.Has_Spa_Predicate[i])
          spa_predicates.put(i, query_Graph.spa_predicate[i]);

      for (int spaID : spaPathsMap.keySet()) {
        PN_size_propertyname.put(spaID, new HashMap<Integer, HashSet<String>>());
        PN_list_propertyname.put(spaID, new HashMap<Integer, HashSet<String>>());
        for (int neighborID : spaPathsMap.get(spaID).keySet()) {
          PN_size_propertyname.get(spaID).put(neighborID, new HashSet<String>());
          PN_list_propertyname.get(spaID).put(neighborID, new HashSet<String>());
          for (String PNName : spaPathsMap.get(spaID).get(neighborID)) {
            PN_size_propertyname.get(spaID).get(neighborID).add(PNName + "_size");
            PN_list_propertyname.get(spaID).get(neighborID).add(PNName);

          }
        }
      }


      if (outputLevelInfo) {
        logWriteLine = String.format("min_hop: %s\nNL_property: %s", min_hop, PN_size_propertyname);
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

            for (int key : spa_predicates.keySet()) {
              MyRectangle queryRectangle = spa_predicates.get(key);

              MyRectangle intersect = MBR.intersect(queryRectangle);
              if (intersect != null) {
                // all overlapped nodes
                // overlap_MBR_list.get(key).add(node);
                overlap_MBR_list.add(node);

                // record the next level tree nodes
                Iterable<Relationship> rels =
                    node.getRelationships(RTreeRel.RTREE_CHILD, Direction.OUTGOING);
                for (Relationship relationship : rels)
                  next_list.add(relationship.getEndNode());
              }
            }
          } else
            throw new Exception(String.format("node %d does not has \"bbox\" property", node));
        }

        if (outputLevelInfo) {
          logWriteLine = String.format("level %d time: %d", level_index,
              System.currentTimeMillis() - startLevel);
          Util.println(logWriteLine);
          OwnMethods.WriteFile(logPath, true, logWriteLine + "\n");
        }

        if (overlap_MBR_list.isEmpty() == true) {
          Util.println("No result satisfy the query.");
          tx.success();
          tx.close();
          setQueryStatistics(QueryType.LAGAQ_RANGE);
          return;
        }

        // traverse to the leaf node level and start to form the cypher query
        if (overlap_MBR_list.isEmpty() == false && next_list.isEmpty()) {
          overlap_leaf_node_count = overlap_MBR_list.size();
          // <spa_id, card>
          HashMap<Integer, Double> spa_cards = new HashMap<Integer, Double>();
          for (int key : spa_predicates.keySet())
            spa_cards.put(key, 0.0);

          // <spa_id, <neighbor_id, card>>
          HashMap<Integer, HashMap<Integer, HashMap<String, Double>>> NL_cards =
              new HashMap<Integer, HashMap<Integer, HashMap<String, Double>>>();
          for (int spa_id : PN_size_propertyname.keySet()) {
            NL_cards.put(spa_id, new HashMap<Integer, HashMap<String, Double>>());
            for (int neighborID : PN_size_propertyname.get(spa_id).keySet()) {
              NL_cards.get(spa_id).put(neighborID, new HashMap<String, Double>());
              for (String propertyName : PN_size_propertyname.get(spa_id).get(neighborID))
                NL_cards.get(spa_id).get(neighborID).put(propertyName, 0.0);
            }
          }


          for (Node node : overlap_MBR_list) {
            double[] bbox = (double[]) node.getProperty("bbox");
            MyRectangle MBR = new MyRectangle(bbox[0], bbox[1], bbox[2], bbox[3]);
            // OwnMethods.Print(MBR);
            double MBR_area = MBR.area();
            int spa_count = (Integer) node.getProperty("count");

            for (int key : spa_predicates.keySet()) {
              MyRectangle queryRectangle = spa_predicates.get(key);

              MyRectangle intersect = MBR.intersect(queryRectangle);
              // calculate overlapped ratio compared to the MBR area
              double ratio;
              if (MBR_area == 0)
                ratio = 1;
              else
                ratio = intersect.area() / MBR_area;
              // estimate spatial predicate cardinality
              spa_cards.put(key, (spa_cards.get(key) + ratio * spa_count));

              // estimate NL cardinality
              // HashMap<Integer, Double> NL_cards_vector = NL_cards.get(key);
              // for ( int neighbor_id : NL_cards_vector.keySet())
              // {
              // int NL_label_size = (Integer)
              // node.getProperty(PN_size_propertyname.get(key).get(neighbor_id));
              // NL_cards_vector.put(neighbor_id, (NL_cards_vector.get(neighbor_id) + ratio *
              // NL_label_size));
              // }
              for (int neighborID : NL_cards.get(key).keySet())
                for (String propertyName : NL_cards.get(key).get(neighborID).keySet()) {
                  if (node.hasProperty(propertyName)) {
                    int PNSize = (Integer) node.getProperty(propertyName);
                    NL_cards.get(key).get(neighborID).put(propertyName,
                        NL_cards.get(key).get(neighborID).get(propertyName) + ratio * PNSize);
                  }
                }
            }
          }

          // find the query node with the minimum cardinality
          double min_spa_card = Double.MAX_VALUE, min_NL_card = Double.MAX_VALUE;
          int minSpaID = 0;// the spatial predicate with minimum cardinality
          int min_NL_spa_id = 0, min_NL_neighbor_id = 0;// min NL spatial id and neighbor id
          String minPNListPropertyname = null, minPNSizePropertyname = "";

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

            // HashMap<Integer, Double> NL_cards_vector = NL_cards.get(key);
            for (int neighbor_id : PN_list_propertyname.get(key).keySet())
              for (String properName : PN_list_propertyname.get(key).get(neighbor_id)) {
                double card = NL_cards.get(key).get(neighbor_id).get(properName + "_size");
                if (card < min_NL_card) {
                  min_NL_spa_id = key;
                  min_NL_neighbor_id = neighbor_id;
                  min_NL_card = card;
                  minPNListPropertyname = properName;
                  minPNSizePropertyname = properName += "_size";
                }
                if (outputLevelInfo) {
                  logWriteLine = String.format("%d %d %s estimate size: %f", key, neighbor_id,
                      properName, card);
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

          // construct the NL_list with the highest selectivity
          long start1 = System.currentTimeMillis();

          HashSet<Integer> min_NL_list = null;
          int realMinPNSize = Integer.MAX_VALUE;
          if (minPNListPropertyname != null) {
            min_NL_list = new HashSet<Integer>();
            for (Node node : overlap_MBR_list) {
              if (node.hasProperty(minPNListPropertyname)) {
                // here id is graph id rather than neo4j id
                int[] NL_list_label = (int[]) node.getProperty(minPNListPropertyname);
                for (int node_id : NL_list_label)
                  min_NL_list.add(node_id);
              }
            }
            realMinPNSize = min_NL_list.size();
          }

          range_query_time += System.currentTimeMillis() - startLevel;

          if (outputLevelInfo) {
            logWriteLine = String.format("min_NL_list size is %d\n", realMinPNSize);
            if (realMinPNSize < min_spa_card)
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

          // if PN is more selective than spatial predicate
          if (forceGraphFirst) {
            Util.println("force graph first even min PN size is " + realMinPNSize);
            realMinPNSize = 0;
          }

          if (realMinPNSize < min_spa_card) {
            Util.println("PN is more selective");
            int index = 0;
            ArrayList<Long> id_pos_list = new ArrayList<Long>();
            double idIndex = 0;
            for (int id : min_NL_list) {
              idIndex++;
              // graph id to neo4j pos id
              id_pos_list.add(graph_pos_map_list[id]);
              index++;
              if (index == 500) {
                Util.println("Executed percentage: " + idIndex / min_NL_list.size());
                String query = formSubgraphQuery(query_Graph, -1, Enums.Explain_Or_Profile.Profile,
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
              Util.println("Executed percentage: " + idIndex / min_NL_list.size());
              String query = formSubgraphQuery(query_Graph, -1, Enums.Explain_Or_Profile.Profile,
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
            setQueryStatistics(QueryType.LAGAQ_RANGE);
            return;
          } else // if spatial predicate is more selective
          {
            MyRectangle queryRect = spa_predicates.get(minSpaID);
            // get located in nodes
            candidate_count = 0;
            int levelTime = 0;
            level_index++;
            double nodeIndex = 0;
            // construct a cypher query for each tree leaf node
            // which consists of several spatial objects
            for (Node node : overlap_MBR_list) {
              nodeIndex++;
              long start = System.currentTimeMillis();
              Iterable<Relationship> rels =
                  node.getRelationships(Direction.OUTGOING, Labels.RTreeRel.RTREE_REFERENCE);

              // default fan-out of neo4j spatial is 100
              ArrayList<Long> ids = new ArrayList<Long>(100);
              for (Relationship relationship : rels) {
                Node geom = relationship.getEndNode();
                double[] bbox = (double[]) geom.getProperty("bbox");
                MyRectangle rectangle = new MyRectangle(bbox);
                if (rectangle.intersect(queryRect) != null) {
                  ids.add(geom.getId());
                  candidate_count++;
                }
              }
              long time = System.currentTimeMillis() - start;
              range_query_time += time;
              levelTime += time;

              if (ids.size() != 0) {
                Util.println("Execute percentage: " + nodeIndex / overlap_MBR_list.size());
                start = System.currentTimeMillis();
                String query = formSubgraphQuery_ForSpatialFirst_Block(query_Graph, limit,
                    Enums.Explain_Or_Profile.Profile, spa_predicates, minSpaID, ids,
                    min_hop.get(minSpaID), node);// bug here
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

                  Util.println("OwnMethods.gethits: " + OwnMethods.GetTotalDBHits(planDescription));
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
              logWriteLine += String.format("Located in nodes: %d\n", candidate_count);
              logWriteLine += String.format("level %d time: %d", level_index, levelTime);
              Util.println(logWriteLine);
              OwnMethods.WriteFile(logPath, true, logWriteLine + "\n\n");
            }
            tx.success();
            tx.close();
            setQueryStatistics(QueryType.LAGAQ_RANGE);
            return;
          }
        }

        cur_list = next_list;
        next_list = new LinkedList<Node>();
        level_index++;
      }
      tx.success();
      tx.close();
      setQueryStatistics(QueryType.LAGAQ_RANGE);
      return;
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
      Enums.Explain_Or_Profile explain_Or_Profile, HashMap<Integer, MyRectangle> spa_predicates,
      int pos, ArrayList<Long> ids, HashMap<Integer, Integer> NL_hopnum, Node node,
      int[][] min_hop_array) {
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
      Enums.Explain_Or_Profile explain_Or_Profile, HashMap<Integer, MyRectangle> spa_predicates,
      int pos, ArrayList<Long> ids, int[][] min_hop_array) {
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
   * Check a node covers all the paths
   *
   * @param node It has to be a node in RisoTree. It cannot be a spatial object.
   * @param paths
   * @return
   */
  public static boolean checkPaths(Node node, LinkedList<String> paths) {
    for (String path : paths) {
      // if ( node.hasProperty(path + "_size"))
      // {
      // if ( (Integer) node.getProperty(path + "_size") > 0)
      // continue;
      // }
      // else
      {
        if (node.hasProperty(path)) {
          // int[] array = (int[]) node.getProperty(path);
          // if ( array.length > 0)
          continue;
        } else
          return false;
      }

    }
    return true;
  }


  /**
   * Check a node covers all the paths
   * 
   * @param node It has to be a node in RisoTree. It cannot be a spatial object.
   * @param paths
   * @return
   */
  public static boolean checkPaths(Node node, Set<String> paths) {
    int coverCount = 0;
    for (String key : node.getPropertyKeys()) {
      if (paths.contains(key)) {
        coverCount++;
        if (coverCount == paths.size()) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * form the cypher query for MBR block
   * 
   * @param query_Graph
   * @param limit -1 is no limit
   * @param explain_Or_Profile -1 is Explain; 1 is Profile; the rest is nothing
   * @param pos query graph node id with the trigger spatial predicate
   * @param id corresponding graph spatial node id (neo4j pos id)
   * @return
   */
  public static String formQuery_KNN(Query_Graph query_Graph, int limit,
      Enums.Explain_Or_Profile explain_Or_Profile, int pos, Long id) {
    String query = "";
    query += CypherEncoder.getMatchPrefix(explain_Or_Profile);
    query += " ";
    query += CypherEncoder.getMatchGraphSkeletonString(query_Graph);

    query += "\n where\n";

    // id
    query += String.format(" (id(a%d)=%d", pos, id);
    query += ")\n";

    // return
    query += "\n return " + CypherEncoder.getMatchReturnString(query_Graph);

    if (limit != -1)
      query += " " + CypherEncoder.getLimit(limit);

    return query;
  }

  /**
   * Query function with KNN predicate.
   * 
   * @param query_Graph
   * @param K
   */
  public List<long[]> LAGAQ_KNN(Query_Graph query_Graph, int K) throws Exception {
    clearTrackingVariables();
    long sumStart = System.currentTimeMillis();
    List<long[]> resultIDs = new ArrayList<long[]>();

    Map<Integer, MyRectangle> spatialPredicatesMap = query_Graph.getSpatialPredicates();
    if (spatialPredicatesMap.size() != 1) {
      throw new Exception(String.format("query graph has %d spatial predicates rather than 1!",
          spatialPredicatesMap.size()));
    }
    int querySpatialVertexID = spatialPredicatesMap.keySet().iterator().next();
    MyRectangle queryRectangle = spatialPredicatesMap.get(querySpatialVertexID);
    MyPoint queryLoc = new MyPoint(queryRectangle.min_x, queryRectangle.min_y);
    Label kNNLabel = Label.label(query_Graph.getLabel(querySpatialVertexID));

    HashMap<Integer, HashMap<Integer, HashSet<String>>> spaPathsMap = recognizePaths(query_Graph);
    if (spaPathsMap.size() != 1) {
      throw new Exception(
          String.format("The number of anchor vertex in the LAGAQ-KNN query is %d rather than 1!",
              spaPathsMap.size()));
    }
    Set<String> paths = new HashSet<String>();
    int i = spaPathsMap.keySet().iterator().next();
    for (int j : spaPathsMap.get(i).keySet()) {
      for (String path : spaPathsMap.get(i).get(j)) {
        paths.add(path);
      }
    }

    String[] columnNames = CypherEncoder.getReturnColumnNames(query_Graph);

    long start = System.currentTimeMillis();
    Transaction tx = dbservice.beginTx();
    Node root_node = RTreeUtility.getRTreeRoot(dbservice, dataset);
    /**
     * If root node does not have label paths, these codes will not be used.
     */
    // if ( checkPaths(root_node, paths) == false)
    // {
    // tx.success();
    // tx.close();
    // return resultIDs;
    // }

    PriorityQueue<Element> queue = new PriorityQueue<Element>(100, new KNNComparator());
    queue.add(new Element(root_node, 0));

    boolean reachKCount = false;
    while (!reachKCount && !queue.isEmpty()) {
      Element element = queue.poll();
      Node node = element.node;
      // Tree non-leaf node
      if (node.hasRelationship(Labels.RTreeRel.RTREE_CHILD, Direction.OUTGOING)) {
        Iterable<Relationship> rels =
            node.getRelationships(Labels.RTreeRel.RTREE_CHILD, Direction.OUTGOING);
        for (Relationship relationship : rels) {
          Node child = relationship.getEndNode();
          long start1 = System.currentTimeMillis();
          if (child.hasRelationship(RTreeRel.RTREE_REFERENCE, Direction.OUTGOING)) {
            if (checkPaths(child, paths) == false) {
              check_paths_time += System.currentTimeMillis() - start1;
              continue;
            }
            check_paths_time += System.currentTimeMillis() - start1;
          }
          double[] bbox = (double[]) child.getProperty("bbox");
          MyRectangle MBR = new MyRectangle(bbox[0], bbox[1], bbox[2], bbox[3]);
          queue.add(new Element(child, Util.distance(queryLoc, MBR)));

        }
      }
      // tree leaf node
      else if (node.hasRelationship(Labels.RTreeRel.RTREE_REFERENCE, Direction.OUTGOING)) {
        Iterable<Relationship> rels =
            node.getRelationships(Labels.RTreeRel.RTREE_REFERENCE, Direction.OUTGOING);
        for (Relationship relationship : rels) {
          Node geom = relationship.getEndNode();
          // if (!geom.hasLabel(kNNLabel)) {
          // continue;
          // }
          Object object = geom.getProperty(lon_name);
          if (object == null)
            throw new Exception(
                String.format("Node %d does not have %s property", geom.getId(), lon_name));
          else {
            double lon = (Double) object;
            double lat = (Double) geom.getProperty(lat_name);
            queue.add(new Element(geom, Util.distance(queryLoc, new MyPoint(lon, lat))));
          }
        }
      }
      // spatial object
      else if (Neo4jGraphUtility.isNodeSpatial(node)) {
        visit_spatial_object_count++;
        long id = node.getId();
        queue_time += System.currentTimeMillis() - start;

        start = System.currentTimeMillis();
        String query = formQuery_KNN(query_Graph, K - resultIDs.size(),
            Enums.Explain_Or_Profile.Profile, querySpatialVertexID, id);
        Result result = dbservice.execute(query);
        get_iterator_time += System.currentTimeMillis() - start;

        start = System.currentTimeMillis();
        while (result.hasNext()) {
          Map<String, Object> row = result.next();
          long[] ids = QueryUtil.getResultRowInArray(columnNames, row);
          resultIDs.add(ids);
          // OwnMethods.Print(String.format("%d, %f", id, element.distance));
          if (resultIDs.size() == K) {
            reachKCount = true;
            break;
          }
        }
        iterate_time += System.currentTimeMillis() - start;
        start = System.currentTimeMillis();

        planDescription = result.getExecutionPlanDescription();
        page_hit_count += OwnMethods.GetTotalDBHits(planDescription);
        // OwnMethods.Print(planDescription);
      } else
        throw new Exception(String.format("Node %d does not affiliate to any type!", node.getId()));
    }

    queue_time += System.currentTimeMillis() - start;
    tx.success();
    tx.close();
    run_time = System.currentTimeMillis() - sumStart;
    result_count = resultIDs.size();
    setQueryStatistics(QueryType.LAGAQ_KNN);
    return resultIDs;
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
                    node.getRelationships(RTreeRel.RTREE_CHILD, Direction.OUTGOING);
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
                String query =
                    formSubgraphQuery_HMBR(query_Graph, -1, Enums.Explain_Or_Profile.Profile,
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
              String query =
                  formSubgraphQuery_HMBR(query_Graph, -1, Enums.Explain_Or_Profile.Profile,
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
                    Enums.Explain_Or_Profile.Profile, spa_predicates, minSpaID, ids,
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

  /**
   * For the query for join
   * 
   * @param query_Graph
   * @param pos Two query vertices in the join predicate
   * @param idPair [id1, id2]
   * @param limit
   * @param explain_Or_Profile
   * @return
   */
  public static String formQueryLAGAQ_Join(Query_Graph query_Graph, ArrayList<Integer> pos,
      Long[] idPair, int limit, Enums.Explain_Or_Profile explain_Or_Profile) {
    String query = "";
    query += CypherEncoder.getMatchPrefix(explain_Or_Profile);
    query += " ";
    query += CypherEncoder.getMatchGraphSkeletonString(query_Graph);

    query += " where\n";

    // id
    query += String.format(" id(a%d)=%d and", pos.get(0), idPair[0]);
    query += String.format(" id(a%d)=%d", pos.get(1), idPair[1]);
    query += "\n";

    // return
    query += " return id(a0)";
    for (int i = 1; i < query_Graph.graph.size(); i++)
      query += String.format(",id(a%d)", i);

    if (limit != -1)
      query += String.format(" limit %d", limit);

    return query;
  }

  /**
   * Use the property key "PN_...list" to decide whether the node is a leaf node. (May needs to be
   * modified when property name changes)
   * 
   * @param node the given node
   * @return
   */
  public boolean isNodeLeaf(Node node) {
    // Iterable<String> keys = node.getPropertyKeys();
    // for ( String key : keys)
    // {
    // if (key.startsWith("PN") && key.endsWith("list"))
    // return true;
    // }
    // return false;
    return node.hasRelationship(RTreeRel.RTREE_REFERENCE, Direction.OUTGOING);
  }

  /**
   * Decide whether child of a given node is a leaf node
   * 
   * @param node the given node
   * @return
   */
  public boolean isChildNodeLeaf(Node node) {
    try {
      Iterable<Relationship> rels = node.getRelationships(RTreeRel.RTREE_CHILD, Direction.OUTGOING);
      Iterator<Relationship> iterator = rels.iterator();
      if (iterator.hasNext()) {
        Node child = iterator.next().getEndNode();
        return isNodeLeaf(child);
      } else
        throw new Exception("This node is not a non-leaf node in the tree");
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
    return false;
  }

  public HashMap<Integer, ArrayList<Integer>> constructPN(Node node,
      HashMap<Integer, HashSet<String>> paths, ArrayList<Integer> overlapVertices) {
    HashMap<Integer, ArrayList<Integer>> pnMap = new HashMap<>();
    for (int id : overlapVertices) {
      ArrayList<Integer> pn = new ArrayList<>();
      boolean isFirst = true;
      for (String pnName : paths.get(id)) {
        int[] l = (int[]) node.getProperty(pnName);
        if (isFirst) {
          for (int element : l)
            pn.add(element);
          isFirst = false;
        } else
          pn = Util.sortedListIntersect(pn, l);
      }
      pnMap.put(id, pn);
    }

    return pnMap;
  }

  public List<Long[]> spatialJoinRTree(double distance, ArrayList<Integer> pos,
      HashMap<Integer, HashMap<Integer, HashSet<String>>> spaPathsMap) {

    LinkedList<String> leftpaths = new LinkedList<String>();
    LinkedList<String> rightpaths = new LinkedList<String>();

    // pos[0]--lp
    // pos[1]--rp
    HashMap<Integer, HashSet<String>> lp = spaPathsMap.get(pos.get(0));
    for (int endID : lp.keySet())
      for (String path : lp.get(endID))
        leftpaths.add(path);

    HashMap<Integer, HashSet<String>> rp = spaPathsMap.get(pos.get(1));
    for (int endID : rp.keySet())
      for (String path : rp.get(endID))
        rightpaths.add(path);

    List<Long[]> result = new LinkedList<Long[]>();
    Queue<NodeAndRec[]> queue = new LinkedList<NodeAndRec[]>();
    Transaction tx = dbservice.beginTx();
    Node root = RTreeUtility.getRTreeRoot(dbservice, dataset);
    MyRectangle rootMBR = RTreeUtility.getNodeMBR(root);

    NodeAndRec[] pair = new NodeAndRec[2];
    pair[0] = new NodeAndRec(root, rootMBR);
    pair[1] = new NodeAndRec(root, rootMBR);
    queue.add(pair);
    while (!queue.isEmpty()) {
      NodeAndRec[] element = queue.poll();
      NodeAndRec left = element[0];
      NodeAndRec right = element[1];

      LinkedList<NodeAndRec> leftChildren = new LinkedList<NodeAndRec>();
      LinkedList<NodeAndRec> rightChildern = new LinkedList<NodeAndRec>();

      // node is leaf
      if (isNodeLeaf(left.node)) {
        Iterable<Relationship> rels = left.node.getRelationships(Direction.OUTGOING);
        for (Relationship relationship : rels) {
          Node child = relationship.getEndNode();
          MyRectangle mbr = RTreeUtility.getNodeMBR(child);
          if (Util.distance(mbr, right.rectangle) <= distance)
            leftChildren.add(new NodeAndRec(child, mbr));
        }

        rels = right.node.getRelationships(Direction.OUTGOING);
        for (Relationship relationship : rels) {
          Node child = relationship.getEndNode();
          MyRectangle mbr = RTreeUtility.getNodeMBR(child);
          if (Util.distance(mbr, left.rectangle) <= distance)
            rightChildern.add(new NodeAndRec(child, mbr));
        }

        for (NodeAndRec leftChild : leftChildren)
          for (NodeAndRec rightChild : rightChildern)
            if (Util.distance(leftChild.rectangle, rightChild.rectangle) <= distance) {
              long id1 = leftChild.node.getId();
              long id2 = rightChild.node.getId();
              if (id1 != id2)
                result.add(new Long[] {id1, id2});
            }
      } else {
        Iterable<Relationship> rels =
            left.node.getRelationships(RTreeRel.RTREE_CHILD, Direction.OUTGOING);
        long start = System.currentTimeMillis();
        Iterator<Relationship> iterator = rels.iterator();
        Node tempNode = iterator.next().getEndNode();
        boolean flag = tempNode.hasRelationship(RTreeRel.RTREE_REFERENCE, Direction.OUTGOING);
        // check_paths_time += System.currentTimeMillis() - start;

        // no path checking in this case
        if (flag == false)
        // if (isChildNodeLeaf(left.node) == false)
        {
          for (Relationship relationship : rels) {
            Node child = relationship.getEndNode();
            MyRectangle mbr = RTreeUtility.getNodeMBR(child);
            if (Util.distance(mbr, right.rectangle) <= distance)
              leftChildren.add(new NodeAndRec(child, mbr));
          }

          rels = right.node.getRelationships(Direction.OUTGOING);
          for (Relationship relationship : rels) {
            Node child = relationship.getEndNode();
            MyRectangle mbr = RTreeUtility.getNodeMBR(child);
            if (Util.distance(mbr, left.rectangle) <= distance)
              rightChildern.add(new NodeAndRec(child, mbr));
          }

          for (NodeAndRec leftChild : leftChildren) {
            for (NodeAndRec rightChild : rightChildern) {
              if (Util.distance(leftChild.rectangle, rightChild.rectangle) <= distance) {
                NodeAndRec[] nodeAndRecs = new NodeAndRec[2];
                nodeAndRecs[0] = new NodeAndRec(leftChild.node, leftChild.rectangle);
                nodeAndRecs[1] = new NodeAndRec(rightChild.node, rightChild.rectangle);
                queue.add(nodeAndRecs);
              }
            }
          }
        }
        // child is leaf, check childern's paths
        else {
          for (Relationship relationship : rels) {
            Node child = relationship.getEndNode();
            long start1 = System.currentTimeMillis();
            if (checkPaths(child, leftpaths) == false) {
              check_paths_time += System.currentTimeMillis() - start1;
              continue;
            }
            check_paths_time += System.currentTimeMillis() - start1;
            MyRectangle mbr = RTreeUtility.getNodeMBR(child);
            if (Util.distance(mbr, right.rectangle) <= distance)
              leftChildren.add(new NodeAndRec(child, mbr));
          }

          rels = right.node.getRelationships(Direction.OUTGOING);
          for (Relationship relationship : rels) {
            Node child = relationship.getEndNode();
            long start1 = System.currentTimeMillis();
            if (checkPaths(child, rightpaths) == false) {
              check_paths_time += System.currentTimeMillis() - start1;
              continue;
            }
            check_paths_time += System.currentTimeMillis() - start1;
            MyRectangle mbr = RTreeUtility.getNodeMBR(child);
            if (Util.distance(mbr, left.rectangle) <= distance)
              rightChildern.add(new NodeAndRec(child, mbr));
          }

          for (NodeAndRec leftChild : leftChildren) {
            for (NodeAndRec rightChild : rightChildern) {
              if (Util.distance(leftChild.rectangle, rightChild.rectangle) <= distance) {
                NodeAndRec[] nodeAndRecs = new NodeAndRec[2];
                nodeAndRecs[0] = new NodeAndRec(leftChild.node, leftChild.rectangle);
                nodeAndRecs[1] = new NodeAndRec(rightChild.node, rightChild.rectangle);
                queue.add(nodeAndRecs);
              }
            }
          }
        }
      }
    }
    tx.success();
    tx.close();
    return result;
  }

  /**
   * Decide whether two given PN maps have intersect in all overlapped query vertices.
   * 
   * @param overlapVertices overlapped query vertices
   * @param pnListLeft <id, pathNeighbors> for the join left predicate
   * @param pnListRight <id, pathNeighbors> for the join right predicate
   * @return
   */
  public static boolean isIntersect(ArrayList<Integer> overlapVertices,
      HashMap<Integer, ArrayList<Integer>> pnListLeft,
      HashMap<Integer, ArrayList<Integer>> pnListRight) {
    for (int id : overlapVertices) {
      if (Util.isSortedIntersect(pnListLeft.get(id), pnListRight.get(id)) == false)
        return false;
    }
    return true;
  }

  public List<Long[]> spatialJoinRTreeOverlap(double distance, ArrayList<Integer> pos,
      ArrayList<Label> targetLabels,
      HashMap<Integer, HashMap<Integer, HashSet<String>>> spaPathsMap) {
    LinkedList<String> leftpaths = new LinkedList<>();
    LinkedList<String> rightpaths = new LinkedList<>();

    // Set<String> leftpaths = new HashSet<>();
    // Set<String> rightpaths = new HashSet<>();

    // pos[0]--lp
    // pos[1]--rp
    Label leftLabel = targetLabels.get(0);
    Label rightLabel = targetLabels.get(1);
    HashMap<Integer, HashSet<String>> lp = spaPathsMap.get(pos.get(0));
    for (int endID : lp.keySet()) {
      for (String path : lp.get(endID)) {
        leftpaths.add(path);
      }
    }

    HashMap<Integer, HashSet<String>> rp = spaPathsMap.get(pos.get(1));
    for (int endID : rp.keySet())
      for (String path : rp.get(endID))
        rightpaths.add(path);

    // find all the overlap vertices
    ArrayList<Integer> overlapVertices = new ArrayList<Integer>();
    for (int lid : lp.keySet())
      for (int rid : rp.keySet())
        if (lid == rid)
          overlapVertices.add(lid);

    List<Long[]> result = new LinkedList<Long[]>();
    Queue<NodeAndRec[]> queue = new LinkedList<NodeAndRec[]>();
    Transaction tx = dbservice.beginTx();
    Node root = RTreeUtility.getRTreeRoot(dbservice, dataset);
    MyRectangle rootMBR = RTreeUtility.getNodeMBR(root);

    NodeAndRec[] pair = new NodeAndRec[2];
    pair[0] = new NodeAndRec(root, rootMBR);
    pair[1] = new NodeAndRec(root, rootMBR);
    queue.add(pair);
    while (!queue.isEmpty()) {
      NodeAndRec[] element = queue.poll();
      NodeAndRec left = element[0];
      NodeAndRec right = element[1];

      LinkedList<NodeAndRec> leftChildren = new LinkedList<NodeAndRec>();
      LinkedList<NodeAndRec> rightChildern = new LinkedList<NodeAndRec>();

      // node is leaf
      if (isNodeLeaf(left.node)) {
        Iterable<Relationship> rels = left.node.getRelationships(Direction.OUTGOING);
        for (Relationship relationship : rels) {
          Node child = relationship.getEndNode();
          MyRectangle mbr = RTreeUtility.getNodeMBR(child);
          if (Util.distance(mbr, right.rectangle) <= distance)
            leftChildren.add(new NodeAndRec(child, mbr));
        }

        rels = right.node.getRelationships(Direction.OUTGOING);
        for (Relationship relationship : rels) {
          Node child = relationship.getEndNode();
          MyRectangle mbr = RTreeUtility.getNodeMBR(child);
          if (Util.distance(mbr, left.rectangle) <= distance)
            rightChildern.add(new NodeAndRec(child, mbr));
        }

        for (NodeAndRec leftChild : leftChildren) {
          if (!leftChild.node.hasLabel(leftLabel)) {
            continue;
          }
          for (NodeAndRec rightChild : rightChildern) {
            if (!rightChild.node.hasLabel(rightLabel)) {
              continue;
            }
            if (Util.distance(leftChild.rectangle, rightChild.rectangle) <= distance) {
              long id1 = leftChild.node.getId();
              long id2 = rightChild.node.getId();
              if (id1 != id2)
                result.add(new Long[] {id1, id2});
            }
          }
        }
      } else { // this node is non-leaf
        Iterable<Relationship> rels =
            left.node.getRelationships(RTreeRel.RTREE_CHILD, Direction.OUTGOING);
        Iterator<Relationship> iterator = rels.iterator();
        Node tempNode = iterator.next().getEndNode();
        // flag means whether tempNode is a leaf node
        boolean flag = tempNode.hasRelationship(RTreeRel.RTREE_REFERENCE, Direction.OUTGOING);

        // no path checking in this case
        if (!flag) {
          for (Relationship relationship : rels) {
            Node child = relationship.getEndNode();
            MyRectangle mbr = RTreeUtility.getNodeMBR(child);
            if (Util.distance(mbr, right.rectangle) <= distance)
              leftChildren.add(new NodeAndRec(child, mbr));
          }

          rels = right.node.getRelationships(Direction.OUTGOING);
          for (Relationship relationship : rels) {
            Node child = relationship.getEndNode();
            MyRectangle mbr = RTreeUtility.getNodeMBR(child);
            if (Util.distance(mbr, left.rectangle) <= distance)
              rightChildern.add(new NodeAndRec(child, mbr));
          }

          for (NodeAndRec leftChild : leftChildren) {
            for (NodeAndRec rightChild : rightChildern) {
              if (Util.distance(leftChild.rectangle, rightChild.rectangle) <= distance) {
                NodeAndRec[] nodeAndRecs = new NodeAndRec[2];
                nodeAndRecs[0] = new NodeAndRec(leftChild.node, leftChild.rectangle);
                nodeAndRecs[1] = new NodeAndRec(rightChild.node, rightChild.rectangle);
                queue.add(nodeAndRecs);
              }
            }
          }
        }
        // child is leaf, check childern's paths
        else {
          LinkedList<HashMap<Integer, ArrayList<Integer>>> pnListLeft = new LinkedList<>();
          for (Relationship relationship : rels) {
            Node child = relationship.getEndNode();
            long start1 = System.currentTimeMillis();
            if (checkPaths(child, leftpaths) == false) {
              check_paths_time += System.currentTimeMillis() - start1;
              continue;
            }
            check_paths_time += System.currentTimeMillis() - start1;
            MyRectangle mbr = RTreeUtility.getNodeMBR(child);
            if (Util.distance(mbr, right.rectangle) <= distance) {
              leftChildren.add(new NodeAndRec(child, mbr));
              start1 = System.currentTimeMillis();
              HashMap<Integer, ArrayList<Integer>> pn = constructPN(child, lp, overlapVertices);
              pnListLeft.add(pn);
              check_overlap_time += System.currentTimeMillis() - start1;
            }
          }

          LinkedList<HashMap<Integer, ArrayList<Integer>>> pnListRight = new LinkedList<>();
          rels = right.node.getRelationships(Direction.OUTGOING);
          for (Relationship relationship : rels) {
            Node child = relationship.getEndNode();
            long start1 = System.currentTimeMillis();
            if (checkPaths(child, rightpaths) == false) {
              check_paths_time += System.currentTimeMillis() - start1;
              continue;
            }
            check_paths_time += System.currentTimeMillis() - start1;
            MyRectangle mbr = RTreeUtility.getNodeMBR(child);
            if (Util.distance(mbr, left.rectangle) <= distance) {
              rightChildern.add(new NodeAndRec(child, mbr));
              start1 = System.currentTimeMillis();
              HashMap<Integer, ArrayList<Integer>> pn = constructPN(child, rp, overlapVertices);
              pnListRight.add(pn);
              check_overlap_time += System.currentTimeMillis() - start1;
            }
          }

          Iterator<HashMap<Integer, ArrayList<Integer>>> iteratorLeft = pnListLeft.iterator();
          for (NodeAndRec leftChild : leftChildren) {
            HashMap<Integer, ArrayList<Integer>> pnLeft = iteratorLeft.next();
            Iterator<HashMap<Integer, ArrayList<Integer>>> iteratorRight = pnListRight.iterator();
            for (NodeAndRec rightChild : rightChildern) {
              HashMap<Integer, ArrayList<Integer>> pnRight = iteratorRight.next();
              long start1 = System.currentTimeMillis();
              boolean isIntersect = isIntersect(overlapVertices, pnLeft, pnRight);
              check_overlap_time += System.currentTimeMillis() - start1;
              if (Util.distance(leftChild.rectangle, rightChild.rectangle) <= distance
                  && isIntersect) {
                NodeAndRec[] nodeAndRecs = new NodeAndRec[2];
                nodeAndRecs[0] = new NodeAndRec(leftChild.node, leftChild.rectangle);
                nodeAndRecs[1] = new NodeAndRec(rightChild.node, rightChild.rectangle);
                queue.add(nodeAndRecs);
              }
            }
          }
        }
      }
    }
    tx.success();
    tx.close();
    return result;
  }

  public List<Long[]> LAGAQ_Join(Query_Graph query_Graph, double distance) throws Exception {
    clearTrackingVariables();
    long totalStart = System.currentTimeMillis();
    List<Long[]> resultPairs = new LinkedList<Long[]>();

    int count = 0;
    ArrayList<Integer> pos = new ArrayList<Integer>(2);
    ArrayList<Label> targetLabels = new ArrayList<>(2);
    for (int i = 0; i < query_Graph.Has_Spa_Predicate.length; i++) {
      if (query_Graph.Has_Spa_Predicate[i] == true) {
        pos.add(i);
        targetLabels.add(Label.label(query_Graph.getLabel(i)));
        count++;
      }
    }
    if (count != 2) {
      throw new Exception(String
          .format("Number of query graph spatial predicate is " + "%d, it should be 2!", count));
    }

    HashMap<Integer, HashMap<Integer, HashSet<String>>> spaPathsMap = recognizePaths(query_Graph);

    long start = System.currentTimeMillis();
    Util.println(pos);
    Util.println(spaPathsMap);
    List<Long[]> idPairs = this.spatialJoinRTreeOverlap(distance, pos, targetLabels, spaPathsMap);
    // List<Long[]> idPairs = this.spatialJoinRTree(distance, pos, spaPathsMap);
    join_time = System.currentTimeMillis() - start;
    join_result_count = idPairs.size();
    Util.println("join time: " + join_time);
    Util.println("candidate pairs count: " + idPairs.size());

    if (joinBatch) {
      batchJoin(query_Graph, pos, idPairs, resultPairs);
    } else {
      for (Long[] idPair : idPairs) {
        start = System.currentTimeMillis();
        String query =
            formQueryLAGAQ_Join(query_Graph, pos, idPair, 1, Enums.Explain_Or_Profile.Profile);
        Result result = dbservice.execute(query);
        get_iterator_time += System.currentTimeMillis() - start;

        start = System.currentTimeMillis();
        if (result.hasNext()) {
          result.next();
          resultPairs.add(idPair);
          // OwnMethods.Print(String.format("%d, %f", id, element.distance));
        }
        iterate_time += System.currentTimeMillis() - start;
        start = System.currentTimeMillis();

        planDescription = result.getExecutionPlanDescription();
        page_hit_count += OwnMethods.GetTotalDBHits(planDescription);
      }
      result_count = resultPairs.size();
    }
    run_time = System.currentTimeMillis() - totalStart;
    setQueryStatistics(QueryType.LAGAQ_JOIN);
    return resultPairs;
  }

  private void batchJoin(Query_Graph query_Graph, ArrayList<Integer> pos, List<Long[]> idPairs,
      List<Long[]> resultPairs) {
    int i = -1;
    String query = "";
    for (Long[] idPair : idPairs) {
      if (i == 0) {
        query = formQueryLAGAQ_Join(query_Graph, pos, idPair, 1, Explain_Or_Profile.Profile);
        continue;
      }
      query += " UNION ALL "
          + formQueryLAGAQ_Join(query_Graph, pos, idPair, 1, Explain_Or_Profile.Nothing);
      if (i == joinBatchSize) {
        // runLAGAQJoin(query, resultPairs);
        long start = System.currentTimeMillis();
        Result result = dbservice.execute(query);
        get_iterator_time += System.currentTimeMillis() - start;

        start = System.currentTimeMillis();
        while (result.hasNext()) {
          result.next();
          result_count++;
          // resultPairs.add(idPair);
          // OwnMethods.Print(String.format("%d, %f", id, element.distance));
        }
        iterate_time += System.currentTimeMillis() - start;
        start = System.currentTimeMillis();

        planDescription = result.getExecutionPlanDescription();
        page_hit_count += OwnMethods.GetTotalDBHits(planDescription);

        i = 0;
        query = "";
      }
    }
  }

  // private void runLAGAQJoin(String query, List<Long[]> resultPairs) {
  // long start = System.currentTimeMillis();
  // Result result = dbservice.execute(query);
  // get_iterator_time += System.currentTimeMillis() - start;
  //
  // start = System.currentTimeMillis();
  // if (result.hasNext()) {
  // result.next();
  //// resultPairs.add(idPair);
  // // OwnMethods.Print(String.format("%d, %f", id, element.distance));
  // }
  // iterate_time += System.currentTimeMillis() - start;
  // start = System.currentTimeMillis();
  //
  // planDescription = result.getExecutionPlanDescription();
  // page_hit_count += OwnMethods.GetTotalDBHits(planDescription);
  //
  // }

  private void clearTrackingVariables() {
    Util.println("Clear variables for tracking by setting to 0.");
    run_time = 0;
    page_hit_count = 0;
    get_iterator_time = 0;
    iterate_time = 0;
    result_count = 0;
    check_paths_time = 0;

    range_query_time = 0;
    set_label_time = 0;
    remove_label_time = 0;
    overlap_leaf_node_count = 0;
    candidate_count = 0;

    queue_time = 0;
    visit_spatial_object_count = 0;

    join_result_count = 0;
    join_time = 0;
    check_overlap_time = 0;
  }

  public Map<QueryStatistic, Object> getQueryStatisticMap() {
    return queryStatisticMap;
  }

  private void setQueryStatistics(QueryType queryType) throws Exception {
    queryStatisticMap = new HashMap<>();
    queryStatisticMap.put(QueryStatistic.run_time, run_time);
    queryStatisticMap.put(QueryStatistic.page_hit_count, page_hit_count);
    queryStatisticMap.put(QueryStatistic.get_iterator_time, get_iterator_time);
    queryStatisticMap.put(QueryStatistic.iterate_time, iterate_time);
    queryStatisticMap.put(QueryStatistic.graph_time, get_iterator_time + iterate_time);
    queryStatisticMap.put(QueryStatistic.result_count, result_count);
    queryStatisticMap.put(QueryStatistic.check_path_time, check_paths_time);

    switch (queryType) {
      case LAGAQ_RANGE:
        queryStatisticMap.put(QueryStatistic.spatial_time, range_query_time);
        queryStatisticMap.put(QueryStatistic.set_label_time, set_label_time);
        queryStatisticMap.put(QueryStatistic.remove_label_time, remove_label_time);
        queryStatisticMap.put(QueryStatistic.overlap_leaf_node_count, overlap_leaf_node_count);
        queryStatisticMap.put(QueryStatistic.candidate_count, candidate_count);
        break;
      case LAGAQ_JOIN:
        queryStatisticMap.put(QueryStatistic.spatial_time, join_time);
        queryStatisticMap.put(QueryStatistic.candidate_count, join_result_count);
        queryStatisticMap.put(QueryStatistic.check_overlap_time, check_overlap_time);
        break;
      case LAGAQ_KNN:
        queryStatisticMap.put(QueryStatistic.spatial_time, queue_time);
        queryStatisticMap.put(QueryStatistic.candidate_count, visit_spatial_object_count);
        break;
      default:
        throw new Exception(queryType + " does not exist!");
    }
  }
}
