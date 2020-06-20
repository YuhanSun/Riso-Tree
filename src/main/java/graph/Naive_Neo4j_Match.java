package graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import commons.Config;
import commons.Enums;
import commons.Enums.QueryStatistic;
import commons.MyRectangle;
import commons.OwnMethods;
import commons.QueryUtil;
import commons.Query_Graph;
import commons.Util;
import cypher.middleware.CypherEncoder;

/**
 * this class is graphfirst approach, spatial predicate is checked during graph search. (spatial
 * predicate is integrated into cypher query)
 * 
 * @author yuhansun
 *
 */
public class Naive_Neo4j_Match {

  public static Config config = new Config();
  public static String lon_name = config.GetLongitudePropertyName();
  public static String lat_name = config.GetLatitudePropertyName();

  // neo4j graphdb service
  public Neo4j_API neo4j_API;

  String minx_name;
  String miny_name;
  String maxx_name;
  String maxy_name;

  public long run_time, get_iterator_time, iterate_time;
  public long result_count = 0;
  public long page_access;
  public ExecutionPlanDescription planDescription;

  Map<QueryStatistic, Object> queryStatisticMap = new HashMap<>();

  public static boolean outputResult = false;

  public Naive_Neo4j_Match(GraphDatabaseService dbservice) {
    neo4j_API = new Neo4j_API(dbservice);
    Config config = new Config();
    lon_name = config.GetLongitudePropertyName();
    lat_name = config.GetLatitudePropertyName();

    String[] rect_corner_name = config.GetRectCornerName();
    minx_name = rect_corner_name[0];
    miny_name = rect_corner_name[1];
    maxx_name = rect_corner_name[2];
    maxy_name = rect_corner_name[3];
  }

  public Naive_Neo4j_Match(String db_path) {
    neo4j_API = new Neo4j_API(db_path);
    Config config = new Config();
    lon_name = config.GetLongitudePropertyName();
    lat_name = config.GetLatitudePropertyName();

    String[] rect_corner_name = config.GetRectCornerName();
    minx_name = rect_corner_name[0];
    miny_name = rect_corner_name[1];
    maxx_name = rect_corner_name[2];
    maxy_name = rect_corner_name[3];
  }

  /**
   * subgraph isomorphism with spatial predicate. It uses java api, ensure that instance is
   * constructed with db_path.
   * 
   * @param query_Graph
   * @param limit
   * @return
   */
  public Result SubgraphMatch_Spa_API(Query_Graph query_Graph, int limit)// use neo4j query
  {
    String query = FormCypherQuery(query_Graph, limit, true);
    Util.println(query);

    Result result = neo4j_API.graphDb.execute(query);
    return result;
  }

  /**
   * Solve a cypher query.
   *
   * @param query
   */
  public void query(String query) {
    iniLogVariables();
    long sumStart = System.currentTimeMillis();
    query = "profile " + query;
    long start = System.currentTimeMillis();
    Result result = neo4j_API.graphDb.execute(query);
    get_iterator_time += System.currentTimeMillis() - start;

    start = System.currentTimeMillis();
    while (result.hasNext()) {
      result_count++;
      Map<String, Object> row = result.next();
      if (outputResult) {
        Util.println(row);
      }
    }
    iterate_time += System.currentTimeMillis() - start;
    planDescription = result.getExecutionPlanDescription();
    page_access = OwnMethods.GetTotalDBHits(planDescription);
    run_time = System.currentTimeMillis() - sumStart;
    setQueryStatisticMap();
  }

  // public Result Explain_SubgraphMatch_Spa_API(Query_Graph query_Graph, int limit)//use neo4j
  // query
  // {
  // String query = FormCypherQuery(query_Graph, limit, false);
  // OwnMethods.Print(query);
  //
  // Result result = neo4j_API.graphDb.execute(query);
  // return result;
  // }

  /**
   * for the cypher query for profile or explain with given query graph
   * 
   * @param query_Graph
   * @param limit
   * @param Profile_Or_Explain set to true if profile, otherwise false
   * @return
   */
  public String FormCypherQuery(Query_Graph query_Graph, int limit, boolean Profile_Or_Explain) {
    String query = "";
    if (Profile_Or_Explain)
      query += "profile match ";
    else
      query += "explain match ";

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
          query += String.format(",(a%d)--(a%d)", i, neighbor);
      }
    }

    // spatial predicate
    int i = 0;
    for (; i < query_Graph.label_list.length; i++)
      if (query_Graph.spa_predicate[i] != null) {
        MyRectangle qRect = query_Graph.spa_predicate[i];
        query += String.format(" where %f <= a%d.%s <= %f ", qRect.min_x, i, lon_name, qRect.max_x);
        query += String.format("and %f <= a%d.%s <= %f", qRect.min_y, i, lat_name, qRect.max_y);
        i++;
        break;
      }
    for (; i < query_Graph.label_list.length; i++)
      if (query_Graph.spa_predicate[i] != null) {
        MyRectangle qRect = query_Graph.spa_predicate[i];
        query += String.format(" and %f <= a%d.%s <= %f ", qRect.min_x, i, lon_name, qRect.max_x);
        query += String.format("and %f <= a%d.%s <= %f", qRect.min_y, i, lat_name, qRect.max_y);
      }

    // return
    query += " return id(a0)";
    for (i = 1; i < query_Graph.graph.size(); i++)
      query += String.format(",id(a%d)", i);

    if (limit != -1)
      query += String.format(" limit %d", limit);

    return query;
  }

  public String formQueryJoin(Query_Graph query_Graph, ArrayList<Integer> pos, double distance,
      Enums.Explain_Or_Profile explain_Or_Profile) {
    String query = CypherEncoder.getMatchPrefix(explain_Or_Profile);
    query += " " + CypherEncoder.getMatchGraphSkeletonString(query_Graph);

    query += " where ";
    // spatial predicate
    query += String.format(
        "(a%1$d.%3$s - a%2$d.%3$s)*(a%1$d.%3$s - a%2$d.%3$s) + "
            + "(a%1$d.%4$s - a%2$d.%4$s)*(a%1$d.%4$s - a%2$d.%4$s) <= %5$s",
        pos.get(0), pos.get(1), lon_name, lat_name, String.valueOf(distance * distance));

    // return
    query += String.format(" return distinct id(a%d), id(a%d)", pos.get(0), pos.get(1));

    return query;
  }

  public List<long[]> LAGAQ_Join(Query_Graph query_Graph, double distance) throws Exception {
    iniLogVariables();
    long sumStart = System.currentTimeMillis();
    List<long[]> res = new LinkedList<>();
    ArrayList<Integer> pos = new ArrayList<>();
    for (int i = 0; i < query_Graph.Has_Spa_Predicate.length; i++) {
      if (query_Graph.Has_Spa_Predicate[i]) {
        pos.add(i);
      }
    }
    if (pos.size() != 2) {
      throw new Exception(String.format("query graph has %d spatial predicate!", pos.size()));
    }
    String query = formQueryJoin(query_Graph, pos, distance, Enums.Explain_Or_Profile.Profile);
    Util.println(query);
    Transaction tx = neo4j_API.graphDb.beginTx();
    long start = System.currentTimeMillis();
    Result result = neo4j_API.graphDb.execute(query);
    get_iterator_time += System.currentTimeMillis() - start;
    start = System.currentTimeMillis();
    while (result.hasNext()) {
      Map<String, Object> row = result.next();
      // OwnMethods.Print(row);
      long id1 = (long) row.get(String.format("id(a%d)", pos.get(0)));
      long id2 = (long) row.get(String.format("id(a%d)", pos.get(1)));
      long[] pair = new long[] {id1, id2};
      res.add(pair);
      result_count++;
    }
    iterate_time += System.currentTimeMillis() - start;

    planDescription = result.getExecutionPlanDescription();
    page_access = OwnMethods.GetTotalDBHits(planDescription);

    tx.success();
    tx.close();
    run_time = System.currentTimeMillis() - sumStart;
    setQueryStatisticMap();
    return res;
  }

  public static String formQueryKNN(Query_Graph query_Graph,
      Enums.Explain_Or_Profile explain_Or_Profile, int K) throws Exception {
    String query = CypherEncoder.getMatchPrefix(explain_Or_Profile);
    query += " " + CypherEncoder.getMatchGraphSkeletonString(query_Graph);
    query += " return " + CypherEncoder.getMatchReturnString(query_Graph);

    Map<Integer, MyRectangle> spatialPredicates = query_Graph.getSpatialPredicates();
    if (spatialPredicates.size() != 1) {
      throw new Exception(
          String.format("Query graph should have exactly ONE spatial predicate, rather than %d",
              spatialPredicates.size()));
    }
    int spatialPredicateIndex = spatialPredicates.keySet().iterator().next();
    MyRectangle queryRectangle = spatialPredicates.get(spatialPredicateIndex);
    query += String.format(" order by (a%d.%s - %f)*(a%d.%s - %f)", spatialPredicateIndex, lon_name,
        queryRectangle.min_x, spatialPredicateIndex, lon_name, queryRectangle.min_x);
    query += String.format(" + (a%d.%s - %f)*(a%d.%s - %f)", spatialPredicateIndex, lat_name,
        queryRectangle.min_y, spatialPredicateIndex, lat_name, queryRectangle.min_y);

    query += " limit " + K;

    return query;
  }

  public List<long[]> LAGAQ_KNN(Query_Graph query_Graph, int K) throws Exception {
    long sumStart = System.currentTimeMillis();
    iniLogVariables();
    List<long[]> res = new ArrayList<>();
    String query = formQueryKNN(query_Graph, Enums.Explain_Or_Profile.Profile, K);
    Util.println(query);
    String[] columnNames = CypherEncoder.getReturnColumnNames(query_Graph);
    Transaction tx = neo4j_API.graphDb.beginTx();
    long start = System.currentTimeMillis();
    Result result = neo4j_API.graphDb.execute(query);
    get_iterator_time += System.currentTimeMillis() - start;
    start = System.currentTimeMillis();
    while (result.hasNext()) {
      Map<String, Object> row = result.next();
      long[] ids = QueryUtil.getResultRowInArray(columnNames, row);
      res.add(ids);
      result_count++;
    }
    iterate_time += System.currentTimeMillis() - start;

    planDescription = result.getExecutionPlanDescription();
    page_access = OwnMethods.GetTotalDBHits(planDescription);

    tx.success();
    tx.close();
    run_time += System.currentTimeMillis() - sumStart;
    setQueryStatisticMap();
    return res;
  }

  private void iniLogVariables() {
    run_time = 0;
    get_iterator_time = 0;
    iterate_time = 0;
    result_count = 0;
    page_access = 0;
    queryStatisticMap = new HashMap<>();
  }

  public Map<QueryStatistic, Object> getQueryStatisticMap() {
    return queryStatisticMap;
  }

  private void setQueryStatisticMap() {
    queryStatisticMap.put(QueryStatistic.run_time, run_time);
    queryStatisticMap.put(QueryStatistic.page_hit_count, page_access);
    queryStatisticMap.put(QueryStatistic.get_iterator_time, get_iterator_time);
    queryStatisticMap.put(QueryStatistic.iterate_time, iterate_time);
    queryStatisticMap.put(QueryStatistic.result_count, result_count);
  }
}
