package experiment.Join;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import commons.Config;
import commons.Enums;
import commons.Enums.ExperimentMethod;
import commons.Enums.QueryType;
import commons.Neo4jGraphUtility;
import commons.OwnMethods;
import commons.Query_Graph;
import commons.Query_Graph.LabelType;
import commons.ReadWriteUtil;
import commons.Util;
import cypher.middleware.CypherDecoder;
import experiment.ResultRecord;
import graph.Naive_Neo4j_Match;
import graph.RisoTreeQueryPN;
import graph.SpatialFirst_List;

public class JoinExperimentUtil {
  /**
   * Convert the one-predicate query graph to two-predicate Randomly pick up the next true spatial
   * vertex. The query graph is for LAGAQ-join input. This function is not used for experiment
   * because it may lead to different runs with different query set.
   *
   * @param service
   * @param query_Graph
   * @return {@code false} means {@code query_Graph} cannot be converted to a query graph for
   *         LAGAQ-join and {@code true} means it can.
   * @throws Exception
   */
  public static boolean convertQueryGraphForJoinRandom(GraphDatabaseService service,
      Query_Graph query_Graph) throws Exception {
    if (!query_Graph.labelType.equals(LabelType.STRING)) {
      throw new Exception(
          query_Graph.labelType + " is not supported by convertQueryGraphForJoinRandom");
    }
    int nodeCount = query_Graph.graph.size();

    int spaCount = 0;
    for (int i = 0; i < nodeCount; i++) {
      Label label = Label.label(query_Graph.label_list_string[i]);
      if (Neo4jGraphUtility.isLabelSpatial(service, label)) {
        spaCount++;
      }
    }

    if (spaCount < 2) {
      return false;
    }

    Random random = new Random();
    while (true) {
      int id = (int) (nodeCount * random.nextDouble());
      if (query_Graph.Has_Spa_Predicate[id])
        continue;

      Label label = Label.label(query_Graph.label_list_string[id]);
      if (Neo4jGraphUtility.isLabelSpatial(service, label)) {
        query_Graph.Has_Spa_Predicate[id] = true;
        return true;
      }
    }
  }

  /**
   * Convert the one-predicate query graph to two-predicate. Always pick up the second true spatial
   * vertex. The query graph is for LAGAQ-join input.
   *
   * @param service
   * @param query_Graph
   * @return {@code false} means {@code query_Graph} cannot be converted to a query graph for
   *         LAGAQ-join and {@code true} means it can.
   * @throws Exception
   */
  public static boolean convertQueryGraphForJoinFixed(GraphDatabaseService service,
      Query_Graph query_Graph) throws Exception {
    if (!query_Graph.labelType.equals(LabelType.STRING)) {
      throw new Exception(
          query_Graph.labelType + " is not supported by convertQueryGraphForJoinRandom");
    }
    int nodeCount = query_Graph.graph.size();

    int spaCount = 0;
    for (int i = 0; i < nodeCount; i++) {
      Label label = Label.label(query_Graph.label_list_string[i]);
      if (Neo4jGraphUtility.isLabelSpatial(service, label)) {
        spaCount++;
      }
    }

    if (spaCount < 2) {
      return false;
    }


    for (int id = 0; id < nodeCount; id++) {
      if (query_Graph.Has_Spa_Predicate[id]) {
        continue;
      }

      Label label = Label.label(query_Graph.label_list_string[id]);
      if (Neo4jGraphUtility.isLabelSpatial(service, label)) {
        query_Graph.Has_Spa_Predicate[id] = true;
        return true;
      }
    }
    return false;
  }

  /**
   * Run experiment for a set of queries. Print the query id and each query.
   *
   * @param dbPath
   * @param dataset
   * @param method
   * @param MAX_HOP
   * @param queryPath
   * @param queryCount
   * @param password
   * @param clearCache
   * @param clearCacheMethod
   * @param outputPath
   * @throws Exception
   */
  public static List<ResultRecord> runExperiment(String dbPath, String dataset,
      Enums.ExperimentMethod method, int MAX_HOP, String queryPath, double joinDistance,
      int queryCount, String password, boolean clearCache, Enums.ClearCacheMethod clearCacheMethod,
      String outputPath) throws Exception {
    List<String> queries = ReadWriteUtil.readFileAllLines(queryPath, Config.SKIPFLAG);
    GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(dbPath);
    List<Query_Graph> oriQueryGraphs = new ArrayList<>(queries.size());
    for (String query : queries) {
      oriQueryGraphs.add(CypherDecoder.getQueryGraph(query, service));
    }
    List<Query_Graph> queryGraphs = new ArrayList<>(queries.size());
    Transaction tx = service.beginTx();
    for (Query_Graph query_Graph : oriQueryGraphs) {
      if (JoinExperimentUtil.convertQueryGraphForJoinFixed(service, query_Graph)) {
        queryGraphs.add(query_Graph);
      }
    }
    tx.success();
    tx.close();
    service.shutdown();

    if (queryGraphs.size() < queryCount) {
      throw new Exception(
          String.format("Only %d query graphs can be used for LAGAQ-join but %d are required!",
              queryGraphs.size(), queryCount));
    }
    queryGraphs = queryGraphs.subList(0, queryCount);

    int queryId = -1;
    List<ResultRecord> records = new ArrayList<>(queryCount);
    for (Query_Graph query : queryGraphs) {
      queryId++;
      Util.println(String.format("query id: %d:", queryId));
      Util.println(query);

      service = Neo4jGraphUtility.getDatabaseService(dbPath);
      ResultRecord record = runExperiment(service, dataset, method, query, MAX_HOP, joinDistance);
      records.add(record);

      service.shutdown();
      if (clearCache) {
        OwnMethods.clearCache(password, clearCacheMethod);
      }
    }
    return records;
  }

  /**
   * Run a given query for a method. Print the {@code record} and {@code planDescription}.
   *
   * @param service
   * @param dataset
   * @param method
   * @param query
   * @param MAX_HOP
   * @param joinDistance
   * @return
   * @throws Exception
   */
  private static ResultRecord runExperiment(GraphDatabaseService service, String dataset,
      Enums.ExperimentMethod method, Query_Graph query, int MAX_HOP, double joinDistance)
      throws Exception {
    ResultRecord record = null;
    ExecutionPlanDescription planDescription = null;
    switch (method) {
      case NAIVE:
        Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match(service);
        naive_Neo4j_Match.LAGAQ_Join(query, joinDistance);
        planDescription = naive_Neo4j_Match.planDescription;
        record = new ResultRecord(QueryType.LAGAQ_JOIN, ExperimentMethod.NAIVE,
            naive_Neo4j_Match.getQueryStatisticMap(), naive_Neo4j_Match.planDescription);
        break;
      case SPATIAL_FIRST:
        SpatialFirst_List spatialFirst_List = new SpatialFirst_List(service, dataset);
        spatialFirst_List.LAGAQ_Join(query, joinDistance);
        planDescription = spatialFirst_List.planDescription;
        record = new ResultRecord(QueryType.LAGAQ_JOIN, ExperimentMethod.SPATIAL_FIRST,
            spatialFirst_List.getQueryStatisticMap(), spatialFirst_List.planDescription);
        break;
      case RISOTREE:
        RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(service, dataset, MAX_HOP);
        risoTreeQueryPN.LAGAQ_Join(query, joinDistance);
        planDescription = risoTreeQueryPN.planDescription;
        record = new ResultRecord(QueryType.LAGAQ_JOIN, ExperimentMethod.RISOTREE,
            risoTreeQueryPN.getQueryStatisticMap(), risoTreeQueryPN.planDescription);
        break;
      default:
        throw new RuntimeException(String.format("method %s does not exist!", method));
    }
    Util.println(record);
    Util.println(planDescription);
    return record;
  }
}
