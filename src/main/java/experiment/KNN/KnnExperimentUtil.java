package experiment.KNN;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import commons.Enums;
import commons.Enums.ExperimentMethod;
import commons.Enums.QueryType;
import commons.Neo4jGraphUtility;
import commons.OwnMethods;
import commons.Query_Graph;
import commons.ReadWriteUtil;
import commons.Util;
import cypher.middleware.CypherDecoder;
import experiment.ResultRecord;
import graph.Naive_Neo4j_Match;
import graph.RisoTreeQueryPN;
import graph.SpatialFirst_List;

public class KnnExperimentUtil {
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
      Enums.ExperimentMethod method, int MAX_HOP, String queryPath, int KValue, int queryCount,
      String password, boolean clearCache, Enums.ClearCacheMethod clearCacheMethod,
      String outputPath) throws Exception {
    List<String> queries = ReadWriteUtil.readFileAllLines(queryPath);
    queries = queries.subList(0, queryCount);
    GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(dbPath);
    List<Query_Graph> queryGraphs = new ArrayList<>(queries.size());
    for (String query : queries) {
      queryGraphs.add(CypherDecoder.getQueryGraph(query, service));
    }
    service.shutdown();

    int queryId = -1;
    List<ResultRecord> records = new ArrayList<>(queryCount);
    for (Query_Graph query : queryGraphs) {
      queryId++;
      Util.println(String.format("query id: %d:", queryId));
      Util.println(query);

      service = Neo4jGraphUtility.getDatabaseService(dbPath);
      ResultRecord record = runExperiment(service, dataset, method, query, MAX_HOP, KValue);
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
   * @param k
   * @return
   * @throws Exception
   */
  private static ResultRecord runExperiment(GraphDatabaseService service, String dataset,
      Enums.ExperimentMethod method, Query_Graph query, int MAX_HOP, int k) throws Exception {
    ResultRecord record = null;
    ExecutionPlanDescription planDescription = null;
    switch (method) {
      case NAIVE:
        Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match(service);
        naive_Neo4j_Match.LAGAQ_KNN(query, k);
        planDescription = naive_Neo4j_Match.planDescription;
        record = new ResultRecord(QueryType.LAGAQ_KNN, ExperimentMethod.NAIVE,
            naive_Neo4j_Match.getQueryStatisticMap(), naive_Neo4j_Match.planDescription);
        break;
      case SPATIAL_FIRST:
        SpatialFirst_List spatialFirst_List = new SpatialFirst_List(service, dataset);
        spatialFirst_List.LAGAQ_KNN(query, k);
        planDescription = spatialFirst_List.planDescription;
        record = new ResultRecord(QueryType.LAGAQ_KNN, ExperimentMethod.SPATIAL_FIRST,
            spatialFirst_List.getQueryStatisticMap(), spatialFirst_List.planDescription);
        break;
      case RISOTREE:
        RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(service, dataset, MAX_HOP);
        risoTreeQueryPN.LAGAQ_KNN(query, k);
        planDescription = risoTreeQueryPN.planDescription;
        record = new ResultRecord(QueryType.LAGAQ_KNN, ExperimentMethod.RISOTREE,
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
