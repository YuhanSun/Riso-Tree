package experiment;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import commons.Config.ClearCacheMethod;
import commons.Config.ExperimentMethod;
import commons.Neo4jGraphUtility;
import commons.OwnMethods;
import commons.ReadWriteUtil;
import commons.Util;
import graph.Naive_Neo4j_Match;
import graph.RisoTreeQueryPN;
import graph.SpatialFirst_List;

public class ExperimentUtil {
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
  private static List<ResultRecord> runExperiment(String dbPath, String dataset,
      ExperimentMethod method, int MAX_HOP, String queryPath, int queryCount, String password,
      boolean clearCache, ClearCacheMethod clearCacheMethod, String outputPath) throws Exception {
    List<String> queries = ReadWriteUtil.readFileAllLines(queryPath);
    queries = queries.subList(0, queryCount);
    int queryId = -1;
    List<ResultRecord> records = new ArrayList<>();
    for (String query : queries) {
      queryId++;
      Util.println(String.format("query id: %d:", queryId));
      Util.println(query);

      GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(dbPath);
      ResultRecord record = runExperiment(service, dataset, method, query, MAX_HOP);
      records.add(record);

      service.shutdown();
      if (clearCache) {
        OwnMethods.clearCache(password, clearCacheMethod);
      }
      service = Neo4jGraphUtility.getDatabaseService(dbPath);
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
   * @return
   * @throws Exception
   */
  private static ResultRecord runExperiment(GraphDatabaseService service, String dataset,
      ExperimentMethod method, String query, int MAX_HOP) throws Exception {
    ResultRecord record = null;
    ExecutionPlanDescription planDescription = null;
    switch (method) {
      case NAIVE:
        Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match(service);
        naive_Neo4j_Match.query(query);
        record = new ResultRecord(naive_Neo4j_Match.run_time, naive_Neo4j_Match.page_access,
            naive_Neo4j_Match.get_iterator_time, naive_Neo4j_Match.iterate_time,
            naive_Neo4j_Match.result_count);
        planDescription = naive_Neo4j_Match.planDescription;
        break;
      case SPATIAL_FIRST:
        SpatialFirst_List spatialFirst_List = new SpatialFirst_List(service, dataset);
        spatialFirst_List.query_Block(query);
        record = new ResultRecord(spatialFirst_List.run_time, spatialFirst_List.page_hit_count,
            spatialFirst_List.get_iterator_time, spatialFirst_List.iterate_time,
            spatialFirst_List.result_count);
        planDescription = spatialFirst_List.planDescription;
        break;
      case RISOTREE:
        RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(service, dataset, MAX_HOP);
        risoTreeQueryPN.queryWithIgnore(query);
        record = new ResultRecord(risoTreeQueryPN.run_time, risoTreeQueryPN.page_hit_count,
            risoTreeQueryPN.get_iterator_time, risoTreeQueryPN.iterate_time,
            risoTreeQueryPN.set_label_time, risoTreeQueryPN.remove_label_time,
            risoTreeQueryPN.result_count);
        planDescription = risoTreeQueryPN.planDescription;
        break;
      default:
        throw new RuntimeException(String.format("method %s does not exist!", method));
    }
    Util.println(record);
    Util.println(planDescription);
    return record;
  }

  private static String getAverageResultOutput(String outputPath, List<ResultRecord> records,
      ExperimentMethod method) {
    String string = "";
    switch (method) {
      case NAIVE:
        string += ResultRecord.getRunTimeAvg(records) + "\t";
        string += ResultRecord.getPageHitAvg(records) + "\t";
        string += ResultRecord.getGetIteratorTimeAvg(records) + "\t";
        string += ResultRecord.getIterateTimeAvg(records) + "\t";
        string += ResultRecord.getResultCountAvg(records);
        break;
      case SPATIAL_FIRST:
        string += ResultRecord.getRunTimeAvg(records) + "\t";
        string += ResultRecord.getPageHitAvg(records) + "\t";
        string += ResultRecord.getGetIteratorTimeAvg(records) + "\t";
        string += ResultRecord.getIterateTimeAvg(records) + "\t";
        string += ResultRecord.getResultCountAvg(records);
        break;
      case RISOTREE:
        string += ResultRecord.getRunTimeAvg(records) + "\t";
        string += ResultRecord.getPageHitAvg(records) + "\t";
        string += ResultRecord.getRangeQueryTimeAvg(records) + "\t";
        string += ResultRecord.getSetLabelTimeAvg(records) + "\t";
        string += ResultRecord.getSetLabelTimeAvg(records) + "\t";
        string += ResultRecord.getGetIteratorTimeAvg(records) + "\t";
        string += ResultRecord.getIterateTimeAvg(records) + "\t";
        string += ResultRecord.getOverLapLeafCountAvg(records) + "\t";
        string += ResultRecord.getResultCountAvg(records);
        break;
      default:
        throw new RuntimeException(String.format("method %s does not exist!", method));
    }
    return string;
  }

  public static String getHeader(ExperimentMethod method) {
    switch (method) {
      case NAIVE:
        return StringUtils.joinWith("\t", "runTime", "pageHit", "getIteratorTime", "iterateTime",
            "resultCount");
      case SPATIAL_FIRST:
        return StringUtils.joinWith("\t", "runTime", "pageHit", "getIteratorTime", "iterateTime",
            "resultCount");
      case RISOTREE:
        return StringUtils.joinWith("\t", "runTime", "pageHit", "rangeQueryTime", "setLabelTime",
            "removeLabelTime", "getIteratorTime", "iterateTime", "overlapLeafCount", "resultCount");
      default:
        throw new RuntimeException(String.format("method %s does not exist!", method));
    }
  }

  public static void runExperimentQueryPathList(String dbPath, String dataset,
      ExperimentMethod method, int MAX_HOP, String queryPaths, int queryCount, String password,
      boolean clearCache, ClearCacheMethod clearCacheMethod, String outputPath) throws Exception {
    Util.checkPathExist(dbPath);
    String header = getHeader(method);
    ReadWriteUtil.WriteFile(outputPath, true, "queryPath\t" + header + "\n");
    String[] queryPathList = queryPaths.split(",");
    for (String queryPath : queryPathList) {
      List<ResultRecord> records = runExperiment(dbPath, dataset, method, MAX_HOP, queryPath,
          queryCount, password, clearCache, clearCacheMethod, outputPath);
      String string = getAverageResultOutput(outputPath, records, method);
      ReadWriteUtil.WriteFile(outputPath, true, StringUtils.joinWith("\t", queryPath, string));
    }
  }
}
