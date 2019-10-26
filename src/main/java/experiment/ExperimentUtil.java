package experiment;

import java.io.FileWriter;
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
  public static List<ResultRecord> runExperiment(String dbPath, String dataset,
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
      // service = Neo4jGraphUtility.getDatabaseService(dbPath);
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
        planDescription = naive_Neo4j_Match.planDescription;
        record = new ResultRecord(naive_Neo4j_Match);
        break;
      case SPATIAL_FIRST:
        SpatialFirst_List spatialFirst_List = new SpatialFirst_List(service, dataset);
        spatialFirst_List.query_Block(query);
        planDescription = spatialFirst_List.planDescription;
        record = new ResultRecord(spatialFirst_List);
        break;
      case RISOTREE:
        RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(service, dataset, MAX_HOP);
        risoTreeQueryPN.queryWithIgnore(query);
        planDescription = risoTreeQueryPN.planDescription;
        record = new ResultRecord(risoTreeQueryPN);
        break;
      default:
        throw new RuntimeException(String.format("method %s does not exist!", method));
    }
    Util.println(record);
    Util.println(planDescription);
    return record;
  }

  public static String getOutputResult(ResultRecord record, ExperimentMethod method) {
    String string = "";
    switch (method) {
      case NAIVE:
        string += record.runTime + "\t";
        string += record.pageHit + "\t";
        string += record.get_iterator_time + "\t";
        string += record.iterate_time + "\t";
        string += record.result_count;
        break;
      case SPATIAL_FIRST:
        string += record.runTime + "\t";
        string += record.pageHit + "\t";
        string += record.range_query_time + "\t";
        string += record.get_iterator_time + "\t";
        string += record.iterate_time + "\t";
        string += record.overlap_leaf_node_count;
        string += record.candidate_count;
        string += record.result_count;
        break;
      case RISOTREE:
        string += record.runTime + "\t";
        string += record.pageHit + "\t";
        string += record.range_query_time + "\t";
        string += record.set_label_time + "\t";
        string += record.remove_label_time + "\t";
        string += record.get_iterator_time + "\t";
        string += record.iterate_time + "\t";
        string += record.overlap_leaf_node_count;
        string += record.candidate_count;
        string += record.result_count;
        break;
      default:
        throw new RuntimeException(String.format("method %s does not exist!", method));
    }
    return string;
  }

  public static void outputDetailResult(List<ResultRecord> records, ExperimentMethod method,
      String outputPath) throws Exception {
    FileWriter writer = Util.getFileWriter(outputPath, true);
    for (int i = 0; i < records.size(); i++) {
      ResultRecord record = records.get(i);
      writer.write(String.format("%d\t%s\n", i, getOutputResult(record, method)));
    }
    for (int i = 0; i < records.size(); i++) {
      ResultRecord record = records.get(i);
      writer.write(i + "\n");
      writer.write(record.toString() + "\n");
      writer.write(String.format("%s", record.planDescription));
    }
    writer.write("\n");
    writer.close();
  }

  public static String getAverageResultOutput(List<ResultRecord> records, ExperimentMethod method) {
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
        string += ResultRecord.getRangeQueryTimeAvg(records) + "\t";
        string += ResultRecord.getGetIteratorTimeAvg(records) + "\t";
        string += ResultRecord.getIterateTimeAvg(records) + "\t";
        string += ResultRecord.getOverLapLeafCountAvg(records) + "\t";
        string += ResultRecord.getCandidateCountAvg(records) + "\t";
        string += ResultRecord.getResultCountAvg(records);
        break;
      case RISOTREE:
        string += ResultRecord.getRunTimeAvg(records) + "\t";
        string += ResultRecord.getPageHitAvg(records) + "\t";
        string += ResultRecord.getRangeQueryTimeAvg(records) + "\t";
        string += ResultRecord.getSetLabelTimeAvg(records) + "\t";
        string += ResultRecord.getRemoveLabelTimeAvg(records) + "\t";
        string += ResultRecord.getGetIteratorTimeAvg(records) + "\t";
        string += ResultRecord.getIterateTimeAvg(records) + "\t";
        string += ResultRecord.getOverLapLeafCountAvg(records) + "\t";
        string += ResultRecord.getCandidateCountAvg(records) + "\t";
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
        return StringUtils.joinWith("\t", "runTime", "pageHit", "rangeQueryTime", "getIteratorTime",
            "iterateTime", "overlapLeafCount", "candidateCount", "resultCount");
      case RISOTREE:
        return StringUtils.joinWith("\t", "runTime", "pageHit", "rangeQueryTime", "setLabelTime",
            "removeLabelTime", "getIteratorTime", "iterateTime", "overlapLeafCount",
            "candidateCount", "resultCount");
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
      String string = getAverageResultOutput(records, method);
      ReadWriteUtil.WriteFile(outputPath, true,
          StringUtils.joinWith("\t", queryPath, string) + "\n");
    }
  }
}
