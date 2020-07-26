package experiment;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import commons.Enums.ClearCacheMethod;
import commons.Enums.ExperimentMethod;
import commons.Enums.QueryStatistic;
import commons.Enums.QueryType;
import commons.Neo4jGraphUtility;
import commons.ReadWriteUtil;
import commons.Util;
import graph.RisoTreeQueryPN;

public class MaxPNSize {

  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }


  public static void maxPNSizeRisoTreeQueryMultiple(String dbPathsStr, String dataset,
      int MAX_HOPNUM, String queryPath, int queryCount, String password, boolean clearCache,
      ClearCacheMethod clearCacheMethod, String outputDir) throws Exception {
    String[] dbPaths = dbPathsStr.split(",");
    Util.checkPathExist(dbPaths);

    String detailPath = getDetailOutputPath(outputDir, dataset, ExperimentMethod.RISOTREE);
    String avgPath = getAvgOutputPath(outputDir, dataset, ExperimentMethod.RISOTREE);

    ReadWriteUtil.WriteFile(avgPath, true,
        getRunningArgs(queryPath, MAX_HOPNUM, queryCount, clearCache, clearCacheMethod) + "\n");
    List<QueryStatistic> queryStatistics =
        ExperimentUtil.getQueryStatistics(QueryType.LAGAQ_RANGE, ExperimentMethod.RISOTREE);
    List<String> queryStatisticStrings = ExperimentUtil.getQueryStatisticsStrings(queryStatistics);
    String header = String.join("\t", queryStatisticStrings);
    ReadWriteUtil.WriteFile(avgPath, true, "dbPath\t" + header + "\n");

    ReadWriteUtil.WriteFile(detailPath, true, queryPath + "\n\n");

    for (String dbPath : dbPaths) {
      ReadWriteUtil.WriteFile(detailPath, true, dbPath + "\n");
      ReadWriteUtil.WriteFile(detailPath, true, "id\t" + header + "\n");
      List<ResultRecord> records =
          ExperimentUtil.runExperiment(dbPath, dataset, ExperimentMethod.RISOTREE, MAX_HOPNUM,
              queryPath, queryCount, password, clearCache, clearCacheMethod);
      ExperimentUtil.outputDetailResult(records, queryStatistics, detailPath);

      String string = ExperimentUtil.getAverageResultOutput(records, queryStatistics);
      ReadWriteUtil.WriteFile(avgPath, true, StringUtils.joinWith("\t", dbPath, string) + "\n");
    }
    ReadWriteUtil.WriteFile(detailPath, true, "\n");
    ReadWriteUtil.WriteFile(avgPath, true, "\n");
  }

  public static String getAvgOutputPath(String outputDir, String dataset, ExperimentMethod method) {
    return String.format("%s/%s_%s_avg.tsv", outputDir, dataset, method.toString());
  }

  public static String getDetailOutputPath(String outputDir, String dataset,
      ExperimentMethod method) {
    return String.format("%s/%s_%s_detail.txt", outputDir, dataset, method.toString());
  }

  public static String getRunningArgs(String queryPath, int MAX_HOP, int queryCount,
      boolean clearCache, ClearCacheMethod clearCacheMethod) {
    return String.format(
        "queryPath:%s, MAX_HOP:%d, queryCount:%d, clearCache:%s, ClearCacheMethod:%s", queryPath,
        MAX_HOP, queryCount, clearCache, clearCacheMethod);
  }

  /**
   * Run the same query in different dbs.
   *
   * @param dbPathsStr
   * @param dataset
   * @param MAX_HOPNUM
   * @param queryPath
   * @param queryId
   * @param outputPath
   * @throws Exception
   */
  public static void maxPNSizeRisoTreeQuery(String dbPathsStr, String dataset, int MAX_HOPNUM,
      String queryPath, int queryId, String outputPath) throws Exception {
    List<String> queries = ReadWriteUtil.readFileAllLines(queryPath);
    String query = queries.get(queryId);
    String[] dbPaths = dbPathsStr.split(",");
    Util.checkPathExist(dbPaths);
    List<ResultRecord> resultRecords = risoTreeQuery(dbPaths, dataset, MAX_HOPNUM, query);
    if (resultRecords.size() != dbPaths.length) {
      throw new RuntimeException("result records and dbpaths count mismatch!");
    }
    ReadWriteUtil.WriteFile(outputPath, true, String.format("%s\t%d\n", queryPath, queryId));
    for (int i = 0; i < dbPaths.length; i++) {
      ResultRecord resultRecord = resultRecords.get(i);
      ReadWriteUtil.WriteFile(outputPath, true,
          String.format("%s\t%d\t%d\n", dbPaths[i], resultRecord.runTime, resultRecord.pageHit));
    }
    ReadWriteUtil.WriteFile(outputPath, true, "\n");
  }

  public static List<ResultRecord> risoTreeQuery(String[] dbPaths, String dataset, int MAX_HOPNUM,
      String query) throws Exception {
    List<ResultRecord> resultRecords = new ArrayList<>();
    for (String dbPath : dbPaths) {
      GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(dbPath);
      RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(service, dataset, MAX_HOPNUM);
      resultRecords.add(risoTreeQuery(query, risoTreeQueryPN));
      Util.close(risoTreeQueryPN.dbservice);
    }

    return resultRecords;
  }



  public static ResultRecord risoTreeQuery(String query, RisoTreeQueryPN risoTreeQueryPN)
      throws Exception {
    risoTreeQueryPN.queryWithIgnore(query);
    // Query_Graph query_Graph = CypherDecoder.getQueryGraph(query, risoTreeQueryPN.dbservice);
    // long start = System.currentTimeMillis();
    // risoTreeQueryPN.queryWithIgnoreNewLabel(query, query_Graph);
    return new ResultRecord(QueryType.LAGAQ_RANGE, ExperimentMethod.RISOTREE,
        risoTreeQueryPN.getQueryStatisticMap(), risoTreeQueryPN.planDescription);
  }

}
