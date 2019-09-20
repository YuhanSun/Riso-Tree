package experiment;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.graphdb.GraphDatabaseService;
import commons.Neo4jGraphUtility;
import commons.Query_Graph;
import commons.ReadWriteUtil;
import commons.Util;
import cypher.middleware.CypherDecoder;
import graph.RisoTreeQueryPN;

public class MaxPNSize {

  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }


  public static void maxPNSizeRisoTreeQueryMultiple(String dbPathsStr, String dataset,
      int MAX_HOPNUM, String queryPath, int queryCount, String outputPath) throws Exception {
    List<String> queries = ReadWriteUtil.readFileAllLines(queryPath);
    List<String> queriesSlice = queries.subList(0, queryCount - 1);

    String outputDetailPath = outputPath + "_details.csv";
    String outputAvgPath = outputPath + "_avg.csv";
    ReadWriteUtil.WriteFile(outputAvgPath, true, String.format("%s\t%d\n", queryPath, queryCount));

    for (String dbPath : dbPathsStr.split(",")) {
      Util.checkPathExist(dbPath);
      ReadWriteUtil.WriteFile(outputDetailPath, true, String.format("%s\n%s\n", dbPath, queryPath));
      GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(dbPath);
      RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(service, dataset, MAX_HOPNUM);
      List<ResultRecord> resultRecords = new ArrayList<>(queryCount);
      for (String query : queriesSlice) {
        ResultRecord resultRecord = risoTreeQuery(query, risoTreeQueryPN);
        resultRecords.add(resultRecord);
        ReadWriteUtil.WriteFile(outputDetailPath, true,
            String.format("%d\t%d\n", resultRecord.runTime, resultRecord.pageHit));
      }
      ReadWriteUtil.WriteFile(outputAvgPath, true, String.format("%s\t%d\t%d\n", dbPath,
          ResultRecord.getRunTimeAvg(resultRecords), ResultRecord.getPageHitAvg(resultRecords)));
      ReadWriteUtil.WriteFile(outputDetailPath, true, "\n");
    }
    ReadWriteUtil.WriteFile(outputDetailPath, true, "\n");
    ReadWriteUtil.WriteFile(outputAvgPath, true, "\n");
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
    for (String dbPath : dbPaths) {
      Util.checkPathExist(dbPath);
    }
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
    Query_Graph query_Graph = CypherDecoder.getQueryGraph(query, risoTreeQueryPN.dbservice);
    long start = System.currentTimeMillis();
    risoTreeQueryPN.queryWithIgnoreNewLabel(query, query_Graph);
    long runTime = System.currentTimeMillis() - start;
    return new ResultRecord(runTime, risoTreeQueryPN.page_hit_count);
  }

}
