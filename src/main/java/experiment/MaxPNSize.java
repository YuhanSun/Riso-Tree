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
