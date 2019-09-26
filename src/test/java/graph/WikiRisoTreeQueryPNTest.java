package graph;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import commons.Config.ClearCacheMethod;
import commons.Neo4jGraphUtility;
import commons.OwnMethods;
import commons.ReadWriteUtil;
import commons.RunTimeConfigure;
import commons.Util;
import experiment.ResultRecord;

public class WikiRisoTreeQueryPNTest {

  private static final Logger logger = Logger.getLogger(WikiRisoTreeQueryPNTest.class.getName());
  static final String dataset = RunTimeConfigure.dataset;
  static final String suffix = RunTimeConfigure.suffix;
  static final String dbPath = RunTimeConfigure.dbPath;

  GraphDatabaseService service = null;

  static final String queryDir = RunTimeConfigure.queryDir;
  static final String queryPath = RunTimeConfigure.queryPath;
  // String queryPath = queryDir + "\\4_0.0001";
  static final int queryCount = RunTimeConfigure.queryCount;

  static final boolean clearCache = RunTimeConfigure.clearCache;
  static final ClearCacheMethod method = RunTimeConfigure.clearMethod;
  static final String password = RunTimeConfigure.password;

  @Before
  public void setUp() throws Exception {
    service = Neo4jGraphUtility.getDatabaseService(dbPath);
  }

  @After
  public void tearDown() throws Exception {
    Util.close(service);
  }

  @Test
  public void validateRemoval() {
    Transaction tx = service.beginTx();
    ResourceIterator<Node> iterator = service.findNodes(Label.label("a0"));
    while (iterator.hasNext()) {
      Util.println(iterator.next().getId());
    }

    iterator = service.findNodes(Label.label("a1"));
    while (iterator.hasNext()) {
      Util.println(iterator.next().getId());
    }
    tx.success();
    tx.close();
  }

  @Test
  public void queryWithIgnoreTest() throws Exception {
    // String query =
    // "MATCH (a:`big city`)-[b:`located in the administrative territorial
    // entity`]-(spatialnode:university) "
    // + "WHERE 17.684805514724214 <= spatialnode.lat <= 33.97125439168383 AND 75.2649384026851 <=
    // spatialnode.lon <= 98.33687275901212 "
    // + "RETURN *";

    // String query =
    // "match (a0:`facility`),(a1:`communist state`),(a2:`village-level division in
    // China`),(a3:`town in China`),(a4:`village-level division in
    // China`),(a0)--(a1),(a1)--(a2),(a2)--(a3),(a3)--(a4) where 110.250591 <= a0.lon <= 110.387789
    // and 25.202411 <= a0.lat <= 25.339609 return id(a0),id(a1),id(a2),id(a3),id(a4)";

    List<String> queries = ReadWriteUtil.readFileAllLines(queryPath);
    String query = queries.get(1);
    logger.info(query);

    // Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match(service);
    // long start = System.currentTimeMillis();
    // naive_Neo4j_Match.queryWithIgnore(query);
    // Util.println("time: " + (System.currentTimeMillis() - start));
    // Util.println("result count: " + naive_Neo4j_Match.result_count);
    // Util.println("page hit: " + naive_Neo4j_Match.page_access);
    // Util.println(naive_Neo4j_Match.planDescription);
    //
    // service.shutdown();
    // service = Neo4jGraphUtility.getDatabaseService(dbPath);

    RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(service, dataset, 1);
    long start2 = System.currentTimeMillis();
    risoTreeQueryPN.queryWithIgnore(query);
    Util.println("time: " + (System.currentTimeMillis() - start2));
    Util.println("range query time: " + risoTreeQueryPN.range_query_time);
    Util.println("check paths time: " + risoTreeQueryPN.check_paths_time);
    Util.println("result count: " + risoTreeQueryPN.result_count);
    Util.println("page hit: " + risoTreeQueryPN.page_hit_count);
    Util.println(risoTreeQueryPN.planDescription);

    // if (naive_Neo4j_Match.result_count != risoTreeQueryPN.result_count) {
    // throw new RuntimeException("result count mismatch!");
    // }
  }

  /**
   * For each query test the two methods.
   * 
   * @throws Exception
   */
  @Test
  public void queryWithIgnoreBulkRowTest() throws Exception {
    List<String> naiveBetter = new ArrayList<>();
    List<String> risoTreeBetter = new ArrayList<>();
    List<Integer> naiveBetterIds = new ArrayList<>();
    List<Integer> risoTreeBetterIds = new ArrayList<>();

    List<ResultRecord> naiveResults = new ArrayList<>();
    List<ResultRecord> risoResults = new ArrayList<>();


    List<String> queries = ReadWriteUtil.readFileAllLines(queryPath);
    queries = queries.subList(0, queryCount);
    int queryId = -1;
    for (String query : queries) {
      queryId++;
      Util.println("query id: " + queryId);
      Util.println(query);
      RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(service, dataset, 2);
      long start2 = System.currentTimeMillis();
      risoTreeQueryPN.queryWithIgnore(query);
      long risoTreeTime = System.currentTimeMillis() - start2;
      Util.println("time: " + risoTreeTime);
      Util.println("range time: " + risoTreeQueryPN.range_query_time);
      Util.println("result count: " + risoTreeQueryPN.result_count);
      Util.println("page hit: " + risoTreeQueryPN.page_hit_count);
      Util.println(risoTreeQueryPN.planDescription);

      service.shutdown();
      if (clearCache) {
        OwnMethods.clearCache(password, method);
      }
      service = Neo4jGraphUtility.getDatabaseService(dbPath);

      Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match(service);
      long start1 = System.currentTimeMillis();
      naive_Neo4j_Match.queryWithIgnore(query);
      long naiveTime = System.currentTimeMillis() - start1;
      Util.println("time: " + naiveTime);
      Util.println("result count: " + naive_Neo4j_Match.result_count);
      Util.println("page hit: " + naive_Neo4j_Match.page_access);
      Util.println(naive_Neo4j_Match.planDescription);

      service.shutdown();
      if (clearCache) {
        OwnMethods.clearCache(password, method);
      }
      service = Neo4jGraphUtility.getDatabaseService(dbPath);

      logger.info(query);
      Util.println("naive:");
      Util.println("time: " + naiveTime);
      Util.println("result count: " + naive_Neo4j_Match.result_count);
      Util.println("page hit: " + naive_Neo4j_Match.page_access);

      Util.println("risotree:");
      Util.println("time: " + risoTreeTime);
      Util.println("result count: " + risoTreeQueryPN.result_count);
      Util.println("page hit: " + risoTreeQueryPN.page_hit_count);

      naiveResults.add(new ResultRecord(naiveTime, naive_Neo4j_Match.page_access));
      risoResults.add(new ResultRecord(risoTreeTime, risoTreeQueryPN.page_hit_count));

      if (naiveTime < risoTreeTime) {
        naiveBetter.add(query);
        naiveBetterIds.add(queryId);
      } else {
        risoTreeBetter.add(query);
        risoTreeBetterIds.add(queryId);
      }
      Util.println("End of one pass.\n");
    }

    Util.println("naive is better:" + naiveBetter.size());
    Util.println("naive better query ids:" + naiveBetterIds);
    Util.println("risotree is better:" + risoTreeBetter.size());
    Util.println("risotree better query ids:" + risoTreeBetterIds);

    Util.println(naiveBetter);
    Util.println(risoTreeBetter);

    for (int i = 0; i < naiveResults.size(); i++) {
      Util.println(String.format("%d\t%d\t%d\t%d", naiveResults.get(i).runTime,
          naiveResults.get(i).pageHit, risoResults.get(i).runTime, risoResults.get(i).pageHit));
    }

    Util.println("\nnaive average time: " + ResultRecord.getRunTimeAvg(naiveResults));
    Util.println("risotree average time: " + ResultRecord.getRunTimeAvg(risoResults));

  }

  /**
   * For each approach test all the queries.
   * 
   * @throws Exception
   */
  @Test
  public void queryWithIgnoreBulkColumnTest() throws Exception {
    List<String> naiveBetter = new ArrayList<>();
    List<String> risoTreeBetter = new ArrayList<>();

    List<Long> naiveTimes = new ArrayList<>();
    List<Long> risoTimes = new ArrayList<>();

    List<String> queries = ReadWriteUtil.readFileAllLines(queryPath);
    queries = queries.subList(0, queryCount);
    int queryId = -1;

    queryId = -1;
    for (String query : queries) {
      queryId++;
      Util.println("query id: " + queryId);
      Util.println(query);

      Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match(service);
      long start1 = System.currentTimeMillis();
      naive_Neo4j_Match.queryWithIgnore(query);
      long naiveTime = System.currentTimeMillis() - start1;
      Util.println("time: " + naiveTime);
      Util.println("result count: " + naive_Neo4j_Match.result_count);
      Util.println("page hit: " + naive_Neo4j_Match.page_access);
      Util.println(naive_Neo4j_Match.planDescription);
      naiveTimes.add(naiveTime);
      service.shutdown();
      if (clearCache) {
        OwnMethods.clearCache(password, method);
      }
      service = Neo4jGraphUtility.getDatabaseService(dbPath);
    }

    queryId = -1;
    for (String query : queries) {
      queryId++;
      Util.println("query id: " + queryId);
      Util.println(query);

      RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(service, dataset, 2);
      long start2 = System.currentTimeMillis();
      risoTreeQueryPN.queryWithIgnore(query);
      long risoTreeTime = System.currentTimeMillis() - start2;
      Util.println("time: " + risoTreeTime);
      Util.println("result count: " + risoTreeQueryPN.result_count);
      Util.println("page hit: " + risoTreeQueryPN.page_hit_count);
      Util.println(risoTreeQueryPN.planDescription);
      risoTimes.add(risoTreeTime);
      service.shutdown();
      if (clearCache) {
        OwnMethods.clearCache(password, method);
      }
      service = Neo4jGraphUtility.getDatabaseService(dbPath);

    }

    for (int i = 0; i < queryCount; i++) {
      if (naiveTimes.get(i) < risoTimes.get(i)) {
        naiveBetter.add(queries.get(i));
      } else {
        risoTreeBetter.add(queries.get(i));
      }
    }

    logger.info("naive is better:" + naiveBetter.size());
    logger.info("risotree is better:" + risoTreeBetter.size());

    Util.println(naiveBetter);
    Util.println(risoTreeBetter);

    for (int i = 0; i < naiveTimes.size(); i++) {
      Util.println(String.format("%d\t%d", naiveTimes.get(i), risoTimes.get(i)));
    }
  }
}
