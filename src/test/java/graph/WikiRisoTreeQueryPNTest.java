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
import commons.Enums;
import commons.Enums.ClearCacheMethod;
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
  static final Enums.ClearCacheMethod method = RunTimeConfigure.clearMethod;
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
    String query = queries.get(0);
    logger.info(query);

    // Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match(service);
    // long start = System.currentTimeMillis();
    // naive_Neo4j_Match.query(query);
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

    // service.shutdown();
    // service = Neo4jGraphUtility.getDatabaseService(dbPath);
    //
    // Util.println("spatial first:");
    // SpatialFirst_List spatialFirst_List = new SpatialFirst_List(service, dataset);
    // spatialFirst_List.query_Block(query);
    // Util.println(spatialFirst_List.run_time);
    // Util.println(spatialFirst_List.range_query_time);
    // Util.println(spatialFirst_List.result_count);
    // Util.println(spatialFirst_List.page_hit_count);

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
      printRisoTreeTime(risoTreeQueryPN);

      service.shutdown();
      if (clearCache) {
        OwnMethods.clearCache(password, method);
      }
      service = Neo4jGraphUtility.getDatabaseService(dbPath);

      Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match(service);
      long start1 = System.currentTimeMillis();
      naive_Neo4j_Match.query(query);
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

    Util.println(naiveBetter);
    Util.println(risoTreeBetter);

    Util.println("naive is better:" + naiveBetter.size());
    Util.println("naive better query ids:" + naiveBetterIds);
    Util.println("risotree is better:" + risoTreeBetter.size());
    Util.println("risotree better query ids:" + risoTreeBetterIds);

    for (int i = 0; i < naiveResults.size(); i++) {
      Util.println(String.format("%d\t%d\t%d\t%d\t%d", i, naiveResults.get(i).runTime,
          naiveResults.get(i).pageHit, risoResults.get(i).runTime, risoResults.get(i).pageHit));
    }

    Util.println("\nnaive average time: " + ResultRecord.getRunTimeAvg(naiveResults));
    Util.println("risotree average time: " + ResultRecord.getRunTimeAvg(risoResults));

  }

  private void printRisoTreeTime(RisoTreeQueryPN risoTreeQueryPN) {
    Util.println("range time: " + risoTreeQueryPN.range_query_time);
    Util.println("set label time: " + risoTreeQueryPN.set_label_time);
    Util.println("remove label time: " + risoTreeQueryPN.remove_label_time);
    Util.println("get iterator time: " + risoTreeQueryPN.get_iterator_time);
    Util.println("iterate time: " + risoTreeQueryPN.iterate_time);

    Util.println("result count: " + risoTreeQueryPN.result_count);
    Util.println("page hit: " + risoTreeQueryPN.page_hit_count);
    Util.println(risoTreeQueryPN.planDescription);
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
    List<Integer> naiveBetterIds = new ArrayList<>();
    List<Integer> risoTreeBetterIds = new ArrayList<>();

    List<ResultRecord> naiveResults = new ArrayList<>();
    List<ResultRecord> risoResults = new ArrayList<>();

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
      try {
        naive_Neo4j_Match.query(query);
      } catch (Exception e) {
        e.printStackTrace();
        // service.shutdown();
        if (clearCache) {
          OwnMethods.clearCache(password, method);
        }
        // service = Neo4jGraphUtility.getDatabaseService(dbPath);
        continue;
      }
      long naiveTime = System.currentTimeMillis() - start1;

      // service.shutdown();
      if (clearCache) {
        OwnMethods.clearCache(password, method);
      }
      // service = Neo4jGraphUtility.getDatabaseService(dbPath);

      Util.println("time: " + naiveTime);
      Util.println("result count: " + naive_Neo4j_Match.result_count);
      Util.println("page hit: " + naive_Neo4j_Match.page_access);
      Util.println(naive_Neo4j_Match.planDescription);
      naiveResults.add(new ResultRecord(naiveTime, naive_Neo4j_Match.page_access));
    }

    service.shutdown();
    OwnMethods.clearCache(password, method);
    service = Neo4jGraphUtility.getDatabaseService(dbPath);

    queryId = -1;
    for (String query : queries) {
      queryId++;
      Util.println("query id: " + queryId);
      Util.println(query);

      RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(service, dataset, 2);
      long start2 = System.currentTimeMillis();
      try {
        risoTreeQueryPN.queryWithIgnore(query);
      } catch (Exception e) {
        // TODO: handle exception
        e.printStackTrace();
        // service.shutdown();
        if (clearCache) {
          OwnMethods.clearCache(password, method);
        }
        // service = Neo4jGraphUtility.getDatabaseService(dbPath);
        continue;
      }
      long risoTreeTime = System.currentTimeMillis() - start2;

      // service.shutdown();
      if (clearCache) {
        OwnMethods.clearCache(password, method);
      }
      // service = Neo4jGraphUtility.getDatabaseService(dbPath);

      Util.println("time: " + risoTreeTime);
      printRisoTreeTime(risoTreeQueryPN);
      risoResults.add(new ResultRecord(risoTreeTime, risoTreeQueryPN.page_hit_count));
    }

    for (int i = 0; i < queryCount; i++) {
      if (naiveResults.get(i).runTime < risoResults.get(i).runTime) {
        naiveBetter.add(queries.get(i));
        naiveBetterIds.add(i);
      } else {
        risoTreeBetter.add(queries.get(i));
        risoTreeBetterIds.add(i);
      }
    }

    Util.println(naiveBetter);
    Util.println(risoTreeBetter);

    Util.println("naive is better:" + naiveBetter.size());
    Util.println("naive better query ids:" + naiveBetterIds);
    Util.println("risotree is better:" + risoTreeBetter.size());
    Util.println("risotree better query ids:" + risoTreeBetterIds);


    for (int i = 0; i < naiveResults.size(); i++) {
      Util.println(String.format("%d\t%d\t%d\t%d\t%d", i, naiveResults.get(i).runTime,
          naiveResults.get(i).pageHit, risoResults.get(i).runTime, risoResults.get(i).pageHit));
    }

    Util.println("\nnaive average time: " + ResultRecord.getRunTimeAvg(naiveResults));
    Util.println("risotree average time: " + ResultRecord.getRunTimeAvg(risoResults));
  }
}
