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
import commons.Neo4jGraphUtility;
import commons.ReadWriteUtil;
import commons.Util;

public class WikiRisoTreeQueryPNTest {

  private static final Logger logger = Logger.getLogger(WikiRisoTreeQueryPNTest.class.getName());
  static final String dataset = "Yelp_100";
  static final String suffix = "Gleenes_1.0_-1_new_version";
  // String dbPath =
  // "D:\\Project_Data\\wikidata-20180308-truthy-BETA.nt\\neo4j-community-3.4.12_risotree_test\\data\\databases\\graph.db";
  // String dbPath =
  // "D:\\Project_Data\\wikidata-20180308-truthy-BETA.nt\\neo4j-community-3.4.12_Gleenes_1.0_40_new_version\\data\\databases\\graph.db";
  static final String dbPath =
      String.format("D:/Project_Data/RisoTree/%s/neo4j-community-3.4.12_%s/data/databases/graph.db",
          dataset, suffix);

  GraphDatabaseService service = null;

  static final String queryDir = "D:\\Google_Drive\\Projects\\risotree\\cypher_query\\" + dataset;
  static final String queryPath = queryDir + "\\2_0.0001";
  // String queryPath = queryDir + "\\4_0.0001";
  static final int queryCount = 10;

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
    // String query =
    // "MATCH (a:`big city`)-[b:`located in the administrative territorial
    // entity`]-(spatialnode:university) "
    // + "WHERE 17.684805514724214 <= spatialnode.lat <= 33.97125439168383 AND 75.2649384026851 <=
    // spatialnode.lon <= 98.33687275901212 "
    // + "RETURN *";

    List<String> naiveBetter = new ArrayList<>();
    List<String> risoTreeBetter = new ArrayList<>();

    List<Long> naiveTimes = new ArrayList<>();
    List<Long> risoTimes = new ArrayList<>();

    List<String> queries = ReadWriteUtil.readFileAllLines(queryPath);
    queries = queries.subList(0, queryCount);
    int queryId = -1;
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

      service.shutdown();
      service = Neo4jGraphUtility.getDatabaseService(dbPath);

      RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(service, dataset, 2);
      long start2 = System.currentTimeMillis();
      risoTreeQueryPN.queryWithIgnore(query);
      long risoTreeTime = System.currentTimeMillis() - start2;
      Util.println("time: " + risoTreeTime);
      Util.println("result count: " + risoTreeQueryPN.result_count);
      Util.println("page hit: " + risoTreeQueryPN.page_hit_count);
      Util.println(risoTreeQueryPN.planDescription);

      service.shutdown();
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

      // if (naive_Neo4j_Match.result_count != risoTreeQueryPN.result_count) {
      // throw new RuntimeException("result count mismatck!");
      // }

      naiveTimes.add(naiveTime);
      risoTimes.add(risoTreeTime);

      if (naiveTime < risoTreeTime) {
        naiveBetter.add(query);
      } else {
        risoTreeBetter.add(query);
      }
      Util.println("End of one pass.\n");
    }

    logger.info("naive is better:" + naiveBetter.size());
    logger.info("risotree is better:" + risoTreeBetter.size());

    Util.println(naiveBetter);
    Util.println(risoTreeBetter);

    for (int i = 0; i < naiveTimes.size(); i++) {
      Util.println(String.format("%d\t%d", naiveTimes.get(i), risoTimes.get(i)));
    }
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
    }
    service.shutdown();
    service = Neo4jGraphUtility.getDatabaseService(dbPath);

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
    }

    for (int i = 0; i < queryId; i++) {
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
