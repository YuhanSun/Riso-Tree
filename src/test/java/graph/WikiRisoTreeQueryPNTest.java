package graph;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import commons.Config.Datasets;
import commons.Neo4jGraphUtility;
import commons.Util;

public class WikiRisoTreeQueryPNTest {

  String dbPath =
      "D:\\Project_Data\\wikidata-20180308-truthy-BETA.nt\\neo4j-community-3.4.12_risotree_test\\data\\databases\\graph.db";
  GraphDatabaseService service = null;

  @Before
  public void setUp() throws Exception {
    service = Neo4jGraphUtility.getDatabaseService(dbPath);
  }

  @After
  public void tearDown() throws Exception {
    Util.close(service);
  }

  @Test
  public void queryWithIgnoreTest() throws Exception {
    String query = "MATCH (a:`big city`)-[b]-(spatialnode:university) "
        + "WHERE 17.684805514724214 <= spatialnode.lat <= 33.97125439168383 AND 75.2649384026851 <= spatialnode.lon <= 98.33687275901212  "
        + "RETURN *";



    Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match(service);
    long start = System.currentTimeMillis();
    naive_Neo4j_Match.queryWithIgnore(query);
    Util.println("time: " + (System.currentTimeMillis() - start));
    Util.println("result count: " + naive_Neo4j_Match.result_count);
    Util.println("page hit: " + naive_Neo4j_Match.page_access);

    RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(service, Datasets.wikidata.name(), 1);
    start = System.currentTimeMillis();
    risoTreeQueryPN.queryWithIgnore(query);
    Util.println("time: " + (System.currentTimeMillis() - start));
    Util.println("result count: " + risoTreeQueryPN.result_count);
    Util.println("page hit: " + risoTreeQueryPN.page_hit_count);
  }

}
