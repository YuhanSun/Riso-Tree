package graph;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import commons.Config.Datasets;
import commons.Neo4jGraphUtility;
import commons.Util;

public class WikiRisoTreeQueryPNTest {

  String dbPath = "";
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
  public void queryWithIgnoreTest() {
    RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(service, Datasets.wikidata.name(), 1);
    String query =
        "MATCH (a:`big city`)-[b]-(spatialnode:university) WHERE 22.279519480377537 <= spatialnode.lat <= 30.210054844294437 AND 76.62143523404609 <= spatialnode.lon <= 91.42557592735976 RETURN * LIMIT 10";
  }

}
