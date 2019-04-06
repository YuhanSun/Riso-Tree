package CypherMiddleWare;

import static org.junit.Assert.assertTrue;
import java.io.File;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import commons.Neo4jGraphUtility;
import commons.Query_Graph;
import commons.Util;

public class CypherDecoderTest {

  String homeDir, dbPath;
  String query;

  @Before
  public void setUp() {
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource("").getFile());
    homeDir = file.getAbsolutePath();
    dbPath = homeDir + "/data/graph.db";
    query = "match (a:A)-->(b:B)<--(c:TEST) where a.type = 0 return a, b, c limit 10";
    query = "MATCH (c:C)--(a:A)-[b]-(spatialnode:Spatial) WHERE "
        + "22.187478257613602 <= spatialnode.lat <= 22.225842149771214 AND "
        + "113.50180238485339 <= spatialnode.lon <= 113.56607615947725 " + "RETURN * LIMIT 10";
    query =
        "MATCH (a:`heritage designation`)-[b]-(spatialnode:museum) WHERE 22.187478257613602 <= spatialnode.lat <= 22.225842149771214 AND 113.50180238485339 <= spatialnode.lon <= 113.56607615947725 RETURN spatialnode LIMIT 5";
    Util.println(query);
  }


  @Test
  public void getNodeStringsTest() {
    String[] strings = CypherDecoder.getNodeStrings(query);
    assertTrue(strings.length == 3);
    assertTrue(strings[0].equals("a:A"));
    assertTrue(strings[1].equals("b:B"));
    assertTrue(strings[2].equals("c:TEST"));
  }

  @Test
  public void getQueryGraphTest() {
    query =
        "MATCH (a:`heritage designation`)-[b]-(spatialnode:museum) WHERE 22.187478257613602 <= spatialnode.lat <= 22.225842149771214 AND 113.50180238485339 <= spatialnode.lon <= 113.56607615947725 RETURN spatialnode LIMIT 5";
    Util.println(query);
    GraphDatabaseService service = Neo4jGraphUtility.getDatabaseServiceNotExistCreate(dbPath);
    Query_Graph query_Graph = CypherDecoder.getQueryGraph(query, "spatialnode",
        "(22.187478257613602, 113.50180238485339, 22.225842149771214, 113.56607615947725)",
        service);
    Util.println(query_Graph);
    Util.println(Arrays.toString(query_Graph.label_list_string));

    service.shutdown();
  }

}
