package CypherMiddleWare;

import static org.junit.Assert.assertTrue;
import java.io.File;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import commons.Neo4jGraphUtility;
import commons.Query_Graph;
import commons.Util;
import cypher.middleware.CypherDecoder;

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
    // Util.println(query);
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
  public void getQueryGraphTest() throws Exception {
    GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(dbPath);
    Query_Graph query_Graph = CypherDecoder.getQueryGraph(query, service);
    Util.println("qury graph: \n" + query_Graph.toString());
    Util.close(service);
  }

  @Test
  public void getSpatialPredicteTest() throws Exception {
    Util.println(CypherDecoder.getSpatialPredicates(query));
  }

  @Test
  public void getSingleAttributeInScalaStringTest() throws Exception {
    // Util.println(CypherDecoder.getSingleAttributeInScalaString(
    // "(spatialnode),PropertyKeyName(lat)),DecimalDoubleLiteral(22.225842149771214)",
    // "DecimalDoubleLiteral"));
    Util.println(CypherDecoder.getSingleAttributeInScalaString(
        "DecimalDoubleLiteral(22.225842149771214)", "DecimalDoubleLiteral"));
  }

  @Test
  public void getStringScalaTest() throws Exception {
    String string =
        "Query(None,SingleQuery(List(Match(false,Pattern(List(EveryPath(RelationshipChain(NodePattern(Some(Variable(a)),List(LabelName(heritage designation)),None),RelationshipPattern(Some(Variable(b)),List(),None,None,BOTH,false),NodePattern(Some(Variable(spatialnode)),List(LabelName(museum)),None))))),List(),Some(Where(And(Ands(Set(LessThanOrEqual(DecimalDoubleLiteral(22.187478257613602),Property(Variable(spatialnode),PropertyKeyName(lat))), LessThanOrEqual(Property(Variable(spatialnode),PropertyKeyName(lat)),DecimalDoubleLiteral(22.225842149771214)))),Ands(Set(LessThanOrEqual(DecimalDoubleLiteral(113.50180238485339),Property(Variable(spatialnode),PropertyKeyName(lon))), LessThanOrEqual(Property(Variable(spatialnode),PropertyKeyName(lon)),DecimalDoubleLiteral(113.56607615947725)))))))), Return(false,ReturnItems(false,List(UnaliasedReturnItem(Variable(spatialnode),spatialnode))),None,None,None,Some(Limit(SignedDecimalIntegerLiteral(5))),Set()))))";
    List<String> strings = CypherDecoder.getStringScala(string, "LessThanOrEqual");
    Util.println(strings);
  }

  @Test
  public void splitTest() {
    List<String> list = null;
    list = CypherDecoder.split("xxxherexxxtherexxxtobe", "xxx");
    assertTrue(list.size() == 4);
    assertTrue(list.get(0).equals(""));
    assertTrue(list.get(1).equals("here"));
    assertTrue(list.get(2).equals("there"));
    assertTrue(list.get(3).equals("tobe"));

    list = CypherDecoder.split("DecimalDoubleLiteral(22.225842149771214)", "DecimalDoubleLiteral");
    assertTrue(list.size() == 2);
    assertTrue(list.get(0).equals(""));
    assertTrue(list.get(1).equals("(22.225842149771214)"));
  }

  @Test
  public void getContentWithinParenthesesTest() throws Exception {
    assertTrue(CypherDecoder.getContentWithinParentheses("(xxx)").equals("xxx"));
    assertTrue(CypherDecoder.getContentWithinParentheses("(x(y))").equals("x(y)"));
    assertTrue(CypherDecoder.getContentWithinParentheses("(x(y(z)))").equals("x(y(z))"));

  }

  // @Test
  // public void getQueryGraphTest() throws Exception {
  // query =
  // "MATCH (a:`heritage designation`)-[b]-(spatialnode:museum) WHERE 22.187478257613602 <=
  // spatialnode.lat <= 22.225842149771214 AND 113.50180238485339 <= spatialnode.lon <=
  // 113.56607615947725 RETURN spatialnode LIMIT 5";
  // Util.println(query);
  // GraphDatabaseService service = Neo4jGraphUtility.getDatabaseServiceNotExistCreate(dbPath);
  // Query_Graph query_Graph = CypherDecoder.getQueryGraph(query, "spatialnode",
  // new MyRectangle(
  // "(22.187478257613602, 113.50180238485339, 22.225842149771214, 113.56607615947725)"),
  // service);
  // Util.println(query_Graph);
  // Util.println(Arrays.toString(query_Graph.label_list_string));
  //
  // Util.close(service);
  // }

}
