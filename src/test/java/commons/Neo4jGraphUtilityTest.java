package commons;

import java.util.HashSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

public class Neo4jGraphUtilityTest {

  private String dbPath =
      "D:\\Ubuntu_shared\\GeoMinHop\\data\\Yelp_100\\neo4j-community-3.1.1_Yelp_100_withPN\\data\\databases\\graph.db";
  private GraphDatabaseService dbservice = null;

  @Before
  public void setUp() throws Exception {
    dbservice = Neo4jGraphUtility.getDatabaseService(dbPath);
  }

  @After
  public void tearDown() throws Exception {
    dbservice.shutdown();
  }

  @Test
  public void getNeighborsTest() {
    Transaction tx = dbservice.beginTx();
    ResourceIterator<Node> testNodes = dbservice.findNodes(Label.label("GRAPH_1"));
    Node testNode = null;
    while (testNodes.hasNext()) {
      testNode = testNodes.next();
      break;
    }
    Utility.print(testNode);
    HashSet<Node> neighbors = Neo4jGraphUtility.getGraphNeighbors(testNode);
    for (Node node : neighbors) {
      System.out.println(node.getLabels());
    }
    tx.success();
    tx.close();
  }

  @Test
  public void nodeHashTest() {
    Transaction tx = dbservice.beginTx();
    Node node1 = dbservice.getNodeById(100);
    Node node2 = dbservice.getNodeById(100);
    System.out.println(node1.equals(node2));

    HashSet<Node> nodes = new HashSet<>();
    nodes.add(node1);
    nodes.add(node2);
    System.out.println(nodes.size());

    tx.success();
    tx.close();
  }

}
