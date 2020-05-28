package CypherMiddleWare;

import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import org.junit.Test;
import commons.Config.Explain_Or_Profile;
import commons.MyRectangle;
import commons.Query_Graph;
import commons.Query_Graph.LabelType;
import commons.Util;
import cypher.middleware.CypherEncoder;

public class CypherEncoderTest {

  @Test
  public void formCypherQueryTest() {
    int nodeCount = 3;
    Query_Graph query_Graph = new Query_Graph(3, LabelType.STRING);
    ArrayList<ArrayList<Integer>> graph = new ArrayList<ArrayList<Integer>>(3);
    graph.add(new ArrayList<Integer>() {
      {
      add(1);
      add(2);
      }
    });
    graph.add(new ArrayList<Integer>() {
      {
        add(0);
      }
    });
    graph.add(new ArrayList<Integer>() {
      {
        add(0);
      }
    });
    query_Graph.graph = graph;
    for (int i = 0; i < nodeCount; i++) {
      query_Graph.nodeVariables[i] = "a" + i;
      query_Graph.label_list_string[i] = "" + i;
    }
    query_Graph.Has_Spa_Predicate[0] = true;
    query_Graph.spa_predicate[0] = new MyRectangle(0, 1, 2, 3);
    String queryString = CypherEncoder.formCypherQuery(query_Graph, -1, Explain_Or_Profile.Explain);
    Util.println(queryString);
    String baselineString =
        "explain match (a0:`0`),(a1:`1`),(a2:`2`),(a0)--(a1),(a0)--(a2) where 0.000000 <= a0.lon <= 2.000000 and 1.000000 <= a0.lat <= 3.000000 return id(a0),id(a1),id(a2)";
    assertTrue(queryString.equals(baselineString));
  }

}
