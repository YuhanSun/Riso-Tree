package commons;
import java.util.ArrayList;
import commons.Query_Graph.LabelType;

public class TestUtils {
  public final static int nodeCount = 3;
  
  public static Query_Graph getExampleGraph() {
    Query_Graph query_Graph = new Query_Graph(nodeCount, LabelType.STRING);
    ArrayList<ArrayList<Integer>> graph = new ArrayList<ArrayList<Integer>>(nodeCount);
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
    return query_Graph;
  }
}
