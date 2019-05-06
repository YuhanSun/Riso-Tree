package cypher.middleware;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import commons.ExecutionPlanDescriptionUtil;
import commons.MyRectangle;
import commons.Query_Graph;
import commons.Query_Graph.LabelType;
import commons.Util;

public class CypherDecoder {

  public static Query_Graph getQueryGraph(String query, String spatialNode, MyRectangle rectangle,
      GraphDatabaseService service) {
    String[] nodeStrings = getNodeStrings(query);
    HashMap<String, Integer> nodeVariableIdMap = getNodeVariableIdMap(nodeStrings);
    String[] labelList = getQueryLabelList(nodeVariableIdMap, nodeStrings);
    Result result = service.execute("explain " + query);
    ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
    Util.println(planDescription);
    List<ExecutionPlanDescription> plans =
        ExecutionPlanDescriptionUtil.getRequired(planDescription);
    ArrayList<ArrayList<Integer>> graphStructure = getGraphStructure(plans, nodeVariableIdMap);

    Query_Graph query_Graph = new Query_Graph(nodeVariableIdMap.size(), LabelType.STRING);
    query_Graph.graph = graphStructure;
    query_Graph.label_list_string = labelList;
    int spatialId = nodeVariableIdMap.get(spatialNode);
    query_Graph.Has_Spa_Predicate[spatialId] = true;
    query_Graph.spa_predicate[spatialId] = rectangle;
    String[] nodeVariables = new String[nodeVariableIdMap.size()];
    for (String variable : nodeVariableIdMap.keySet()) {
      nodeVariables[nodeVariableIdMap.get(variable)] = variable;
    }
    query_Graph.nodeVariables = nodeVariables;
    return query_Graph;
  }

  private static ArrayList<ArrayList<Integer>> getGraphStructure(
      List<ExecutionPlanDescription> plans, HashMap<String, Integer> nodeVariableIdMap) {
    ArrayList<TreeSet<Integer>> treeGraph = new ArrayList<>();
    for (int i = 0; i < nodeVariableIdMap.size(); i++) {
      treeGraph.add(new TreeSet<>());
    }
    for (ExecutionPlanDescription planDescription : plans) {
      String[] edge =
          ExecutionPlanDescriptionUtil.getEdgeInExpandExpressionPlanNode(planDescription);
      Util.println(Arrays.toString(edge));
      int id1 = nodeVariableIdMap.get(edge[0]);
      int id2 = nodeVariableIdMap.get(edge[1]);
      treeGraph.get(id1).add(id2);
      treeGraph.get(id2).add(id1);
    }
    ArrayList<ArrayList<Integer>> graph = new ArrayList<>(treeGraph.size());
    for (TreeSet<Integer> neighbors : treeGraph) {
      graph.add(new ArrayList<Integer>(neighbors));
    }
    return graph;
  }

  private static String[] getQueryLabelList(HashMap<String, Integer> nodeVariableIdMap,
      String[] nodeStrings) {
    String[] labelList = new String[nodeVariableIdMap.size()];
    for (String string : nodeStrings) {
      Util.println(string);
      if (string.contains(":")) {
        String[] strings = StringUtils.split(string, ":");
        String nodeVariable = strings[0];
        int nodeId = nodeVariableIdMap.get(nodeVariable);
        String label = strings[1];
        labelList[nodeId] = getLabel(label);
      }
    }
    return labelList;
  }

  private static HashMap<String, Integer> getNodeVariableIdMap(String[] nodeStrings) {
    HashMap<String, Integer> nodeVariableIdMap = new HashMap<>();
    int id = 0;
    for (String string : nodeStrings) {
      if (string.contains(":")) {
        string = StringUtils.split(string, ":")[0];
      }
      if (nodeVariableIdMap.containsKey(string)) {
        continue;
      }
      nodeVariableIdMap.put(getLabel(string), id);
      id++;
    }
    return nodeVariableIdMap;
  }

  /**
   * If label contains `, remove it.
   *
   * @param label
   * @return
   */
  private static String getLabel(String label) {
    return StringUtils.replace(label, "`", "");
  }

  public static String[] getNodeStrings(String query) {
    return StringUtils.substringsBetween(query, "(", ")");
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
