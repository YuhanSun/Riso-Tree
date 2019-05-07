package cypher.middleware;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.cypher.internal.frontend.v3_4.ast.Statement;
import org.neo4j.cypher.internal.frontend.v3_4.parser.CypherParser;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import commons.Config;
import commons.ExecutionPlanDescriptionUtil;
import commons.MyRectangle;
import commons.Query_Graph;
import commons.Query_Graph.LabelType;
import commons.Util;

public class CypherDecoder {

  private static final String lessThanOrEqualKeyword = "LessThanOrEqual";
  private static final String variableKeyword = "Variable";
  private static final String propertyKeyNameKeyword = "PropertyKeyName";
  private static final String decimalDoubleLiteralKeyword = "DecimalDoubleLiteral";

  /**
   * Convert a Cypher query to Query_Graph format.
   *
   * @param query
   * @param spatialNode
   * @param rectangle
   * @param service
   * @return
   */
  public static Query_Graph getQueryGraph(String query, GraphDatabaseService service) {
    String spatialNode = "";
    MyRectangle rectangle = new MyRectangle();

    return getQueryGraph(query, spatialNode, rectangle, service);
  }

  public static Map<String, MyRectangle> getSpatialPredicates(String query) throws Exception {
    Map<String, MyRectangle> spatialPredicates = new HashMap<>();
    CypherParser parser = new CypherParser();
    Statement statement = parser.parse(query, null);
    String ast = statement.toString();
    List<String> strings = getStringScala(ast, lessThanOrEqualKeyword);
    Set<String> spatialNodeVariables = getSpatialNodeVariables(strings);
    Util.println(spatialNodeVariables);



    return spatialPredicates;
  }

  private static MyRectangle getSpatialPredicateRange(String variable, List<String> strings)
      throws Exception {
    ArrayList<Double> longitudes = new ArrayList<>();
    ArrayList<Double> latitudes = new ArrayList<>();
    for (String string : strings) {
      if (split(string, variableKeyword).get(0).equals(variable)) {
        String propertyKeyName = getSingleAttributeInScalaString(string, propertyKeyNameKeyword);
        if (propertyKeyName.equals(Config.longitude_property_name)) {
          double lon = Double
              .parseDouble(getSingleAttributeInScalaString(string, decimalDoubleLiteralKeyword));
          longitudes.add(lon);
        } else if (propertyKeyName.equals(Config.latitude_property_name)) {
          double lat = Double
              .parseDouble(getSingleAttributeInScalaString(string, decimalDoubleLiteralKeyword));
          latitudes.add(lat);
        }
        double[] lons = longitudes.toArray(new Double[2]);
      }
    }
    if (longitudes.size() != 2 || latitudes.size() != 2) {
      throw new Exception(strings + " does not contain all the range values!");
    }
  }

  public static String getSingleAttributeInScalaString(String string, String keyword)
      throws Exception {
    return getStringScala(string, keyword).get(0);
  }

  private static Set<String> getSpatialNodeVariables(List<String> strings) throws Exception {
    Set<String> spatialNodeVariables = new HashSet<>();
    for (String string : strings) {
      String variable = getSingleAttributeInScalaString(string, variableKeyword);
      spatialNodeVariables.add(variable);
    }
    return spatialNodeVariables;
  }

  public static List<String> getStringScala(String string, String keyword) throws Exception {
    List<String> res = new LinkedList<>();
    List<String> strings = split(string, keyword);
    if (strings.size() == 1) {
      throw new Exception(String.format("%s does not contain %s", string, keyword));
    }

    strings.remove(0);
    for (String piece : strings) {
      Util.println(piece);
      Util.println(getContentWithinParentheses(piece));
      res.add(getContentWithinParentheses(piece));
    }
    return res;
  }

  /**
   * Split a string by a given string.
   *
   * @param string
   * @param split
   * @return
   */
  public static List<String> split(String string, String split) {
    List<String> res = new LinkedList<>();
    int length = split.length();
    String curString = string.toString();
    while (true) {
      int index = curString.indexOf(split);
      if (index == -1) {
        break;
      }

      if (index == 0) {
        res.add("");
      }

      res.add(curString.substring(0, index));
      curString = curString.substring(index + length, curString.length());
    }

    res.add(curString);

    return res;
  }

  /**
   * For a string with the format (xxx(xx)), return the content within the most outside pair of
   * parentheses.
   *
   * @param string
   * @return
   * @throws Exception
   */
  public static String getContentWithinParentheses(String string) throws Exception {
    int left = 0;
    int index = 0;
    for (char c : string.toCharArray()) {
      if (c == '(') {
        left++;
      } else if (c == ')') {
        left--;
      }
      if (left == 0) {
        return string.substring(1, index);
      }
      index++;
    }
    throw new Exception(string + " is not valid!");
  }

  /**
   * Convert a Cypher query to Query_Graph format.
   *
   * @param query
   * @param spatialNode
   * @param rectangle
   * @param service
   * @return
   */
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
