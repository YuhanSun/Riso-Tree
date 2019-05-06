package commons;

import java.util.ArrayList;
import java.util.HashMap;

public class Query_Graph {

  public static enum LabelType {
    STRING, INT,
  }

  public LabelType labelType;

  public int[] label_list;
  public String[] label_list_string;
  public String[] nodeVariables; // variable name of each node id

  public ArrayList<ArrayList<Integer>> graph;
  public MyRectangle[] spa_predicate;
  public boolean[] Has_Spa_Predicate;

  // statistic
  public HashMap<Integer, Integer> labelDistribution;
  public int highestSelectivityLabel;

  public Query_Graph(int node_count) {
    this(node_count, LabelType.INT);
  }

  public Query_Graph(int node_count, LabelType labelType) {
    this.labelType = labelType;
    label_list = new int[node_count];
    nodeVariables = new String[node_count];
    graph = new ArrayList<ArrayList<Integer>>(node_count);
    for (int i = 0; i < node_count; i++)
      graph.add(new ArrayList<Integer>());
    spa_predicate = new MyRectangle[node_count];
    Has_Spa_Predicate = new boolean[node_count];
  }

  public Query_Graph(String string) {
    String[] lStrings = string.split("\n");
    int nodeCount = lStrings.length;
    labelType = LabelType.INT;

    label_list = new int[nodeCount];
    graph = new ArrayList<ArrayList<Integer>>(nodeCount);
    for (int i = 0; i < nodeCount; i++)
      graph.add(new ArrayList<Integer>());
    spa_predicate = new MyRectangle[nodeCount];
    Has_Spa_Predicate = new boolean[nodeCount];

    for (String line : lStrings) {
      String[] lineList = line.split(" ");
      int id = Integer.parseInt(lineList[0]);
      label_list[id] = Integer.parseInt(lineList[1]);
      int neighborCount = Integer.parseInt(lineList[2]);
      for (int i = 0; i < neighborCount; i++) {
        int neighborID = Integer.parseInt(lineList[i + 3]);
        graph.get(id).add(neighborID);
      }
      Has_Spa_Predicate[id] = Boolean.valueOf(lineList[lineList.length - 1]);
    }
  }

  @Override
  public String toString() {
    String string = "";
    for (int i = 0; i < label_list.length; i++) {
      ArrayList<Integer> neighbors = graph.get(i);
      string += String.format("%d,%d", i, neighbors.size());
      for (int neighbor : neighbors)
        string += String.format(",%d", neighbor);
      string += "," + String.valueOf(Has_Spa_Predicate[i]);
      string += String.format(",%d\n", label_list[i]);
    }

    return string;
  }

  public String getNodeLabelString(int id) {
    return labelType.equals(LabelType.INT) ? String.valueOf(label_list[id]) : label_list_string[id];
  }

  public void iniStatistic() {
    labelDistribution = new HashMap<Integer, Integer>();
    for (int label : label_list)
      if (labelDistribution.containsKey(label) == false)
        labelDistribution.put(label, 0);

    for (int label : label_list)
      labelDistribution.put(label, labelDistribution.get(label) + 1);

    int highestSelectivity = Integer.MAX_VALUE;
    for (int key : labelDistribution.keySet())
      if (labelDistribution.get(key) < highestSelectivity) {
        highestSelectivity = labelDistribution.get(key);
        highestSelectivityLabel = key;
      }
  }

  /**
   * decide whether a given query graph is the isomorphism spatial predicate is not considered in
   * this case\ very loose, consider number of labels and labels distribution
   * 
   * @param query_Graph
   * @return
   */
  public boolean isIsomorphism(Query_Graph query_Graph) {
    if (label_list.length != query_Graph.label_list.length)
      return false;

    HashMap<Integer, Integer> p_labelDistribution = query_Graph.labelDistribution;

    if (labelDistribution.size() != query_Graph.labelDistribution.size())
      return false;

    for (int key : labelDistribution.keySet()) {
      if (p_labelDistribution.containsKey(key) == false)
        return false;
      if (p_labelDistribution.get(key) != labelDistribution.get(key))
        return false;
    }

    return true;

  }
}
