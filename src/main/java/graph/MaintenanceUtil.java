package graph;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import commons.Config;
import commons.Enums.MaintenanceStatistic;
import commons.Neo4jGraphUtility;
import commons.Util;

public class MaintenanceUtil {

  public static <K> long getAverage(List<Map<K, Object>> records, K key)
      throws Exception {
    long sum = 0;
    for (Map<K, Object> map : records) {
      Object value = map.get(key);
      if (!map.containsKey(key)) {
        throw new Exception(key + " does not exist in record!");
      }
      sum += Long.parseLong(value.toString());
    }
    return sum / records.size();
  }

  public static List<MaintenanceStatistic> getMaintenanceStatistic() {
    List<MaintenanceStatistic> maintenanceStatisticList =
        new LinkedList<>(Arrays.asList(MaintenanceStatistic.runTime, MaintenanceStatistic.getPNTime,
            MaintenanceStatistic.convertIdTime, MaintenanceStatistic.updatePNTime,
            MaintenanceStatistic.updateSafeNodesTime, MaintenanceStatistic.getRTreeLeafNodeTime,
            MaintenanceStatistic.updateLeafNodePNTime, MaintenanceStatistic.createEdgeTime,
            MaintenanceStatistic.commitTime, MaintenanceStatistic.visitedNodeCount));
    return maintenanceStatisticList;
  }

  public static String attach(String left, String right) {
    return left + Config.PNSeparator + right;
  }

  /**
   * Divide a set of nodes based on their labels.
   *
   * @param nodes
   * @return
   */
  public static Map<String, Set<Node>> divideByLabel(Set<Node> nodes) {
    Map<String, Set<Node>> labelNodesMap = new HashMap<>();
    for (Node node : nodes) {
      for (Label label : node.getLabels()) {
        Set<Node> labelNodes = labelNodesMap.get(label.toString());
        if (labelNodes == null) {
          labelNodes = new HashSet<>();
          labelNodesMap.put(label.toString(), labelNodes);
        }
        labelNodes.add(node);
      }
    }
    return labelNodesMap;
  }

  /**
   * Get the PN for a given node.
   *
   * @param dbservice
   * @param source
   * @param bound
   * @return
   */
  public static Map<String, Set<Node>> getPNGeneral(GraphDatabaseService dbservice, Node source,
      int bound) {
    // <PN_propertyName, set_of_nodes>
    Map<String, Set<Node>> PNMap = new HashMap<>();
    Queue<String> propertyNames = new LinkedList<>();
    // Initialize the start state from the source node
    Map<String, Set<Node>> labelNodesMap = divideByLabel(new HashSet<>(Arrays.asList(source)));
    for (String labelStr : labelNodesMap.keySet()) {
      String propertyName = attach(Config.PNPrefix, labelStr);
      propertyNames.add(propertyName);
      PNMap.put(propertyName, new HashSet<>(Arrays.asList(source)));
    }
    int curCount = propertyNames.size(); // count of PN properties with current hop number
    for (int i = 1; i <= bound; i++) {
      int nextCount = 0;
      for (int j = 0; j < curCount; j++) {
        String properName = propertyNames.poll();
        Set<Node> pathNeighbors = PNMap.get(properName);
        Set<Node> nextNeighbors = getNeighborsInSet(pathNeighbors);
        Map<String, Set<Node>> nextLabelNodes = divideByLabel(nextNeighbors);
        for (String labelStr : nextLabelNodes.keySet()) {
          String nextPropertyName = attach(properName, labelStr);
          PNMap.put(nextPropertyName, nextLabelNodes.get(labelStr));
          propertyNames.add(nextPropertyName);
        }
        nextCount += nextLabelNodes.size();
      }
      curCount = nextCount;
    }
    return PNMap;
  }

  private static Set<Node> getNeighborsInSet(Set<Node> pathNeighbors) {
    Set<Node> nodes = new HashSet<>();
    for (Node node : pathNeighbors) {
      Set<Node> neighbors = Neo4jGraphUtility.getGraphNeighborsNoRTree(node);
      nodes.addAll(neighbors);
    }
    return nodes;
  }

  /**
   * Get path neighbors of a given node. The returned format is <propertyName, set_of_nodes>. The
   * source node itself is stored in <PN_labelOfSource, source>.
   * 
   * @param dbservice
   * @param source
   * @param bound
   * @return
   */
  static HashMap<String, HashSet<Node>> getPN(GraphDatabaseService dbservice, Node source,
      int bound) {
    Queue<String> propertyNames = new LinkedList<>();
    int labelId = getNodeLabelId(source);
    propertyNames.add("" + labelId);
    // <PN_propertyName, set_of_nodes>
    HashMap<String, HashSet<Node>> PNMap = new HashMap<>();
    PNMap.put("" + labelId, new HashSet<>(Arrays.asList(source)));
    int curCount = 1; // count of PN properties with current hop number
    for (int i = 0; i < bound; i++) {
      int nextCount = 0;
      for (int j = 0; j < curCount; j++) {
        String properName = propertyNames.poll();
        HashSet<Node> pathNeighbors = PNMap.get(properName);
        HashMap<Integer, HashSet<Node>> nextPathNeighbors = new HashMap<>();
        for (Node pathNeighbor : pathNeighbors) {
          HashSet<Node> nextNeighbors = Neo4jGraphUtility.getGraphNeighbors(pathNeighbor);
          for (Node nextNeighbor : nextNeighbors) {
            Iterable<Label> labels = nextNeighbor.getLabels();
            for (Label label : labels) {
              String labelName = label.toString();
              String[] lStrings = labelName.split("_");
              int labelID = Integer.parseInt(lStrings[1]);
              HashSet<Node> update = nextPathNeighbors.get(labelID);
              if (update == null) {
                update = new HashSet<>();
                nextPathNeighbors.put(labelID, update);
              }
              update.add(nextNeighbor);
            }
          }
        }

        nextCount += nextPathNeighbors.size();
        for (Integer key : nextPathNeighbors.keySet()) {
          String nextPropertyName = String.format("%s_%d", properName, key);
          propertyNames.add(nextPropertyName);
          PNMap.put(nextPropertyName, nextPathNeighbors.get(key));
        }

      }
      curCount = nextCount;
    }
    return PNMap;
  }

  /**
   * Get the id int from "GRAPH_id" format.
   *
   * @param node
   * @return
   */
  public static int getNodeLabelId(Node node) {
    Iterable<Label> labels = node.getLabels();
    for (Label label : labels) {
      String string = label.toString();
      if (string.matches("GRAPH_\\d+")) {
        return Integer.parseInt(string.replaceFirst("GRAPH_", ""));
      }
    }
    Util.println("Node " + node + " has no label of format \"GRAPH_\\d\"");
    node.getGraphDatabase().shutdown();
    System.exit(-1);
    return -1;
  }

  public static String getReversePropertyName(String[] labelStrings) {
    String prefix = Config.PNPrefix;
    for (int i = labelStrings.length - 1; i >= 0; i--) {
      prefix += "_" + labelStrings[i];
    }
    return prefix;
  }

  public static String getReversePnName(String pnName) {
    String[] strings = StringUtils.split(pnName, Config.PNSeparator);
    String reverse = strings[0];
    for (int i = strings.length - 1; i >= 1; i--) {
      reverse = attach(reverse, strings[i]);
    }
    return reverse;
  }

  public static String concatenatePnNames(String leftPnName, String rightPnName) {
    String rightLeftOver = rightPnName.replaceFirst(Config.PNPrefix, "");
    return leftPnName + rightLeftOver;
  }

}
