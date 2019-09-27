package graph;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import commons.Config;
import commons.Neo4jGraphUtility;
import commons.Util;

public class MaintenanceUtil {

  // static boolean addEdge(GraphDatabaseService dbservice, Node src, Node trg, int bound) {
  // HashMap<String, HashSet<Node>> pathNeighborsSrc = getPN(dbservice, src, bound - 1);
  // HashMap<String, HashSet<Node>> pathNeighborsTrg = getPN(dbservice, trg, bound - 1);
  //
  // for (String propertyName : pathNeighborsSrc.keySet()) {
  // String[] labelList = propertyName.split("_");
  // String endLabel = labelList[labelList.length - 1];
  // // if the end label is 1 which indicates it is spatial
  // if (endLabel.equals("1")) {
  // int hopDist = labelList.length - 1;
  // String prefix = getReversePropertyName(labelList);
  // HashSet<Node> neighbors = pathNeighborsSrc.get(propertyName);
  //
  // List<String> usefulProperties = new ArrayList<>();
  // for (String trgPropertyName : pathNeighborsTrg.keySet()) {
  // String[] trgLabelList = trgPropertyName.split("_");
  // if (trgLabelList.length <= bound - hopDist) {
  // usefulProperties.add(trgPropertyName);
  // }
  // }
  //
  // if (usefulProperties.size() > 0) {
  // HashSet<Node> leafNodes = new HashSet<>();
  // for (Node node : neighbors) {
  // Node leafNode = node.getSingleRelationship(RTreeRel.RTREE_REFERENCE, Direction.INCOMING)
  // .getStartNode();
  // leafNodes.add(leafNode);
  // }
  // for (Node leafNode : leafNodes) {
  // for (String usefulProperty : usefulProperties) {
  // String updateProperty = prefix + "_" + usefulProperty;
  // TreeSet<Integer> updateList = new TreeSet<>();
  // int[] neighborList = null;
  // if (leafNode.hasProperty(updateProperty)) {
  // neighborList = (int[]) leafNode.getProperty(updateProperty);
  // TreeSet<Integer> addIds = new TreeSet<>();
  // HashSet<Node> nodes = pathNeighborsTrg.get(usefulProperty);
  // for (int id : neighborList) {
  // updateList.add(id);
  // }
  // for (Node node : nodes) {
  // updateList.add((int) node.getProperty("id"));
  // }
  // } else {
  // neighborList = new int[0];
  // }
  //
  // if (updateList.size() > neighborList.length) {
  // int[] updateArray = new int[updateList.size()];
  // int i = 0;
  // for (int element : updateList) {
  // updateArray[i] = element;
  // i++;
  // }
  // leafNode.setProperty(updateProperty, updateArray);
  // }
  // }
  // }
  // }
  //
  // }
  // }
  // return true;
  // }

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
    for (int i = labelStrings.length - 2; i >= 0; i--) {
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
