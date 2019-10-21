package graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import commons.Labels;
import commons.Neo4jGraphUtility;
import commons.RTreeUtility;
import commons.ReadWriteUtil;
import commons.RisoTreeUtil;
import commons.Util;

public class RisoTreeMaintenance {

  int MAX_HOPNUM;
  int maxPNSize;
  GraphDatabaseService databaseService;
  boolean safeNodesUsed;
  /**
   * The vertices that within B-hop from any spatial vertexes. Determine whether the RisoTree update
   * is required.
   */
  String safeNodesPath;
  Set<Long> safeNodes;

  public int safeCaseHappenCount = 0;

  public RisoTreeMaintenance(String dbPath, int MAX_HOPNUM, int maxPNSize, Boolean safeNodesUsed,
      String safeNodesPath) throws Exception {
    this.MAX_HOPNUM = MAX_HOPNUM;
    this.maxPNSize = maxPNSize == -1 ? Integer.MAX_VALUE : maxPNSize;
    this.safeNodesUsed = safeNodesUsed;
    if (this.safeNodesUsed) {
      this.safeNodesPath = safeNodesPath;
      this.safeNodes = readSafeNodes();
    }
    this.databaseService = Neo4jGraphUtility.getDatabaseService(dbPath);
  }

  private Set<Long> readSafeNodes() throws Exception {
    List<String> arrayList = ReadWriteUtil.readFileAllLines(safeNodesPath);
    Set<Long> safeNodes = new HashSet<>();
    for (String id : arrayList) {
      safeNodes.add(Long.parseLong(id));
    }
    return safeNodes;
  }

  public void addEdge(long src, long trg) {
    addEdge(databaseService.getNodeById(src), databaseService.getNodeById(trg));
  }

  public void addEdge(Node src, Node trg) {
    if (!safeNodes.contains(src.getId()) || !safeNodes.contains(trg.getId())) {
      addEdgeUpdateCase(src, trg);
    } else {
      safeCaseHappenCount++;
    }
    src.createRelationshipTo(trg, Labels.GraphRel.GRAPH_INSERT);
  }

  private void addEdgeUpdateCase(Node src, Node trg) {
    Map<String, Set<Node>> pathNeighborsSrc =
        MaintenanceUtil.getPNGeneral(databaseService, src, MAX_HOPNUM - 1);
    Map<String, Set<Node>> pathNeighborsTrg =
        MaintenanceUtil.getPNGeneral(databaseService, trg, MAX_HOPNUM - 1);

    // src has spatial pn
    if (!safeNodes.contains(src.getId())) {
      Map<String, int[]> pathNeighborsTrgSortedIds = convertToPNSortedIds(pathNeighborsTrg);
      addEdgeUpdateSingleDirection(pathNeighborsSrc, pathNeighborsTrgSortedIds);
    }
    if (!safeNodes.contains(trg.getId())) {
      Map<String, int[]> pathNeighborsSrcSortedIds = convertToPNSortedIds(pathNeighborsSrc);
      addEdgeUpdateSingleDirection(pathNeighborsTrg, pathNeighborsSrcSortedIds);
    }
  }

  /**
   * Convert the pns from node format to sorted array format.
   *
   * @param pathNeighbors
   * @return
   */
  public static Map<String, int[]> convertToPNSortedIds(Map<String, Set<Node>> pathNeighbors) {
    Map<String, int[]> pnIds = new HashMap<>();
    for (String key : pathNeighbors.keySet()) {
      Set<Node> nodes = pathNeighbors.get(key);
      TreeSet<Long> sortedIds = new TreeSet<>();
      for (Node node : nodes) {
        sortedIds.add(node.getId());
      }
      int[] ids = new int[sortedIds.size()];
      Iterator<Long> iterator = sortedIds.iterator();
      for (int i = 0; i < ids.length; i++) {
        long id = iterator.next();
        if (id > Integer.MAX_VALUE) {
          throw new RuntimeException(String.format("id is %d exceed integer range!", id));
        }
        ids[i] = (int) id;
      }
      pnIds.put(key, ids);
    }
    return pnIds;
  }

  /**
   * Update the pn of the leaf nodes that contain spatial nodes in {@code pathNeighborsSrc}.
   *
   * @param pathNeighborsSrc
   * @param pathNeighborsTrg
   */
  private void addEdgeUpdateSingleDirection(Map<String, Set<Node>> pathNeighborsSrc,
      Map<String, int[]> pathNeighborsTrgSortedIds) {
    Iterator<Entry<String, Set<Node>>> iterator = pathNeighborsSrc.entrySet().iterator();
    int minDist = Integer.MAX_VALUE;// min dist from spatial node to src node
    while (iterator.hasNext()) {
      Entry<String, Set<Node>> entry = iterator.next();
      String propertyName = entry.getKey();
      for (Node node : entry.getValue()) {
        if (Neo4jGraphUtility.isNodeSpatial(node)) {
          minDist = Math.min(RisoTreeUtil.getHopNumber(propertyName), minDist);
          Node leafNode = RTreeUtility.getParentLeafNode(node);
          updateLeafNodePN(leafNode, propertyName, pathNeighborsTrgSortedIds);
        }
      }
    }
    if (safeNodesUsed) {
      updateSafeNodes(pathNeighborsTrgSortedIds, MAX_HOPNUM - minDist);
    }
  }

  private void updateSafeNodes(Map<String, int[]> pathNeighborsTrgSortedIds, int updateUpperBound) {
    Iterator<Entry<String, int[]>> iterator = pathNeighborsTrgSortedIds.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, int[]> entry = iterator.next();
      if (RisoTreeUtil.getHopNumber(entry.getKey()) <= updateUpperBound) {
        for (int id : entry.getValue()) {
          safeNodes.add((long) id);
        }
      }
    }
  }

  /**
   * If {@code pathNeighborsTrg} contains some nodes that do not exist in {@code leafNode}, expand
   * {@code leafNode}.
   *
   * @param leafNode
   * @param propertyName
   * @param pathNeighborsTrg
   */
  private void updateLeafNodePN(Node leafNode, String propertyName,
      Map<String, int[]> pathNeighborsTrg) {
    String leftPnName = MaintenanceUtil.getReversePnName(propertyName);
    int length = RisoTreeUtil.getHopNumber(leftPnName);
    for (String rightPnName : pathNeighborsTrg.keySet()) {
      int rightLength = RisoTreeUtil.getHopNumber(rightPnName);
      if (length + rightLength <= MAX_HOPNUM) {
        int[] nodesAdded = pathNeighborsTrg.get(rightPnName);
        String pnName = MaintenanceUtil.concatenatePnNames(leftPnName, rightPnName);
        updateLeafNodeSinglePn(leafNode, pnName, nodesAdded);
      }
    }
  }

  /**
   * Update the {@code leafNode} of property {@code pnName} with node ids to be added.
   *
   * @param leafNode
   * @param pnName
   * @param nodesAdded
   */
  private void updateLeafNodeSinglePn(Node leafNode, String pnName, int[] nodesAdded) {
    Set<String> shorterPaths = RisoTreeUtil.formIgnoreSearchSet(pnName);
    Map<String, Object> properties = leafNode.getAllProperties();
    Iterator<Map.Entry<String, Object>> iterator = properties.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, Object> entry = iterator.next();
      String key = entry.getKey();
      if (key.equals(pnName)) { // leafNode has this label path
        int[] pn = (int[]) entry.getValue();
        if (pn.length == 0) { // pnName is ignored
          return;
        }
        int[] pnAfterUpdate = Util.sortedArrayMerge(pn, nodesAdded);
        if (pn.length != pnAfterUpdate.length) {
          if (pnAfterUpdate.length >= maxPNSize) {
            pnAfterUpdate = new int[0];
          }
          leafNode.setProperty(key, pnAfterUpdate);
        }
      } else if (shorterPaths.contains(key)) {
        int[] pn = (int[]) entry.getValue();
        if (pn.length == 0) { // pnName is ignored because of shorter paths
          return;
        }
      }
    }
  }

  public void deleteEdge(long src, long trg) {
    deleteEdge(databaseService.getNodeById(src), databaseService.getNodeById(trg));
  }

  private void deleteEdge(Node nodeById, Node nodeById2) {
    // TODO Auto-generated method stub

  }
}
