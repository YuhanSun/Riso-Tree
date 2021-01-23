package graph;

import java.io.FileWriter;
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
import commons.Enums.MaintenanceStatistic;
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
  public Set<Long> safeNodes;

  public long runTime = 0;
  public long getGraphNodePNTime = 0;
  public long convertIdTime = 0;
  public long updatePNTimeTotal = 0;
  public long getRTreeLeafNodeTime = 0;
  public long updateLeafNodePNTime = 0;
  public long updateSafeNodesTime = 0;
  public long createEdgeTime = 0;

  public Map<Long, Long> updateLeafNodeTimeMap = new HashMap<>();
  Map<MaintenanceStatistic, Object> maintenanceStatisticMap = new HashMap<>();

  public int safeCaseHappenCount = 0;
  public int visitedNodeCount = 0;
  public int updatePNCount = 0;

  public RisoTreeMaintenance(GraphDatabaseService service, int MAX_HOPNUM, int maxPNSize,
      Boolean safeNodesUsed, String safeNodesPath) throws Exception {
    this.MAX_HOPNUM = MAX_HOPNUM;
    this.maxPNSize = maxPNSize == -1 ? Integer.MAX_VALUE : maxPNSize;
    this.safeNodesUsed = safeNodesUsed;
    if (this.safeNodesUsed) {
      this.safeNodesPath = safeNodesPath;
      readSafeNodes();
    } else {
      this.safeNodes = new HashSet<>();
    }
    this.databaseService = service;
  }

  private void iniLogVariables() {
    runTime = 0;
    getGraphNodePNTime = 0;
    convertIdTime = 0;
    updatePNTimeTotal = 0;
    getRTreeLeafNodeTime = 0;
    updateLeafNodePNTime = 0;
    updateSafeNodesTime = 0;
    createEdgeTime = 0;
    visitedNodeCount = 0;
    
    maintenanceStatisticMap = new HashMap<>();
  }

  private void setMaintenanceStatisticMap() {
    maintenanceStatisticMap.put(MaintenanceStatistic.runTime, runTime);
    maintenanceStatisticMap.put(MaintenanceStatistic.getGraphNodePNTime, getGraphNodePNTime);
    maintenanceStatisticMap.put(MaintenanceStatistic.convertIdTime, convertIdTime);
    maintenanceStatisticMap.put(MaintenanceStatistic.updatePNTimeTotal, updatePNTimeTotal);
    maintenanceStatisticMap.put(MaintenanceStatistic.getRTreeLeafNodeTime, getRTreeLeafNodeTime);
    maintenanceStatisticMap.put(MaintenanceStatistic.updateLeafNodePNTime, updateLeafNodePNTime);
    maintenanceStatisticMap.put(MaintenanceStatistic.updateSafeNodesTime, updateSafeNodesTime);
    maintenanceStatisticMap.put(MaintenanceStatistic.createEdgeTime, createEdgeTime);
    maintenanceStatisticMap.put(MaintenanceStatistic.visitedNodeCount, visitedNodeCount);
  }

  public Map<MaintenanceStatistic, Object> getMaintenanceStatisticMap() {
    return maintenanceStatisticMap;
  }

  private void readSafeNodes() throws Exception {
    List<String> arrayList = ReadWriteUtil.readFileAllLines(safeNodesPath);
    safeNodes = new HashSet<>();
    for (String id : arrayList) {
      safeNodes.add(Long.parseLong(id));
    }
  }

  public void addEdge(long src, long trg) {
    iniLogVariables();
    long start = System.currentTimeMillis();
    addEdge(databaseService.getNodeById(src), databaseService.getNodeById(trg));
    runTime += System.currentTimeMillis() - start;
    setMaintenanceStatisticMap();
  }

  private void addEdge(Node src, Node trg) {
    if (!safeNodes.contains(src.getId()) || !safeNodes.contains(trg.getId())) {
      addEdgeUpdateCase(src, trg);
    } else {
      safeCaseHappenCount++;
    }
    long start = System.currentTimeMillis();
    src.createRelationshipTo(trg, Labels.GraphRel.GRAPH_INSERT);
    createEdgeTime += System.currentTimeMillis() - start;
  }

  private void addEdgeUpdateCase(Node src, Node trg) {
    long start = System.currentTimeMillis();
    Map<String, Set<Node>> pathNeighborsSrc =
        MaintenanceUtil.getPNGeneral(databaseService, src, MAX_HOPNUM - 1);
    Map<String, Set<Node>> pathNeighborsTrg =
        MaintenanceUtil.getPNGeneral(databaseService, trg, MAX_HOPNUM - 1);
    getGraphNodePNTime += System.currentTimeMillis() - start;

    for (Set<Node> nodes : pathNeighborsSrc.values()) {
      visitedNodeCount += nodes.size();
    }

    for (Set<Node> nodes : pathNeighborsTrg.values()) {
      visitedNodeCount += nodes.size();
    }

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
  public Map<String, int[]> convertToPNSortedIds(Map<String, Set<Node>> pathNeighbors) {
    long start = System.currentTimeMillis();
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
    convertIdTime += System.currentTimeMillis() - start;
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
    long start = System.currentTimeMillis();
    Iterator<Entry<String, Set<Node>>> iterator = pathNeighborsSrc.entrySet().iterator();
    int minDist = Integer.MAX_VALUE;// min dist from spatial node to src node
    while (iterator.hasNext()) {
      Entry<String, Set<Node>> entry = iterator.next();
      String propertyName = entry.getKey();
      for (Node node : entry.getValue()) {
        if (Neo4jGraphUtility.isNodeSpatial(node)) {
          minDist = Math.min(RisoTreeUtil.getHopNumber(propertyName), minDist);
          long start2 = System.currentTimeMillis();
          Node leafNode = RTreeUtility.getParentLeafNode(node);
          getRTreeLeafNodeTime += System.currentTimeMillis() - start2;

          start2 = System.currentTimeMillis();
          updateLeafNodePN(leafNode, propertyName, pathNeighborsTrgSortedIds);
          updateLeafNodePNTime += System.currentTimeMillis() - start2;
        }
      }
    }
    updatePNTimeTotal += System.currentTimeMillis() - start;
    if (safeNodesUsed) {
      updateSafeNodes(pathNeighborsTrgSortedIds, MAX_HOPNUM - minDist);
    }
  }

  /**
   * Remove a node from safeNodes if its distance to spatial nodes are within B.
   *
   * @param pathNeighborsTrgSortedIds
   * @param updateUpperBound
   */
  private void updateSafeNodes(Map<String, int[]> pathNeighborsTrgSortedIds, int updateUpperBound) {
    long start = System.currentTimeMillis();
    Iterator<Entry<String, int[]>> iterator = pathNeighborsTrgSortedIds.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, int[]> entry = iterator.next();
      if (RisoTreeUtil.getHopNumber(entry.getKey()) <= updateUpperBound) {
        for (int id : entry.getValue()) {
          safeNodes.remove((long) id);
        }
      }
    }
    updateSafeNodesTime += System.currentTimeMillis() - start;
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
    long start = System.currentTimeMillis();
    String leftPnName = MaintenanceUtil.getReversePnName(propertyName);
    int length = RisoTreeUtil.getHopNumber(leftPnName);
    for (String rightPnName : pathNeighborsTrg.keySet()) {
      int rightLength = RisoTreeUtil.getHopNumber(rightPnName);
      if (length + rightLength < MAX_HOPNUM) {// not equal because the inserted edge will count for
                                              // 1 more hop
        int[] nodesAdded = pathNeighborsTrg.get(rightPnName);
        String pnName = MaintenanceUtil.concatenatePnNames(leftPnName, rightPnName);
        updateLeafNodeSinglePn(leafNode, pnName, nodesAdded);
      }
    }
    long time = System.currentTimeMillis() - start;
    long id = leafNode.getId();

    updateLeafNodeTimeMap.put(id, updateLeafNodeTimeMap.getOrDefault(id, (long) 0) + time);
  }

  /**
   * Update the {@code leafNode} of property {@code pnName} with node ids to be added.
   *
   * @param leafNode
   * @param pnName
   * @param nodesAdded
   */
  private void updateLeafNodeSinglePn(Node leafNode, String pnName, int[] nodesAdded) {
    updatePNCount++;
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

  public void writeBackSafeNodes() throws Exception {
    FileWriter writer = Util.getFileWriter(safeNodesPath);
    for (long id : safeNodes) {
      writer.write(id + "\n");
    }
    Util.close(writer);
  }
}
