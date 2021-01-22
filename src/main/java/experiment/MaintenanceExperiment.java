package experiment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import commons.Edge;
import commons.Enums.MaintenanceStatistic;
import commons.GraphUtil;
import commons.Neo4jGraphUtility;
import commons.ReadWriteUtil;
import commons.Util;
import graph.MaintenanceUtil;
import graph.RisoTreeMaintenance;

public class MaintenanceExperiment {

  /**
   * Sample the graph_property_edge.txt file. The edges between two nodes with different directions
   * are treated as the same in the sample. As a result, the sampled edges will not be between the
   * same two nodes. A map whose key is the edge (always from id min to id max) will be used to
   * achieve this.
   *
   * @param inputPath
   * @param ratio
   * @param outputPath
   * @throws Exception
   */
  public static void sampleFile(String inputPath, double ratio, String outputPath)
      throws Exception {
    List<String> lines = ReadWriteUtil.readFileAllLines(inputPath);
    ArrayList<String> linesArray = new ArrayList<>(lines);
    int len = linesArray.size();
    int count = (int) (ratio * len);

    Map<String, String> sampleMap = new HashMap<>();
    Random random = new Random();
    while (sampleMap.size() < count) {
      int id = random.nextInt(len);
      String line = linesArray.get(id);
      int[] startEnd = GraphUtil.getEdgeStartEnd(line);
      int start = startEnd[0];
      int end = startEnd[1];
      int min = Math.min(start, end);
      int max = Math.max(start, end);
      String key = new Edge(min, max).toString();
      if (sampleMap.containsKey(key)) {
        continue;
      }
      sampleMap.put(key, line);
    }

    ArrayList<String> sampleLines = new ArrayList<>(sampleMap.values());
    ReadWriteUtil.WriteFile(outputPath, false, sampleLines);
  }

  /**
   * Generate random edge without considering current edges. (totally random)
   *
   * @param graphPath
   * @param count
   * @param outputPath
   */
  public static void generateRandomAddEdge(String graphPath, int count, String outputPath) {
    ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graphPath);
    ArrayList<TreeSet<Integer>> treeGraph = GraphUtil.convertListGraphToTreeSetGraph(graph);
    int len = graph.size();
    Map<String, String> sampleMap = new HashMap<>();
    Random random = new Random();
    while (sampleMap.size() < count) {
      int start = random.nextInt(len);
      int end = random.nextInt(len);
      int min = Math.min(start, end);
      int max = Math.max(start, end);
      String key = new Edge(min, max).toString();
      if (sampleMap.containsKey(key) || treeGraph.get(start).contains(end)) {
        continue;
      }
      sampleMap.put(key, key);
    }

    ArrayList<String> sampleLines = new ArrayList<>(sampleMap.values());
    ReadWriteUtil.WriteFile(outputPath, false, sampleLines);
  }

  /**
   * Generate added edge only for safeNodes.
   *
   * @param graphPath
   * @param count
   * @param outputPath
   * @throws Exception
   */
  public static void generateRandomAddEdgeSafeNodes(String graphPath, String safeNodesPath,
      int count, String outputPath) throws Exception {
    ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graphPath);
    ArrayList<TreeSet<Integer>> treeGraph = GraphUtil.convertListGraphToTreeSetGraph(graph);
    List<String> arrayList = ReadWriteUtil.readFileAllLines(safeNodesPath);
    List<Integer> safeNodes = new ArrayList<>(arrayList.size());
    for (String id : arrayList) {
      safeNodes.add(Integer.parseInt(id));
    }
    int len = safeNodes.size();
    Map<String, String> sampleMap = new HashMap<>();
    Random random = new Random();
    while (sampleMap.size() < count) {
      int start = safeNodes.get(random.nextInt(len));
      int end = safeNodes.get(random.nextInt(len));
      int min = Math.min(start, end);
      int max = Math.max(start, end);
      String key = new Edge(min, max).toString();
      if (sampleMap.containsKey(key) || treeGraph.get(start).contains(end)) {
        continue;
      }
      sampleMap.put(key, key);
    }

    ArrayList<String> sampleLines = new ArrayList<>(sampleMap.values());
    ReadWriteUtil.WriteFile(outputPath, false, sampleLines);
  }

  /**
   * Generate the correct graph.txt file for the new graph whose edges are removed.
   *
   * @param inputGraphPath
   * @param edgePath
   * @param outputGraphPath
   * @throws Exception
   */
  public static void removeEdgesFromGraphFile(String inputGraphPath, String edgePath,
      String outputGraphPath) throws Exception {
    Util.checkPathExist(inputGraphPath);
    Util.checkPathExist(edgePath);

    ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(inputGraphPath);
    List<String> lines = ReadWriteUtil.readFileAllLines(edgePath);
    ArrayList<TreeSet<Integer>> graphTreeSet = GraphUtil.convertListGraphToTreeSetGraph(graph);
    int notFound1 = 0, notFound2 = 0;
    for (String line : lines) {
      int[] startEnd = GraphUtil.getEdgeStartEnd(line);
      int start = startEnd[0];
      int end = startEnd[1];
      if (!graphTreeSet.get(start).remove((end))) {
        // This can happen when there exists an edge from a node to itself.
        // throw new RuntimeException(String.format("Edge %s is not found!", line));
        Util.println(line);
        notFound1++;
      }
      if (!graphTreeSet.get(end).remove(start)) {
        // This can happen when there exists an edge from a node to itself.
        // throw new RuntimeException(String.format("Edge %s is not found!", line));
        Util.println(line);
        notFound2++;
      }
    }
    Util.println(String.format("not found in graph.txt: %d\t%d", notFound1, notFound2));
    GraphUtil.writeGraphTreeSet(graphTreeSet, outputGraphPath);
  }

  /**
   * Remove the tested edges from the original graph database.
   *
   * @param dbPath
   * @param edgePath
   * @throws Exception
   */
  public static void removeEdgesFromDb(String dbPath, String edgePath) throws Exception {
    Util.checkPathExist(edgePath);
    GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(dbPath);
    Transaction tx = service.beginTx();
    List<String> lines = ReadWriteUtil.readFileAllLines(edgePath);
    int notFound = 0;
    for (String line : lines) {
      int[] startEnd = GraphUtil.getEdgeStartEnd(line);
      int start = startEnd[0];
      int end = startEnd[1];
      boolean found = removeEdge(service, start, end) || removeEdge(service, end, start);
      if (!found) {
        Util.println(String.format("Edge %s is not found!", line));
        notFound++;
      }
    }
    Util.println("not found in db: " + notFound);
    tx.success();
    tx.close();
    service.shutdown();
  }

  /**
   * Remove all the edges from start to end.
   *
   * @param service
   * @param start
   * @param end
   * @return whether such an edge exists or not
   */
  public static boolean removeEdge(GraphDatabaseService service, long start, long end) {
    boolean exist = false;
    Node startNode = service.getNodeById(start);
    Iterable<Relationship> rels = startNode.getRelationships(Direction.OUTGOING);
    for (Relationship relationship : rels) {
      if (relationship.getEndNode().getId() == end) {
        relationship.delete();
        exist = true;
      }
    }
    return exist;
  }

  public static boolean removeEdge(GraphDatabaseService service, long start, long end,
      String edgeType) {
    Node startNode = service.getNodeById(start);
    Iterable<Relationship> rels =
        startNode.getRelationships(RelationshipType.withName(edgeType), Direction.OUTGOING);
    for (Relationship relationship : rels) {
      if (relationship.getEndNode().getId() == end) {
        relationship.delete();
        return true;
      }
    }
    return false;
  }

  /**
   * 
   * @param dbPath
   * @param MAX_HOPNUM
   * @param maxPNSize
   * @param edgePath
   * @param testCount
   * @param safeNodesPath
   * @param outputPath the directory
   * @throws Exception
   */
  public static void addEdgeExperiment(String dbPath, int MAX_HOPNUM, int maxPNSize,
      String edgePath, int testCount, String safeNodesPath, String outputPath) throws Exception {
    boolean safeNodesUsed = safeNodesPath.isEmpty() ? false : true;
    GraphDatabaseService databaseService = Neo4jGraphUtility.getDatabaseService(dbPath);
    RisoTreeMaintenance maintenance = new RisoTreeMaintenance(databaseService, MAX_HOPNUM,
        maxPNSize, safeNodesUsed, safeNodesPath);
    int safeCount = maintenance.safeNodes.size();
    List<Edge> edges = GraphUtil.readEdges(edgePath);
    List<Edge> testEdges = edges.subList(0, testCount);
    List<Map<MaintenanceStatistic, Object>> records = new ArrayList<>(testCount);

    // Write summary info
    String outputDetailPath = outputPath + "_details.csv";
    String outputAvgPath = outputPath + "_avg.csv";
    Util.println("output detail path: " + outputDetailPath);
    Util.println("output avg path: " + outputAvgPath);

    ReadWriteUtil.WriteFile(outputAvgPath, true,
        String.format("%s\t%s\t%d\n", dbPath, edgePath, testCount));
    ReadWriteUtil.WriteFile(outputDetailPath, true,
        String.format("%s\t%s\t%d\n", dbPath, edgePath, testCount));

    // Write header
    String header = getHeaderString();
    ReadWriteUtil.WriteFile(outputAvgPath, true, header + "\n");
    ReadWriteUtil.WriteFile(outputDetailPath, true,
        String.format("id   startID  endID  %s\n", header));

    Transaction tx = databaseService.beginTx();
    Util.println("test edges count: " + testEdges.size());
    int id = 0;
    for (Edge edge : testEdges) {
      Util.println(edge);
      maintenance.addEdge(edge.start, edge.end);
      records.add(maintenance.getMaintenanceStatisticMap());
      ReadWriteUtil.WriteFile(outputDetailPath, true,
          getDetailOutputLine(id, edge, maintenance) + "\n");
      id++;
    }
    long start = System.currentTimeMillis();
    tx.success();
    tx.close();
    long commitTime = System.currentTimeMillis() - start;

    String outString = getAverageOutput(records);
    outString += "commitTime: " + commitTime + "\n";
    outString += "safeCaseHappenCount: " + maintenance.safeCaseHappenCount + "\n";
    outString += "safe count before add: " + safeCount + "\n";
    outString += "safe count after add: " + maintenance.safeNodes.size() + "\n";
    outString += "update PN count: " + maintenance.updatePNCount + "\n";
    outString += "updateLeafNodeTimeMap: " + maintenance.updateLeafNodeTimeMap + "\n";
    ReadWriteUtil.WriteFile(outputAvgPath, true, outString);
    if (safeNodesUsed) {
      maintenance.writeBackSafeNodes();
    }
    databaseService.shutdown();
  }

  private static String getHeaderString() {
    List<MaintenanceStatistic> maintenanceStatistics = MaintenanceUtil.getMaintenanceStatistic();
    List<String> strings = new LinkedList<>();
    for (MaintenanceStatistic maintenanceStatistic : maintenanceStatistics) {
      strings.add(maintenanceStatistic.name());
    }
    return String.join("\t", strings);
  }

  private static String getDetailOutputLine(int id, Edge edge, RisoTreeMaintenance maintenance)
      throws Exception {
    String line = String.valueOf(id);
    line += "\t" + edge.start + "\t" + edge.end;
    
    Map<MaintenanceStatistic, Object> maintenanceStatisticMap = maintenance.getMaintenanceStatisticMap();
    for(MaintenanceStatistic maintenanceStatistic : MaintenanceUtil.getMaintenanceStatistic()) {
      if (maintenanceStatisticMap.containsKey(maintenanceStatistic)) {
        throw new Exception(maintenanceStatistic + " does not exist in maintenanceStatisticMap!");
      }
      line += "\t" + maintenanceStatisticMap.get(maintenanceStatistic);
    }

    return line;
  }

  private static String getAverageOutput(List<Map<MaintenanceStatistic, Object>> records)
      throws Exception {
    String result = "";
    for(MaintenanceStatistic maintenanceStatistic : MaintenanceUtil.getMaintenanceStatistic()) {
      result += maintenanceStatistic.name() + ": "
          + MaintenanceUtil.getAverage(records, maintenanceStatistic) + "\n";
    }
    return result;
  }
}
