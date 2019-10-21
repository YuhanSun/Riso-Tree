package experiment;

import java.util.ArrayList;
import java.util.HashMap;
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
import commons.GraphUtil;
import commons.Neo4jGraphUtility;
import commons.ReadWriteUtil;
import commons.Util;
import graph.RisoTreeMaintenance;

public class MaintenanceExperiment {

  public static int[] getStartEnd(String edgeString) {
    String[] strings = edgeString.split(",");
    int start = 0, end = 0;
    if (strings.length == 3) {
      start = Integer.parseInt(strings[0]);
      end = Integer.parseInt(strings[2]);
    } else if (strings.length == 2) {
      start = Integer.parseInt(strings[0]);
      end = Integer.parseInt(strings[1]);
    } else {
      throw new RuntimeException(edgeString + " is not edge-format!");
    }
    return new int[] {start, end};
  }

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
      int[] startEnd = getStartEnd(line);
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
      int[] startEnd = getStartEnd(line);
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
      int[] startEnd = getStartEnd(line);
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
    RisoTreeMaintenance maintenance =
        new RisoTreeMaintenance(dbPath, MAX_HOPNUM, maxPNSize, safeNodesUsed, safeNodesPath);
    List<Edge> edges = GraphUtil.readEdges(edgePath);
    List<Edge> testEdges = edges.subList(0, testCount);
    List<ResultRecord> records = new ArrayList<>(testCount);

    // Write header
    String outputDetailPath = outputPath + "_details.csv";
    String outputAvgPath = outputPath + "_avg.csv";
    ReadWriteUtil.WriteFile(outputAvgPath, true,
        String.format("%s\t%s\t%d\n", dbPath, edgePath, testCount));
    ReadWriteUtil.WriteFile(outputDetailPath, true,
        String.format("%s\t%s\t%d\n", dbPath, edgePath, testCount));

    for (Edge edge : testEdges) {
      long start = System.currentTimeMillis();
      maintenance.addEdge(edge.start, edge.end);
      long time = System.currentTimeMillis() - start;
      records.add(new ResultRecord(time, -1));
      ReadWriteUtil.WriteFile(outputDetailPath, true, "" + time + "\n");
    }
    ReadWriteUtil.WriteFile(outputAvgPath, true,
        "avg time: " + ResultRecord.getRunTimeAvg(records) + "\n");
    ReadWriteUtil.WriteFile(outputAvgPath, true,
        "safeCaseHappenCount: " + maintenance.safeCaseHappenCount + "\n\n");
  }

}
