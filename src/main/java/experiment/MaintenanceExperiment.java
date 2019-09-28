package experiment;

import java.util.ArrayList;
import java.util.List;
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
import commons.OwnMethods;
import commons.ReadWriteUtil;
import commons.Util;
import graph.RisoTreeMaintenance;

public class MaintenanceExperiment {

  public static void sampleFile(String inputPath, double ratio, String outputPath)
      throws Exception {
    List<String> lines = ReadWriteUtil.readFileAllLines(inputPath);
    ArrayList<String> linesArray = new ArrayList<>(lines);
    int count = (int) (ratio * linesArray.size());
    ArrayList<String> sampleLines = OwnMethods.GetRandom_NoDuplicate(linesArray, count);
    ReadWriteUtil.WriteFile(outputPath, false, sampleLines);
  }

  public static void removeEdgesFromGraphFile(String inputGraphPath, String edgePath,
      String outputGraphPath) throws Exception {
    ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(inputGraphPath);
    List<String> lines = ReadWriteUtil.readFileAllLines(edgePath);
    ArrayList<TreeSet<Integer>> graphTreeSet = GraphUtil.convertListGraphToTreeSetGraph(graph);
    int notFound1 = 0, notFound2 = 0;
    for (String line : lines) {
      String[] strings = line.split(",");
      int start = Integer.parseInt(strings[0]);
      int end = Integer.parseInt(strings[2]);
      if (!graphTreeSet.get(start).remove((end))) {
        // throw new RuntimeException(String.format("Edge %s is not found!", line));
        Util.println(line);
        notFound1++;
      }
      if (!graphTreeSet.get(end).remove(start)) {
        // throw new RuntimeException(String.format("Edge %s is not found!", line));
        Util.println(line);
        notFound2++;
      }
    }
    Util.println(String.format("not found in graph.txt: %d\t%d", notFound1, notFound2));
    GraphUtil.writeGraphTreeSet(graphTreeSet, outputGraphPath);
  }

  public static void removeEdgesFromDb(String dbPath, String edgePath) throws Exception {
    GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(dbPath);
    Transaction tx = service.beginTx();
    List<String> lines = ReadWriteUtil.readFileAllLines(edgePath);
    int notFound = 0;
    for (String line : lines) {
      String[] strings = line.split(",");
      int start = Integer.parseInt(strings[0]);
      int end = Integer.parseInt(strings[2]);
      String edgeType = strings[1];
      boolean found = removeEdge(service, start, end, edgeType);
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

  public static void AddEdgeExperiment(String dbPath, int MAX_HOPNUM, int maxPNSize,
      String edgePath, int testCount, Boolean safeNodesUsed, String safeNodesPath,
      String outputPath) throws Exception {
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
      ReadWriteUtil.WriteFile(outputDetailPath, true, "" + time);
    }

  }

}
