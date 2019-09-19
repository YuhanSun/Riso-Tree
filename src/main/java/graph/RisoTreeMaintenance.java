package graph;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import commons.Neo4jGraphUtility;
import commons.ReadWriteUtil;

public class RisoTreeMaintenance {
  int MAX_HOPNUM;
  GraphDatabaseService databaseService;
  /**
   * The vertices that within B-hop from any spatial vertexes. Determine whether the RisoTree update
   * is required.
   */
  Set<Long> checkList;

  public RisoTreeMaintenance(Set<Long> checkList, int MAX_HOPNUM,
      GraphDatabaseService databaseService) {
    this.checkList = checkList;
    this.MAX_HOPNUM = MAX_HOPNUM;
    this.databaseService = databaseService;
  }

  public RisoTreeMaintenance(String checkListPath, int MAX_HOPNUM, String dbPath) throws Exception {
    List<String> arrayList = ReadWriteUtil.readFileAllLines(checkListPath);
    Set<Long> checkList = new HashSet<>();
    for (String id : arrayList) {
      checkList.add(Long.parseLong(id));
    }
    GraphDatabaseService databaseService = Neo4jGraphUtility.getDatabaseService(dbPath);
    this.checkList = checkList;
    this.MAX_HOPNUM = MAX_HOPNUM;
    this.databaseService = databaseService;
  }

  public Relationship addEdge(Node start, Node end) {
    return null;
  }
}
