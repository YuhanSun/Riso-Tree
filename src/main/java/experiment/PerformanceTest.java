package experiment;

import java.io.File;
import java.util.Map;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import commons.Config;
import commons.Config.system;
import commons.OwnMethods;
import commons.Util;

public class PerformanceTest {

  static Config config = new Config();
  static String dataset = config.getDatasetName();
  static String version = config.GetNeo4jVersion();
  static system systemName = config.getSystemName();
  static String password = config.getPassword();

  static String db_path;
  static String querygraphDir;
  static String graph_pos_map_path;
  static String entityPath;

  public static void initializeParameters() {
    switch (systemName) {
      case Ubuntu:
        db_path = String.format(
            "/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version,
            dataset);
        querygraphDir =
            String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph/%s/", dataset);
        graph_pos_map_path =
            "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/node_map_RTree.txt";
        entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
        break;
      case Windows:
        db_path = String.format(
            "D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset,
            version, dataset);
        querygraphDir =
            String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\query_graph\\%s\\", dataset);
        graph_pos_map_path =
            "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset + "\\node_map_RTree.txt";
        entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
        break;
    }
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    initializeParameters();
    // getLabel();
    // patternMatch();
    // profileQuery();
    profilePredicate();
  }

  public static void profilePredicate() {
    try {
      GraphDatabaseService databaseService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = databaseService.beginTx();
      // String query = "profile match (a1:GRAPH_1) where 856.877643 <= a1.lon <= 857.751571 "
      //// + "and 886.156102 <= a1.lat <= 887.030030 return a1";
      // + "return a1";

      String query = "profile match (a1:GRAPH_1) where 856.877643 <= a1.lon "
          // + "and 886.156102 <= a1.lat <= 887.030030 return a1";
          + "return a1";

      long start = System.currentTimeMillis();
      Result result = databaseService.execute(query);
      long getIteratorTime = System.currentTimeMillis() - start;
      long start1 = System.currentTimeMillis();
      float count = 0;
      while (result.hasNext()) {
        Map<String, Object> row = result.next();
        count++;
      }
      long iteratorTime = System.currentTimeMillis() - start1;

      long time = System.currentTimeMillis() - start;
      Util.println(String.format("%d %d %d", getIteratorTime, iteratorTime, time));

      ExecutionPlanDescription description = result.getExecutionPlanDescription();
      Util.println(description);

      tx.success();
      tx.close();
      databaseService.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void profileQuery() {
    try {
      // for ( int loopIndex = 0; loopIndex < 5; loopIndex++)
      {
        GraphDatabaseService databaseService =
            new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
        Transaction tx = databaseService.beginTx();
        // String query = "profile match (a1:GRAPH_1)--(a2:GRAPH_1) where 856.877643 <= a1.lon <=
        // 857.751571 "
        // + "and 886.156102 <= a1.lat <= 887.030030 return id(a1), id(a2)";
        // String query = "profile match (a1:GRAPH_1)--(a2:GRAPH_1) where 841.540442 <= a1.lon <=
        // 873.088772 "
        // + "and 870.818901 <= a1.lat <= 902.367231 return id(a1), id(a2)";

        // String query = "profile match (a1:GRAPH_1)--(a2:GRAPH_1), (a1:GRAPH_1)--(a3:GRAPH_1),
        // (a2:GRAPH_1)--(a3:GRAPH_1)"
        // + " where 856.877643 <= a1.lon <= 857.751571 "
        // + "and 886.156102 <= a1.lat <= 887.030030 return id(a1), id(a2), id(a3)";

        String query =
            "profile match (a1:GRAPH_1), (a2:GRAPH_22), (a3:GRAPH_64), (a1)--(a2), (a1)--(a3), (a2)--(a3)"
                + " where 856.877643 <= a1.lon <= 857.751571 "
                + "and 886.156102 <= a1.lat <= 887.030030 return id(a1), id(a2), id(a3)";

        long start = System.currentTimeMillis();
        Result result = databaseService.execute(query);
        long getIteratorTime = System.currentTimeMillis() - start;
        long start1 = System.currentTimeMillis();
        float count = 0;
        while (result.hasNext()) {
          Map<String, Object> row = result.next();
          count++;
        }
        long iteratorTime = System.currentTimeMillis() - start1;

        long time = System.currentTimeMillis() - start;
        Util.println(String.format("%d %d %d", getIteratorTime, iteratorTime, time));

        ExecutionPlanDescription description = result.getExecutionPlanDescription();
        Util.println(description);

        tx.success();
        tx.close();
        databaseService.shutdown();
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static void patternMatch() {
    try {
      for (int loopIndex = 0; loopIndex < 5; loopIndex++) {
        OwnMethods.ClearCache(password);
        // Thread.sleep(15000);
        GraphDatabaseService databaseService =
            new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
        Transaction tx = databaseService.beginTx();
        String query = "match (a1:GRAPH_0)--(a2:GRAPH_1) where 856.877643 <= a1.lon <= 886.156102 "
            + "and 857.751571 <= a1.lat <= 887.030030 return id(a1), id(a2)";
        long start = System.currentTimeMillis();
        Result result = databaseService.execute(query);
        long getIteratorTime = System.currentTimeMillis() - start;
        long start1 = System.currentTimeMillis();
        float count = 0;
        while (result.hasNext()) {
          Map<String, Object> row = result.next();
          count++;
        }
        long iteratorTime = System.currentTimeMillis() - start1;

        long time = System.currentTimeMillis() - start;
        Util.println(String.format("%d %d %d", getIteratorTime, iteratorTime, time));
        tx.success();
        tx.close();
        databaseService.shutdown();
        OwnMethods.ClearCache(password);
        // Thread.sleep(15000);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static void getLabel() {
    try {
      // for ( int labelIndex = 1; labelIndex < 100; labelIndex += 10)
      // for ( int labelIndex = 1; labelIndex < 10; labelIndex += 1)
      for (int labelIndex = 0; labelIndex < 2; labelIndex += 1) {
        GraphDatabaseService databaseService =
            new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
        Transaction tx = databaseService.beginTx();
        Label label = DynamicLabel.label(String.format("GRAPH_%d", labelIndex));
        String query = String.format("match (n:%s) return id(n)", label);
        long start = System.currentTimeMillis();
        Result result = databaseService.execute(query);
        float count = 0;
        while (result.hasNext()) {
          Map<String, Object> row = result.next();
          long id = (Long) row.get("id(n)");
          count++;
        }
        long time = System.currentTimeMillis() - start;
        Util.println(String.format("%s:%d", label.toString(), time));
        // OwnMethods.Print(String.format("%s:%f", label.toString(), time / count));
        tx.success();
        tx.close();
        databaseService.shutdown();
        OwnMethods.ClearCache(password);
        Thread.sleep(5000);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
