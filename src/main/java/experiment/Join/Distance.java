package experiment.Join;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import commons.Config;
import commons.Entity;
import commons.MyRectangle;
import commons.OwnMethods;
import commons.Query_Graph;
import commons.Util;
import commons.Config.Datasets;
import commons.Config.system;
import graph.Naive_Neo4j_Match;
import graph.RisoTreeQueryPN;
import graph.SpatialFirst_List;

public class Distance {

  public Config config;
  public String dataset;
  public String version;
  public system systemName;
  public String password;
  public int MAX_HOPNUM;

  public String db_path;
  public String graph_pos_map_path;
  public String entityPath;

  public String querygraphDir;
  // public String spaPredicateDir;
  public String resultDir;
  // public String queryrectCenterPath = null;
  // public ArrayList<Entity> entities = null;
  public ArrayList<MyRectangle> queryrect = null;
  public ArrayList<Query_Graph> queryGraphs = null;

  public int entityCount, spaCount;

  public boolean TEST_FORMAT;
  public int nodeCount = 5;

  public Distance() {
    config = new Config();
    initializeParameters();
  }

  public void initializeParameters() {
    TEST_FORMAT = false;
    dataset = config.getDatasetName();
    version = config.GetNeo4jVersion();
    systemName = config.getSystemName();
    password = config.getPassword();
    MAX_HOPNUM = config.getMaxHopNum();
    switch (systemName) {
      case Ubuntu:
        db_path = String.format(
            "/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version,
            dataset);
        graph_pos_map_path =
            "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/node_map_RTree.txt";
        entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
        querygraphDir =
            String.format("/mnt/hgfs/Google_Drive/Projects/risotree/query/query_graph/%s", dataset);
        // spaPredicateDir =
        // String.format("/mnt/hgfs/Google_Drive/Projects/risotree/query/spa_predicate/%s",
        // dataset);
        // queryrectCenterPath = String.format("%s/%s_centerids.txt", spaPredicateDir, dataset);
        resultDir =
            String.format("/mnt/hgfs/Google_Drive/Experiment_Result/Riso-Tree/Join/%s", dataset);
        // resultDir =
        // String.format("/mnt/hgfs/Google_Drive/Experiment_Result/Riso-Tree/%s/switch_point",
        // dataset);
        break;
      case Windows:
        db_path = String.format(
            "D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset,
            version, dataset);
        graph_pos_map_path =
            "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset + "\\node_map_RTree.txt";
        entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
        querygraphDir =
            String.format("D:\\Google_Drive\\Projects\\risotree\\query\\query_graph\\%s", dataset);
        // spaPredicateDir =
        // String.format("D:\\Google_Drive\\Projects\\risotree\\query\\spa_predicate\\%s", dataset);
        // queryrectCenterPath = String.format("%s/%s_centerids.txt", spaPredicateDir,
        // dataset.split("_")[0]);
        resultDir =
            String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\Join\\%s", dataset);
        break;
    }

    String querygraph_path = String.format("%s/%d.txt", querygraphDir, nodeCount);
    Util.println(querygraph_path);
    queryGraphs = Util.ReadQueryGraph_Spa(querygraph_path, 5);

    // entities = OwnMethods.ReadEntity(entityPath);
    // ArrayList<Integer> ids = OwnMethods.readIntegerArray(queryrectCenterPath);
    // queryrect = new ArrayList<MyRectangle>();
    // for ( int id :ids)
    // {
    // Entity entity = entities.get(id);
    // queryrect.add(new MyRectangle(entity.lon, entity.lat, entity.lon, entity.lat));
    // }

    spaCount = OwnMethods.GetSpatialEntityCount(entityPath);
    entityCount = OwnMethods.getEntityCount(entityPath);

  }

  public static void main(String[] args) {
    ArrayList<Double> distanceList = new ArrayList<>();
    // Patents dataset
    // distanceList.add(0.001);
    // distanceList.add(0.0002);
    // distanceList.add(0.0004);
    // distanceList.add(0.0008);

    // wikidata_100
    // distanceList.add(0.0000000001);
    // distanceList.add(0.000000001);
    distanceList.add(0.00000001);
    // distanceList.add(0.0000001);
    // distanceList.add(0.000001);
    try {
      // String dataset = Datasets.Patents_100_random_80.name();
      String dataset = Datasets.wikidata_100.name();
      Distance distanceExperiment = new Distance();
      distanceExperiment.config.setDatasetName(dataset);
      distanceExperiment.initializeParameters();
      // distanceExperiment.risoTreeQueryPN(distanceList, 0);
      // distanceExperiment.spatialFirstList(distanceList, 0);
      // distanceExperiment.Neo4j_Naive(distanceList, 0);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * use spatial first list approach
   * 
   * @param nodeCount
   * @param query_id
   * @throws Exception
   */
  public void spatialFirstList(ArrayList<Double> distanceList, int query_id) {
    try {
      long start;
      long time;

      Util.println("read map from " + graph_pos_map_path);
      long[] graph_pos_map_list = OwnMethods.ReadMap(graph_pos_map_path, entityCount);

      Query_Graph query_Graph = queryGraphs.get(query_id);
      OwnMethods.convertQueryGraphForJoin(query_Graph);
      Util.println(query_Graph);

      String result_avg_path = null;
      switch (systemName) {
        case Ubuntu:
          result_avg_path = String.format("%s/distance_spatialFirst_%d_%d_avg.txt", resultDir,
              nodeCount, query_id);
          // result_avg_path = String.format("%s/risotree_%d_%d_avg_test.txt", resultDir, nodeCount,
          // query_id);
          break;
        case Windows:
          result_avg_path =
              String.format("%s\\distance_risotree_%d_%d_avg.txt", resultDir, nodeCount, query_id);
          break;
      }

      Util.println(result_avg_path);
      String write_line = String.format("%s\t%d\n", dataset, query_id);
      if (!TEST_FORMAT)
        OwnMethods.WriteFile(result_avg_path, true, write_line);

      String head_line =
          "result_count\tjoin_count\tjoin_time\tget_iterator_time\titerate_time\ttotal_time\taccess_pages\n";
      if (!TEST_FORMAT)
        OwnMethods.WriteFile(result_avg_path, true, "distance\t" + head_line);

      for (double distance : distanceList) {
        Util.println("\n" + String.valueOf(distance));

        if (!TEST_FORMAT) {
          OwnMethods.ClearCache(password);
          Thread.currentThread();
          Thread.sleep(5000);
          Util.println(String.format(
              "initialize SpatialFirst_List with dbpath: %s\n dataset: %s\n", db_path, dataset));
          SpatialFirst_List spatialFirstList =
              new SpatialFirst_List(db_path, dataset, graph_pos_map_list);

          start = System.currentTimeMillis();
          spatialFirstList.LAGAQ_Join(query_Graph, distance);
          time = System.currentTimeMillis() - start;

          write_line = String.valueOf(distance) + "\t";
          write_line += String.format("%d\t", spatialFirstList.result_count);
          write_line += String.format("%d\t%d\t", spatialFirstList.join_result_count,
              spatialFirstList.join_time);
          write_line += String.format("%d\t", spatialFirstList.get_iterator_time);
          write_line += String.format("%d\t%d\t", spatialFirstList.get_iterator_time, time);
          write_line += String.format("%d\n", spatialFirstList.page_hit_count);
          if (!TEST_FORMAT)
            OwnMethods.WriteFile(result_avg_path, true, write_line);

          spatialFirstList.dbservice.shutdown();
        }
      }
      OwnMethods.WriteFile(result_avg_path, true, "\n");
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * use RisoTree and graph first approach
   * 
   * @param nodeCount
   * @param query_id
   * @throws Exception
   */
  public void risoTreeQueryPN(ArrayList<Double> distanceList, int query_id) {
    try {
      long start;
      long time;

      Util.println("read map from " + graph_pos_map_path);
      long[] graph_pos_map_list = OwnMethods.ReadMap(graph_pos_map_path, entityCount);

      Query_Graph query_Graph = queryGraphs.get(query_id);
      OwnMethods.convertQueryGraphForJoin(query_Graph);
      Util.println(query_Graph);

      String result_avg_path = null;
      switch (systemName) {
        case Ubuntu:
          result_avg_path = String.format("%s/distance_risotree_PN%d_%d_%d_avg.txt", resultDir,
              MAX_HOPNUM, nodeCount, query_id);
          // result_avg_path = String.format("%s/risotree_%d_%d_avg_test.txt", resultDir, nodeCount,
          // query_id);
          break;
        case Windows:
          result_avg_path = String.format("%s\\distance_risotree_PN_%d_%d_avg.txt", resultDir,
              nodeCount, query_id);
          break;
      }

      String write_line = String.format("%s\t%d\n", dataset, query_id);

      String head_line =
          "result_count\tjoin_count\tjoin_time\tcheck_path_time\t_check_overlap_time\tget_iterator_time\titerate_time\ttotal_time\taccess_pages\n";
      if (!TEST_FORMAT)
        OwnMethods.WriteFile(result_avg_path, true, "distance\t" + head_line);

      for (double distance : distanceList) {
        write_line = String.valueOf(distance) + "\n" + head_line;

        if (!TEST_FORMAT) {
          OwnMethods.ClearCache(password);
          Thread.currentThread();
          Thread.sleep(5000);
          Util.println(String.format(
              "initialize RisoTreeQueryPN with " + "dbpath: %s\n dataset: %s\n MAX_HOPNUM: %d",
              db_path, dataset, MAX_HOPNUM));
          RisoTreeQueryPN risoTreeQueryPN =
              new RisoTreeQueryPN(db_path, dataset, graph_pos_map_list, MAX_HOPNUM);

          start = System.currentTimeMillis();
          risoTreeQueryPN.LAGAQ_Join(query_Graph, distance);
          time = System.currentTimeMillis() - start;

          write_line = String.valueOf(distance) + "\t";
          write_line += String.format("%d\t", risoTreeQueryPN.result_count);
          write_line += String.format("%d\t%d\t", risoTreeQueryPN.join_result_count,
              risoTreeQueryPN.join_time);
          write_line += String.format("%d\t%d\t", risoTreeQueryPN.check_paths_time,
              risoTreeQueryPN.check_overlap_time);
          write_line += String.format("%d\t", risoTreeQueryPN.get_iterator_time);
          write_line += String.format("%d\t%d\t", risoTreeQueryPN.get_iterator_time, time);
          write_line += String.format("%d\n", risoTreeQueryPN.page_hit_count);
          if (!TEST_FORMAT)
            OwnMethods.WriteFile(result_avg_path, true, write_line);

          risoTreeQueryPN.dbservice.shutdown();
        }
      }
      OwnMethods.WriteFile(result_avg_path, true, "\n");
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public void Neo4j_Naive(ArrayList<Double> distanceList, int query_id) {
    try {
      long start;
      long time;

      Query_Graph query_Graph = queryGraphs.get(query_id);
      OwnMethods.convertQueryGraphForJoin(query_Graph);
      Util.println(query_Graph);

      String result_detail_path = null, result_avg_path = null;
      switch (systemName) {
        case Ubuntu:
          result_detail_path =
              String.format("%s/neo4j_naive_%d_%d.txt", resultDir, nodeCount, query_id);
          result_avg_path =
              String.format("%s/neo4j_naive_%d_%d_avg.txt", resultDir, nodeCount, query_id);
          break;
        case Windows:
          result_detail_path =
              String.format("%s\\neo4j_naive_%d_%d.txt", resultDir, nodeCount, query_id);
          result_avg_path =
              String.format("%s\\risotree_%d_%d_avg.txt", resultDir, nodeCount, query_id);
          break;
      }

      String write_line = String.format("%s\n", dataset);
      if (!TEST_FORMAT) {
        OwnMethods.WriteFile(result_detail_path, true, write_line);
        OwnMethods.WriteFile(result_avg_path, true, write_line);
      }

      String head_line = "count\tget_iterator_time\ttotal_time\taccess_pages\n";
      if (!TEST_FORMAT)
        OwnMethods.WriteFile(result_avg_path, true, "distance\t" + head_line);

      for (double distance : distanceList) {
        write_line = String.valueOf(distance) + "\n" + head_line;
        if (!TEST_FORMAT)
          OwnMethods.WriteFile(result_detail_path, true, write_line);

        Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match(db_path);

        if (!TEST_FORMAT) {
          OwnMethods.ClearCache(password);
          Thread.currentThread();
          Thread.sleep(5000);
          start = System.currentTimeMillis();
          naive_Neo4j_Match.LAGAQ_Join(query_Graph, distance);
          time = System.currentTimeMillis() - start;

          write_line = String.valueOf(distance) + "\t";
          write_line += String.format("%d\t", naive_Neo4j_Match.result_count);
          write_line += String.format("%d\t", naive_Neo4j_Match.get_iterator_time);
          write_line += String.format("%d\t%d\t", naive_Neo4j_Match.iterate_time, time);
          write_line += String.format("%d\n", naive_Neo4j_Match.page_access);
          if (!TEST_FORMAT)
            OwnMethods.WriteFile(result_avg_path, true, write_line);

          naive_Neo4j_Match.neo4j_API.ShutDown();
        }
      }
      OwnMethods.WriteFile(result_detail_path, true, "\n");
      OwnMethods.WriteFile(result_avg_path, true, "\n");
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

}
