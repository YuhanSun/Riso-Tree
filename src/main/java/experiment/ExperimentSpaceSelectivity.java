package experiment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import javax.sound.sampled.Line;
import javax.sound.sampled.LineListener;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import commons.Config;
import commons.MyRectangle;
import commons.OwnMethods;
import commons.Query_Graph;
import commons.Util;
import commons.Config.system;
import graph.Naive_Neo4j_Match;
import graph.Neo4j_API;
import graph.RisoTreeQueryPN;

public class ExperimentSpaceSelectivity {
  public static Config config = new Config();
  public static String dataset = config.getDatasetName();
  public static String version = config.GetNeo4jVersion();
  public static system systemName = config.getSystemName();
  public static String password = config.getPassword();
  public static int MAX_HOPNUM = config.getMaxHopNum();

  public static String db_path;
  public static String graph_pos_map_path;
  public static String entityPath;

  public static String queryDir, querygraphDir;
  public static String spaPredicateDir;
  public static String resultDir;

  public static boolean TEST_FORMAT;
  public static int experimentCount = 20;
  public int areaLevel = 4;
  public int area;
  public static int factor;

  public static int spaCount;

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
        queryDir = "/mnt/hgfs/Google_Drive/Projects/risotree/query";
        querygraphDir = String.format("%s/query_graph/%s", queryDir, dataset);
        spaPredicateDir = String.format("%s/spa_predicate/SpaceSelectivity/%s", queryDir, dataset);
        resultDir = String.format(
            "/mnt/hgfs/Google_Drive/Experiment_Result/Riso-Tree/SpaceSelectivity/%s", dataset);
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
        queryDir = "D:\\Google_Drive\\Projects\\risotree\\query";
        // querygraphDir = String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\query_graph\\%s",
        // dataset);
        // spaPredicateDir = String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\spa_predicate\\%s",
        // dataset);
        resultDir = String.format(
            "D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\SpaceSelectivity\\%s", dataset);
        break;
    }

    if (dataset.equals(Config.Datasets.Gowalla_100.name())
        || dataset.equals(Config.Datasets.foursquare_100.name())) {
      area = 1;
      factor = 50;
    } else {
      if (dataset.equals(Config.Datasets.go_uniprot_100_random_80.name()))
        area = 1;
      else {
        area = 10;
      }
      factor = 10;
    }
  }

  public ExperimentSpaceSelectivity(Config p_Config) {
    this.config = p_Config;
    initializeParameters();
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    // ExperimentSpaceSelectivity experimentSpaceSelectivity = new ExperimentSpaceSelectivity(new
    // Config());
    // experimentSpaceSelectivity.initializeParameters();
    // experimentSpaceSelectivity.generateRandomCenterLocation();
    // experimentSpaceSelectivity.generateQueryRectangleForSpaceSelectivityMeter();
    Query();
    //
  }

  public static void Query() {
    int nodeCount = 10, queryIndex = 0;

    ArrayList<String> dataset_a =
        new ArrayList<String>(Arrays.asList(Config.Datasets.Gowalla_100.name(),
            Config.Datasets.foursquare_100.name(), Config.Datasets.Patents_100_random_80.name(),
            Config.Datasets.go_uniprot_100_random_80.name()));

    for (String dataset : dataset_a)
    // String dataset = Config.Datasets.foursquare_100.name();
    {
      Config config = new Config();
      config.setDatasetName(dataset);

      ExperimentSpaceSelectivity experimentSpaceSelectivity =
          new ExperimentSpaceSelectivity(config);
      experimentSpaceSelectivity.experimentCount = 3;
      try {
        experimentSpaceSelectivity.Neo4j_Naive(nodeCount, queryIndex);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      // PN
      // for ( int hopNum = 0; hopNum <= 2; hopNum++)
      // {
      // if (dataset.equals(Config.Datasets.go_uniprot_100_random_80) && hopNum == 2)
      // continue;
      // config.setMAXHOPNUM(hopNum);
      // ExperimentSpaceSelectivity experimentSpaceSelectivity = new
      // ExperimentSpaceSelectivity(config);
      //// experimentSpaceSelectivity.area = 2500;
      //// experimentSpaceSelectivity.areaLevel = 2;
      // try {
      //// experimentSpaceSelectivity.risoTreeQueryPN(nodeCount, queryIndex);
      //
      // } catch (Exception e) {
      // // TODO Auto-generated catch block
      // e.printStackTrace(); System.exit(-1);
      // }
      // }

    }
  }

  public void generateRandomCenterLocation() {
    ArrayList<String> dataset_a =
        new ArrayList<String>(Arrays.asList(Config.Datasets.Patents_100_random_80.name(),
            Config.Datasets.go_uniprot_100_random_80.name(), Config.Datasets.Gowalla_100.name(),
            Config.Datasets.foursquare_100.name()));
    ArrayList<MyRectangle> totalRange_a = new ArrayList<MyRectangle>(
        Arrays.asList(new MyRectangle(0, 0, 1000, 1000), new MyRectangle(0, 0, 1000, 1000),
            new MyRectangle(-180, -90, 180, 90), new MyRectangle(-180, -90, 180, 90)));
    try {
      for (int i = 0; i < dataset_a.size(); i++) {
        String dataset = dataset_a.get(i);
        MyRectangle total_range = totalRange_a.get(i);
        double totalWidth = total_range.max_x - total_range.min_x;
        double totalHeight = total_range.max_y - total_range.min_y;
        int experiment_count = 500;
        Random random = new Random();
        String output_path = null;

        switch (systemName) {
          case Ubuntu:
            output_path = String.format("%s/spa_predicate/SpaceSelectivity/%s_centerpoint.txt",
                queryDir, dataset);
            break;
          case Windows:
            output_path = String.format("%s\\spa_predicate\\SpaceSelectivity\\%s_centerpoint.txt",
                queryDir, dataset);
            break;
        }
        String write_line = "";
        for (int k = 0; k < experiment_count; k++) {
          double centerX = random.nextDouble() * totalWidth + total_range.min_x;
          double centerY = random.nextDouble() * totalHeight + total_range.min_y;
          write_line += String.format("%f\t%f\n", centerX, centerY);
        }
        OwnMethods.WriteFile(output_path, true, write_line);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * Generate random position rectangles whose selectivity are based on their region area.
   * (Kilometer)
   */
  public void generateQueryRectangleForSpaceSelectivityMeter() {
    ArrayList<String> dataset_a =
        new ArrayList<String>(Arrays.asList(Config.Datasets.Patents_100_random_80.name(),
            Config.Datasets.go_uniprot_100_random_80.name(), Config.Datasets.Gowalla_100.name(),
            Config.Datasets.foursquare_100.name()));
    ArrayList<MyRectangle> totalRange_a = new ArrayList<MyRectangle>(
        Arrays.asList(new MyRectangle(0, 0, 1000, 1000), new MyRectangle(0, 0, 1000, 1000),
            new MyRectangle(-180, -90, 180, 90), new MyRectangle(-180, -90, 180, 90)));
    try {
      for (int i = 0; i < dataset_a.size(); i++) {
        String dataset = dataset_a.get(i);
        MyRectangle total_range = totalRange_a.get(i);

        if (dataset.equals(Config.Datasets.Gowalla_100.name())
            || dataset.equals(Config.Datasets.foursquare_100.name())) {
          area = 1;
          factor = 50;
        } else {
          if (dataset.equals(Config.Datasets.go_uniprot_100_random_80.name()))
            area = 1;
          else {
            area = 10;
          }
          factor = 10;
        }

        String centerPath = null;
        switch (systemName) {
          case Ubuntu:
            centerPath = String.format("%s/spa_predicate/SpaceSelectivity/%s_centerpoint.txt",
                queryDir, dataset);
            break;
          case Windows:
            centerPath = String.format("%s\\spa_predicate\\SpaceSelectivity\\%s_centerpoint.txt",
                queryDir, dataset);
            break;
        }

        ArrayList<Double> lonList = new ArrayList<Double>(experimentCount);
        ArrayList<Double> latList = new ArrayList<Double>(experimentCount);
        BufferedReader reader = new BufferedReader(new FileReader(new File(centerPath)));
        String line = null;
        while ((line = reader.readLine()) != null) {
          String[] lineList = line.split("\t");
          lonList.add(Double.parseDouble(lineList[0]));
          latList.add(Double.parseDouble(lineList[1]));
        }

        for (int j = 0; j < areaLevel; j++) {
          String output_path = null;

          switch (systemName) {
            case Ubuntu:
              output_path = String.format("%s/spa_predicate/SpaceSelectivity/%s_%d.txt", queryDir,
                  dataset, area);
              break;
            case Windows:
              output_path = String.format("%s\\spa_predicate\\SpaceSelectivity\\%s_%d.txt",
                  queryDir, dataset, area);
              break;
          }
          for (int k = 0; k < experimentCount; k++) {
            double centerX = lonList.get(k);
            double centerY = latList.get(k);
            String write_line = "";

            if (total_range.min_x < 0) {
              double sideLength = Math.sqrt((double) area);
              sideLength *= 1000;
              double deltaLongitude = Util.distToLongitude(sideLength);
              double deltaLatitude = Util.distToLatitude(sideLength, centerX, centerY);
              double offsetLon = deltaLongitude / 2.0;
              double offsetLat = deltaLatitude / 2.0;
              write_line = String.format("%f\t%f\t%f\t%f\n", centerX - offsetLon,
                  centerY - offsetLat, centerX + offsetLon, centerY + offsetLat);
            } else {
              double sideLength = Math.sqrt((double) area);
              double offset = sideLength / 2.0;
              write_line = String.format("%f\t%f\t%f\t%f\n", centerX - offset, centerY - offset,
                  centerX + offset, centerY + offset);
            }
            OwnMethods.WriteFile(output_path, true, write_line);
          }
          area *= factor;
        }
        reader.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * Generate random position rectangles whose selectivity are based on their region area. (Angle)
   */
  public static void generateQueryRectangleForSpaceSelectivityDegree() {
    ArrayList<String> dataset_a =
        new ArrayList<String>(Arrays.asList(Config.Datasets.Patents_100_random_80.name(),
            Config.Datasets.go_uniprot_100_random_80.name(), Config.Datasets.Gowalla_100.name(),
            Config.Datasets.foursquare_100.name()));
    ArrayList<MyRectangle> totalRange_a = new ArrayList<MyRectangle>(
        Arrays.asList(new MyRectangle(0, 0, 1000, 1000), new MyRectangle(0, 0, 1000, 1000),
            new MyRectangle(-180, -90, 180, 90), new MyRectangle(-180, -90, 180, 90)));

    double startSelectivity = 0.000001;
    double endSelectivity = 0.002;
    try {
      for (int i = 0; i < dataset_a.size(); i++) {
        String dataset = dataset_a.get(i);
        MyRectangle total_range = totalRange_a.get(i);
        int experiment_count = 500;
        Random random = new Random();
        double selectivity = startSelectivity;
        while (selectivity < endSelectivity) {
          double rect_size_x = (total_range.max_x - total_range.min_x) * Math.sqrt(selectivity);
          double rect_size_y = (total_range.max_y - total_range.min_y) * Math.sqrt(selectivity);

          String output_path = null;

          switch (systemName) {
            case Ubuntu:
              output_path = String.format("%s/spa_predicate/SpaceSelectivity/%s_%.8f.txt", queryDir,
                  dataset, selectivity);
              break;
            case Windows:
              output_path = String.format("%s\\spa_predicate\\SpaceSelectivity\\%s_%.8f.txt",
                  queryDir, dataset, selectivity);
              break;
          }

          Util.println("Write query rectangles to " + output_path);
          String write_line = "";
          for (int j = 0; j < experiment_count; j++) {
            MyRectangle rectangle =
                OwnMethods.GenerateQueryRectangle(random, rect_size_x, rect_size_y, total_range);
            write_line = String.format("%f\t%f\t%f\t%f\n", rectangle.min_x, rectangle.min_y,
                rectangle.min_y, rectangle.max_y);
            OwnMethods.WriteFile(output_path, true, write_line);
          }
          selectivity *= 10;
        }

      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public void Neo4j_Naive(int nodeCount, int query_id) throws Exception {
    try {
      long start;
      long time;
      int limit = -1;

      String querygraph_path = String.format("%s/%d.txt", querygraphDir, nodeCount);
      ArrayList<Query_Graph> queryGraphs =
          Util.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
      Query_Graph query_Graph = queryGraphs.get(query_id);

      String result_detail_path = null, result_avg_path = null;
      switch (systemName) {
        case Ubuntu:
          result_detail_path =
              String.format("%s/neo4j_%d_%d_API.txt", resultDir, nodeCount, query_id);
          result_avg_path =
              String.format("%s/neo4j_%d_%d_API_avg.txt", resultDir, nodeCount, query_id);
          break;
        case Windows:
          result_detail_path =
              String.format("%s\\neo4j_%d_%d_API.txt", resultDir, nodeCount, query_id);
          result_avg_path =
              String.format("%s\\neo4j_%d_%d_API_avg.txt", resultDir, nodeCount, query_id);
          break;
      }

      ArrayList<Long> time_get_iterator = new ArrayList<Long>();
      ArrayList<Long> time_iterate = new ArrayList<Long>();
      ArrayList<Long> total_time = new ArrayList<Long>();
      ArrayList<Long> count = new ArrayList<Long>();
      ArrayList<Long> access = new ArrayList<Long>();

      String write_line = String.format("%s\t%d\n", dataset, limit);
      if (!TEST_FORMAT) {
        OwnMethods.WriteFile(result_detail_path, true, write_line);
        OwnMethods.WriteFile(result_avg_path, true, write_line);
      }

      String head_line = "count\tget_iterator_time\titerate_time\ttotal_time\taccess_pages\n";
      if (!TEST_FORMAT)
        OwnMethods.WriteFile(result_avg_path, true, "selectivity\t" + head_line);

      for (int levelIndex = 0; levelIndex < areaLevel; levelIndex++) {
        String queryrect_path = null;
        switch (systemName) {
          case Ubuntu:
            queryrect_path = String.format("%s_%d.txt", spaPredicateDir, area);
            break;
          case Windows:
            queryrect_path = String.format("%s_%d.txt", spaPredicateDir, area);
            break;
        }
        write_line = area + "\n" + head_line;
        if (!TEST_FORMAT)
          OwnMethods.WriteFile(result_detail_path, true, write_line);
        ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
        Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match(db_path);
        for (int i = 0; i < experimentCount; i++) {
          MyRectangle rectangle = queryrect.get(i);
          query_Graph.spa_predicate = new MyRectangle[query_Graph.graph.size()];

          int j = 0;
          for (; j < query_Graph.graph.size(); j++)
            if (query_Graph.Has_Spa_Predicate[j])
              break;
          query_Graph.spa_predicate[j] = rectangle;

          if (!TEST_FORMAT) {
            Util.println(String.format("%d : %s", i, rectangle.toString()));

            Result result =
                naive_Neo4j_Match.neo4j_API.graphDb.execute("match (n) where id(n) = 0 return n");
            while (result.hasNext())
              result.next();

            start = System.currentTimeMillis();
            result = naive_Neo4j_Match.SubgraphMatch_Spa_API(query_Graph, limit);
            time = System.currentTimeMillis() - start;
            time_get_iterator.add(time);

            start = System.currentTimeMillis();
            while (result.hasNext())
              result.next();
            time = System.currentTimeMillis() - start;
            time_iterate.add(time);

            int index = time_get_iterator.size() - 1;
            total_time.add(time_get_iterator.get(index) + time_iterate.get(index));

            ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
            count.add(planDescription.getProfilerStatistics().getRows());
            access.add(OwnMethods.GetTotalDBHits(planDescription));

            write_line = String.format("%d\t%d\t", count.get(i), time_get_iterator.get(i));
            write_line += String.format("%d\t%d\t", time_iterate.get(i), total_time.get(i));
            write_line += String.format("%d\n", access.get(i));

            naive_Neo4j_Match.neo4j_API.ShutDown();

            OwnMethods.ClearCache(password);
            Thread.currentThread();
            Thread.sleep(5000);

            naive_Neo4j_Match.neo4j_API = new Neo4j_API(db_path);

            if (!TEST_FORMAT)
              OwnMethods.WriteFile(result_detail_path, true, write_line);
          }
        }
        naive_Neo4j_Match.neo4j_API.ShutDown();
        OwnMethods.ClearCache(password);

        write_line = String.valueOf(area) + "\t";
        write_line +=
            String.format("%d\t%d\t", Util.Average(count), Util.Average(time_get_iterator));
        write_line +=
            String.format("%d\t%d\t", Util.Average(time_iterate), Util.Average(total_time));
        write_line += String.format("%d\n", Util.Average(access));
        if (!TEST_FORMAT)
          OwnMethods.WriteFile(result_avg_path, true, write_line);

        // long larger_time = Utility.Average(total_time);
        // if (larger_time * expe_count > 150 * 1000)
        // expe_count = (int) (expe_count * 0.5 / (larger_time * expe_count / 150.0 / 1000.0));
        // if(expe_count < 1)
        // expe_count = 1;

        count.clear();
        time_get_iterator.clear();
        time_iterate.clear();
        total_time.clear();
        access.clear();

        area *= factor;

      }

      OwnMethods.WriteFile(result_detail_path, true, "\n");
      OwnMethods.WriteFile(result_avg_path, true, "\n");
    } catch (Exception e) {
      // TODO: handle exception
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
  public void risoTreeQueryPN(int nodeCount, int query_id) throws Exception {
    try {
      long start;
      long time;
      int limit = -1;

      String querygraph_path = String.format("%s/%d.txt", querygraphDir, nodeCount);
      ArrayList<Query_Graph> queryGraphs =
          Util.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
      Query_Graph query_Graph = queryGraphs.get(query_id);

      String result_detail_path = null, result_avg_path = null;
      switch (systemName) {
        case Ubuntu:
          result_detail_path = String.format("%s/risotree_PN%d_%d_%d.txt", resultDir, MAX_HOPNUM,
              nodeCount, query_id);
          result_avg_path = String.format("%s/risotree_PN%d_%d_%d_avg.txt", resultDir, MAX_HOPNUM,
              nodeCount, query_id);
          // result_detail_path = String.format("%s/risotree_%d_%d_test.txt", resultDir, nodeCount,
          // query_id);
          // result_avg_path = String.format("%s/risotree_%d_%d_avg_test.txt", resultDir, nodeCount,
          // query_id);
          break;
        case Windows:
          // result_detail_path = String.format("%s\\risotree_PN_%d_%d.txt", resultDir, nodeCount,
          // query_id);
          // result_avg_path = String.format("%s\\risotree_PN_%d_%d_avg.txt.txt", resultDir,
          // nodeCount, query_id);
          break;
      }

      String write_line = String.format("%s\t%d\n", dataset, limit);
      if (!TEST_FORMAT) {
        OwnMethods.WriteFile(result_detail_path, true, write_line);
        OwnMethods.WriteFile(result_avg_path, true, write_line);
      }

      String head_line =
          "count\trange_time\tget_iterator_time\titerate_time\ttotal_time\taccess_pages\n";
      if (!TEST_FORMAT)
        OwnMethods.WriteFile(result_avg_path, true, "selectivity\t" + head_line);

      for (int levelIndex = 0; levelIndex < areaLevel; levelIndex++) {

        String queryrect_path = null;
        switch (systemName) {
          case Ubuntu:
            queryrect_path = String.format("%s_%d.txt", spaPredicateDir, area);
            break;
          case Windows:
            queryrect_path = String.format("%s_%d.txt", spaPredicateDir, area);
            break;
        }

        write_line = area + "\n" + head_line;
        if (!TEST_FORMAT)
          OwnMethods.WriteFile(result_detail_path, true, write_line);

        ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
        HashMap<String, String> graph_pos_map = OwnMethods.ReadMap(graph_pos_map_path);
        long[] graph_pos_map_list = new long[graph_pos_map.size()];
        for (String key_str : graph_pos_map.keySet()) {
          int key = Integer.parseInt(key_str);
          int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
          graph_pos_map_list[key] = pos_id;
        }
        RisoTreeQueryPN risoTreeQueryPN =
            new RisoTreeQueryPN(db_path, dataset, graph_pos_map_list, MAX_HOPNUM);

        ArrayList<Long> range_query_time = new ArrayList<Long>();
        ArrayList<Long> time_get_iterator = new ArrayList<Long>();
        ArrayList<Long> time_iterate = new ArrayList<Long>();
        ArrayList<Long> total_time = new ArrayList<Long>();
        ArrayList<Long> count = new ArrayList<Long>();
        ArrayList<Long> access = new ArrayList<Long>();

        for (int i = 0; i < experimentCount; i++) {
          MyRectangle rectangle = queryrect.get(i);
          if (rectangle.area() == 0.0) {
            double delta = Math.pow(0.1, 10);
            rectangle = new MyRectangle(rectangle.min_x - delta, rectangle.min_y - delta,
                rectangle.max_x + delta, rectangle.max_y + delta);
          }

          query_Graph.spa_predicate = new MyRectangle[query_Graph.graph.size()];

          // only handle query with one spatial predicate
          int j = 0;
          for (; j < query_Graph.graph.size(); j++)
            if (query_Graph.Has_Spa_Predicate[j])
              break;
          query_Graph.spa_predicate[j] = rectangle;

          if (!TEST_FORMAT) {
            Util.println(String.format("%d : %s", i, rectangle.toString()));

            start = System.currentTimeMillis();
            risoTreeQueryPN.Query(query_Graph, -1);
            time = System.currentTimeMillis() - start;

            time_get_iterator.add(risoTreeQueryPN.get_iterator_time);
            time_iterate.add(risoTreeQueryPN.iterate_time);
            total_time.add(time);
            count.add(risoTreeQueryPN.result_count);
            access.add(risoTreeQueryPN.page_hit_count);
            Util.println("Page access:" + risoTreeQueryPN.page_hit_count);
            range_query_time.add(risoTreeQueryPN.range_query_time);

            write_line = String.format("%d\t%d\t", count.get(i), range_query_time.get(i));
            write_line += String.format("%d\t", time_get_iterator.get(i));
            write_line += String.format("%d\t%d\t", time_iterate.get(i), total_time.get(i));
            write_line += String.format("%d\n", access.get(i));
            if (!TEST_FORMAT)
              OwnMethods.WriteFile(result_detail_path, true, write_line);
          }

          risoTreeQueryPN.dbservice.shutdown();

          OwnMethods.ClearCache(password);
          Thread.currentThread();
          Thread.sleep(5000);

          risoTreeQueryPN.dbservice =
              new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));

        }
        risoTreeQueryPN.dbservice.shutdown();

        write_line = String.valueOf(area) + "\t";
        write_line +=
            String.format("%d\t%d\t", Util.Average(count), Util.Average(range_query_time));
        write_line += String.format("%d\t", Util.Average(time_get_iterator));
        write_line +=
            String.format("%d\t%d\t", Util.Average(time_iterate), Util.Average(total_time));
        write_line += String.format("%d\n", Util.Average(access));
        if (!TEST_FORMAT)
          OwnMethods.WriteFile(result_avg_path, true, write_line);

        // long larger_time = Utility.Average(total_time);
        // if (larger_time * expe_count > 450 * 1000)
        // expe_count = (int) (expe_count * 0.5 / (larger_time * expe_count / 450.0 / 1000.0));
        // if(expe_count < 1)
        // expe_count = 1;

        area *= factor;
      }
      OwnMethods.WriteFile(result_detail_path, true, "\n");
      OwnMethods.WriteFile(result_avg_path, true, "\n");
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

}
