package experiment;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import commons.*;
import commons.Config.system;
import graph.*;

/**
 * Experiment for selectivity The selectivity is evaluated by using number spatial objects.
 * 
 * @author ysun138
 *
 */
public class Experiment {

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
  public String spaPredicateDir;
  public String resultDir;

  public boolean TEST_FORMAT;
  public int experimentCount = 1;

  // non-spatial ratio 20
  // static double startSelectivity = 0.000001;
  // static double endSelectivity = 0.002;

  // non-spatial ratio 80
  // double startSelectivity = 0.00001;
  // double endSelectivity = 0.2;

  // foursquare_100
  // public double startSelectivity = 0.000001;
  // public double endSelectivity = 0.002;

  // wikidata_100
  // public double startSelectivity = 0.000001;
  // public double endSelectivity = 0.02;

  // yelp
  public double startSelectivity = 0.0001;
  public double endSelectivity = 0.2;

  // Patents
  // double startSelectivity = 0.00001;
  // double endSelectivity = 0.002;

  // for switching point detect
  // static double startSelectivity = 0.01;
  // static double endSelectivity = 0.2;

  // static double startSelectivity = 0.0001;
  // static double endSelectivity = 0.2;

  public int entityCount, spaCount;

  public Experiment(Config config) {
    this.config = config;
    initializeParameters();
  }

  static String dir = "/hdd2/data/ysun138/RisoTree";

  public void initializeParametersServer() {
    TEST_FORMAT = false;
    dataset = Config.Datasets.wikidata_100.name();
    version = config.GetNeo4jVersion();
    systemName = system.Ubuntu;
    db_path = dir + "/neo4j-community-3.1.1/data/databases/graph.db";
    entityPath = dir + "/entity.txt";
    graph_pos_map_path = dir + "/node_map_RTree.txt";
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
        spaPredicateDir = String
            .format("/mnt/hgfs/Google_Drive/Projects/risotree/query/spa_predicate/%s", dataset);
        resultDir = String.format("/mnt/hgfs/Google_Drive/Experiment_Result/Riso-Tree/%s", dataset);
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
        // querygraphDir = String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\query_graph\\%s",
        // dataset);
        // spaPredicateDir = String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\spa_predicate\\%s",
        // dataset);
        resultDir = String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s", dataset);
        break;
    }
    spaCount = OwnMethods.GetSpatialEntityCount(entityPath);
    entityCount = OwnMethods.getEntityCount(entityPath);
  }

  public static void main(String[] args) throws Exception {
    try {

      Config config = new Config();

      // ArrayList<String> dataset_a = new ArrayList<String>(Arrays.asList(
      // Config.Datasets.Gowalla_100.name(),
      // Config.Datasets.foursquare_100.name(),
      // Config.Datasets.Patents_100_random_80.name(),
      // Config.Datasets.go_uniprot_100_random_80.name()));

      // config.setDatasetName(Config.Datasets.wikidata_100.name());
      // Experiment experiment = new Experiment(config);
      // experiment.experimentCount = 3;
      // experiment.Neo4j_Naive(10, 0);

      config.setDatasetName(Config.Datasets.Yelp_100.name());

      for (int hopNum = 0; hopNum >= 0; hopNum--)
      // int hopNum = 2;
      {
        config.setMAXHOPNUM(hopNum);
        Experiment experiment = new Experiment(config);
        experiment.initializeParameters();
        experiment.experimentCount = 3;
        experiment.risoTreeQueryPN(5, 3);
      }

      Experiment experiment = new Experiment(config);
      experiment.experimentCount = 3;
      experiment.Neo4j_Naive(5, 3);

      // for (int nodeCount = 5; nodeCount <= 35; nodeCount+=5)
      // // for ( int queryIndex = 0; queryIndex < 9; queryIndex++)
      // for (int k = 0; k < 1;k ++)
      // {
      // for ( int queryIndex = 0; queryIndex < 1; queryIndex++)
      // {
      // }
      // }

    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * HMBR query
   * 
   * @param nodeCount
   * @param query_id
   * @throws Exception
   */
  public void HMBR(int nodeCount, int query_id) throws Exception {
    long start;
    long time;
    int limit = -1;

    String querygraph_path = String.format("%s/%d.txt", querygraphDir, nodeCount);
    ArrayList<Query_Graph> queryGraphs = Util.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
    Query_Graph query_Graph = queryGraphs.get(query_id);

    String result_detail_path = null, result_avg_path = null;
    switch (systemName) {
      case Ubuntu:
        result_detail_path = String.format("%s/HMBR_%d_%d_API.txt", resultDir, nodeCount, query_id);
        result_avg_path =
            String.format("%s/HMBR_%d_%d_API_avg.txt", resultDir, nodeCount, query_id);
        break;
      case Windows:
        result_detail_path =
            String.format("%s\\HMBR_%d_%d_API.txt", resultDir, nodeCount, query_id);
        result_avg_path =
            String.format("%s\\HMBR_%d_%d_API_avg.txt", resultDir, nodeCount, query_id);
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

    double selectivity = startSelectivity;
    int times = 10;
    while (selectivity <= endSelectivity) {
      int name_suffix = (int) (selectivity * spaCount);

      String queryrect_path = null;
      switch (systemName) {
        case Ubuntu:
          queryrect_path = String.format("%s/queryrect_%d.txt", spaPredicateDir, name_suffix);
          break;
        case Windows:
          queryrect_path = String.format("%s\\queryrect_%d.txt", spaPredicateDir, name_suffix);
          break;
      }

      write_line = selectivity + "\n" + head_line;
      if (!TEST_FORMAT)
        OwnMethods.WriteFile(result_detail_path, true, write_line);

      ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
      HMBR hmbr = new HMBR(db_path);
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

          start = System.currentTimeMillis();
          Result result = hmbr.Query(query_Graph, limit);
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

          hmbr.dbservice.shutdown();

          OwnMethods.ClearCache(password);
          Thread.currentThread();
          Thread.sleep(5000);

          hmbr.dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));

          if (!TEST_FORMAT)
            OwnMethods.WriteFile(result_detail_path, true, write_line);
        }
      }
      hmbr.dbservice.shutdown();
      OwnMethods.ClearCache(password);

      write_line = String.valueOf(selectivity) + "\t";
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

      selectivity *= times;
    }
    OwnMethods.WriteFile(result_detail_path, true, "\n");
    OwnMethods.WriteFile(result_avg_path, true, "\n");
  }

  /**
   * use RisoTree query with HMBR and graph first approach
   * 
   * @param nodeCount
   * @param query_id
   * @throws Exception
   */
  public void risoTreeQuery_HMBR(int nodeCount, int query_id) throws Exception {
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
              String.format("%s/risotree_HMBR_%d_%d.txt", resultDir, nodeCount, query_id);
          result_avg_path =
              String.format("%s/risotree_HMBR_%d_%d_avg.txt", resultDir, nodeCount, query_id);
          // result_detail_path = String.format("%s/risotree_%d_%d_test.txt", resultDir, nodeCount,
          // query_id);
          // result_avg_path = String.format("%s/risotree_%d_%d_avg_test.txt", resultDir, nodeCount,
          // query_id);
          break;
        case Windows:
          result_detail_path =
              String.format("%s\\risotree_HMBR_%d_%d.txt", resultDir, nodeCount, query_id);
          result_avg_path =
              String.format("%s\\risotree_HMBR_%d_%d_avg.txt.txt", resultDir, nodeCount, query_id);
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

      double selectivity = startSelectivity;
      int times = 10;
      while (selectivity <= endSelectivity) {
        int name_suffix = (int) (selectivity * spaCount);

        String queryrect_path = null;
        switch (systemName) {
          case Ubuntu:
            queryrect_path = String.format("%s/queryrect_%d.txt", spaPredicateDir, name_suffix);
            break;
          case Windows:
            queryrect_path = String.format("%s\\queryrect_%d.txt", spaPredicateDir, name_suffix);
            break;
        }

        write_line = selectivity + "\n" + head_line;
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
        RisoTreeQuery risoTreeQuery = new RisoTreeQuery(db_path, dataset, graph_pos_map_list);

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
            risoTreeQuery.QueryHMBR(query_Graph, -1);
            time = System.currentTimeMillis() - start;

            time_get_iterator.add(risoTreeQuery.get_iterator_time);
            time_iterate.add(risoTreeQuery.iterate_time);
            total_time.add(time);
            count.add(risoTreeQuery.result_count);
            access.add(risoTreeQuery.page_hit_count);
            range_query_time.add(risoTreeQuery.range_query_time);

            write_line = String.format("%d\t%d\t", count.get(i), range_query_time.get(i));
            write_line += String.format("%d\t", time_get_iterator.get(i));
            write_line += String.format("%d\t%d\t", time_iterate.get(i), total_time.get(i));
            write_line += String.format("%d\n", access.get(i));
            if (!TEST_FORMAT)
              OwnMethods.WriteFile(result_detail_path, true, write_line);
          }

          risoTreeQuery.dbservice.shutdown();

          OwnMethods.ClearCache(password);
          Thread.currentThread();
          Thread.sleep(5000);

          risoTreeQuery.dbservice =
              new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));

        }
        risoTreeQuery.dbservice.shutdown();

        write_line = String.valueOf(selectivity) + "\t";
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

        selectivity *= times;
      }
      OwnMethods.WriteFile(result_detail_path, true, "\n");
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
  public void risoTreeQueryPN(int nodeCount, int query_id) throws Exception {
    try {
      long start;
      long time;
      int limit = -1;

      Util.println("read map from " + graph_pos_map_path);
      long[] graph_pos_map_list = OwnMethods.ReadMap(graph_pos_map_path, entityCount);

      String querygraph_path = String.format("%s/%d.txt", querygraphDir, nodeCount);
      Util.println("read query graph from " + querygraph_path);
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
          result_detail_path =
              String.format("%s\\risotree_PN_%d_%d.txt", resultDir, nodeCount, query_id);
          result_avg_path =
              String.format("%s\\risotree_PN_%d_%d_avg.txt.txt", resultDir, nodeCount, query_id);
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

      double selectivity = startSelectivity;
      int times = 10;
      while (selectivity <= endSelectivity) {
        int name_suffix = (int) (selectivity * spaCount);

        String queryrect_path = null;
        switch (systemName) {
          case Ubuntu:
            queryrect_path = String.format("%s/queryrect_%d.txt", spaPredicateDir, name_suffix);
            break;
          case Windows:
            queryrect_path = String.format("%s\\queryrect_%d.txt", spaPredicateDir, name_suffix);
            break;
        }

        write_line = selectivity + "\n" + head_line;
        if (!TEST_FORMAT)
          OwnMethods.WriteFile(result_detail_path, true, write_line);

        Util.println("read query rectangle from " + queryrect_path);
        ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);

        Util.println("initialize RisoTreeQueryPN \ndbpath: " + db_path + "\n dataset: " + dataset
            + "\n MAX_HOPNUM: " + MAX_HOPNUM);

        RisoTreeQueryPN risoTreeQueryPN = null;
        if (MAX_HOPNUM != 0)
          risoTreeQueryPN =
              new RisoTreeQueryPN(db_path, dataset, graph_pos_map_list, MAX_HOPNUM, true);
        else
          risoTreeQueryPN =
              new RisoTreeQueryPN(db_path, dataset, graph_pos_map_list, MAX_HOPNUM, false);

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
            // OwnMethods.Print(String.format("%d : %s", i, rectangle.toString()));
            Util.println(query_Graph);
            Util.println(query_Graph.spa_predicate);

            // prehot the database
            Result result = risoTreeQueryPN.dbservice.execute("match (n) where id(n) = 0 return n");
            while (result.hasNext())
              result.next();

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

        write_line = String.valueOf(selectivity) + "\t";
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

        selectivity *= times;
      }
      OwnMethods.WriteFile(result_detail_path, true, "\n");
      OwnMethods.WriteFile(result_avg_path, true, "\n");
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * Use RisoTree and graph first approach (not used any more)
   * 
   * @param nodeCount
   * @param query_id
   * @throws Exception
   */
  public void risoTreeQuery(int nodeCount, int query_id) throws Exception {
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
              String.format("%s/risotree_%d_%d.txt", resultDir, nodeCount, query_id);
          result_avg_path =
              String.format("%s/risotree_%d_%d_avg.txt", resultDir, nodeCount, query_id);
          // result_detail_path = String.format("%s/risotree_%d_%d_test.txt", resultDir, nodeCount,
          // query_id);
          // result_avg_path = String.format("%s/risotree_%d_%d_avg_test.txt", resultDir, nodeCount,
          // query_id);
          break;
        case Windows:
          result_detail_path =
              String.format("%s\\risotree_%d_%d.txt", resultDir, nodeCount, query_id);
          result_avg_path =
              String.format("%s\\risotree_%d_%d_avg.txt.txt", resultDir, nodeCount, query_id);
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

      double selectivity = startSelectivity;
      int times = 10;
      while (selectivity <= endSelectivity) {
        int name_suffix = (int) (selectivity * spaCount);

        String queryrect_path = null;
        switch (systemName) {
          case Ubuntu:
            queryrect_path = String.format("%s/queryrect_%d.txt", spaPredicateDir, name_suffix);
            break;
          case Windows:
            queryrect_path = String.format("%s\\queryrect_%d.txt", spaPredicateDir, name_suffix);
            break;
        }

        write_line = selectivity + "\n" + head_line;
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
        RisoTreeQuery risoTreeQuery = new RisoTreeQuery(db_path, dataset, graph_pos_map_list);

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
            risoTreeQuery.Query(query_Graph, -1);
            time = System.currentTimeMillis() - start;

            time_get_iterator.add(risoTreeQuery.get_iterator_time);
            time_iterate.add(risoTreeQuery.iterate_time);
            total_time.add(time);
            count.add(risoTreeQuery.result_count);
            access.add(risoTreeQuery.page_hit_count);
            Util.println("Page access:" + risoTreeQuery.page_hit_count);
            range_query_time.add(risoTreeQuery.range_query_time);

            write_line = String.format("%d\t%d\t", count.get(i), range_query_time.get(i));
            write_line += String.format("%d\t", time_get_iterator.get(i));
            write_line += String.format("%d\t%d\t", time_iterate.get(i), total_time.get(i));
            write_line += String.format("%d\n", access.get(i));
            if (!TEST_FORMAT)
              OwnMethods.WriteFile(result_detail_path, true, write_line);
          }

          risoTreeQuery.dbservice.shutdown();

          OwnMethods.ClearCache(password);
          Thread.currentThread();
          Thread.sleep(5000);

          risoTreeQuery.dbservice =
              new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));

        }
        risoTreeQuery.dbservice.shutdown();

        write_line = String.valueOf(selectivity) + "\t";
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

        selectivity *= times;
      }
      OwnMethods.WriteFile(result_detail_path, true, "\n");
      OwnMethods.WriteFile(result_avg_path, true, "\n");
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
      Util.println("read query graph from " + querygraph_path);
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

      double selectivity = startSelectivity;
      int times = 10;
      while (selectivity <= endSelectivity) {
        int name_suffix = (int) (selectivity * spaCount);

        String queryrect_path = null;
        switch (systemName) {
          case Ubuntu:
            queryrect_path = String.format("%s/queryrect_%d.txt", spaPredicateDir, name_suffix);
            break;
          case Windows:
            queryrect_path = String.format("%s\\queryrect_%d.txt", spaPredicateDir, name_suffix);
            break;
        }

        write_line = selectivity + "\n" + head_line;
        if (!TEST_FORMAT)
          OwnMethods.WriteFile(result_detail_path, true, write_line);

        Util.println("read query rectangle from " + queryrect_path);
        ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
        Util.println("initialize Naive_Neo4j_Match with db_path: " + db_path);
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
            // OwnMethods.Print(String.format("%d : %s", i, rectangle.toString()));
            Util.println(query_Graph);
            Util.println(query_Graph.spa_predicate);

            // prehot the database
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

        write_line = String.valueOf(selectivity) + "\t";
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

        selectivity *= times;
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
   * use SpatialFirst_List class and block query
   * 
   * @param nodeCount
   * @param query_id
   * @throws Exception
   */
  public void SpatialFirstList_Block(int nodeCount, int query_id) throws Exception {
    long start;
    long time;
    int limit = -1;

    String querygraph_path = String.format("%s/%d.txt", querygraphDir, nodeCount);
    ArrayList<Query_Graph> queryGraphs = Util.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
    Query_Graph query_Graph = queryGraphs.get(query_id);

    String result_detail_path = null, result_avg_path = null;
    switch (systemName) {
      case Ubuntu:
        result_detail_path =
            String.format("%s/spa_first_list_Block_%d_%d_API.txt", resultDir, nodeCount, query_id);
        result_avg_path = String.format("%s/spa_first_list_Block_%d_%d_API_avg.txt", resultDir,
            nodeCount, query_id);
        break;
      case Windows:
        result_detail_path =
            String.format("%s\\spa_first_list_Block_%d_%d_API.txt", resultDir, nodeCount, query_id);
        result_avg_path = String.format("%s\\spa_first_list_Block_%d_%d_API_avg.txt", resultDir,
            nodeCount, query_id);
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

    double selectivity = startSelectivity;
    int times = 10;
    while (selectivity <= endSelectivity) {
      int name_suffix = (int) (selectivity * spaCount);

      String queryrect_path = null;
      switch (systemName) {
        case Ubuntu:
          queryrect_path = String.format("%s/queryrect_%d.txt", spaPredicateDir, name_suffix);
          break;
        case Windows:
          queryrect_path = String.format("%s\\queryrect_%d.txt", spaPredicateDir, name_suffix);
          break;
      }

      write_line = selectivity + "\n" + head_line;
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
      SpatialFirst_List spa_First_list =
          new SpatialFirst_List(db_path, dataset, graph_pos_map_list);

      ArrayList<Long> range_query_time = new ArrayList<Long>();
      ArrayList<Long> time_get_iterator = new ArrayList<Long>();
      ArrayList<Long> time_iterate = new ArrayList<Long>();
      ArrayList<Long> total_time = new ArrayList<Long>();
      ArrayList<Long> count = new ArrayList<Long>();
      ArrayList<Long> access = new ArrayList<Long>();

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

          start = System.currentTimeMillis();
          spa_First_list.query_Block(query_Graph, -1);
          time = System.currentTimeMillis() - start;

          time_get_iterator.add(spa_First_list.get_iterator_time);
          time_iterate.add(spa_First_list.iterate_time);
          total_time.add(time);
          count.add(spa_First_list.result_count);
          access.add(spa_First_list.page_hit_count);
          range_query_time.add(spa_First_list.range_query_time);

          write_line = String.format("%d\t%d\t", count.get(i), range_query_time.get(i));
          write_line += String.format("%d\t", time_get_iterator.get(i));
          write_line += String.format("%d\t%d\t", time_iterate.get(i), total_time.get(i));
          write_line += String.format("%d\n", access.get(i));
          if (!TEST_FORMAT)
            OwnMethods.WriteFile(result_detail_path, true, write_line);
        }

        spa_First_list.shutdown();

        OwnMethods.ClearCache(password);
        Thread.currentThread();
        Thread.sleep(5000);

        spa_First_list.dbservice =
            new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));

      }
      spa_First_list.shutdown();
      OwnMethods.ClearCache(password);

      write_line = String.valueOf(selectivity) + "\t";
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

      selectivity *= times;
    }
    OwnMethods.WriteFile(result_detail_path, true, "\n");
    OwnMethods.WriteFile(result_avg_path, true, "\n");
  }

  /**
   * Use SpatialFirst_List class and non-block query. Same performance with simple SpatialFirst
   * approach. So not recorded in the experiment.
   * 
   * @param query_id
   * @throws InterruptedException
   */
  public void SpatialFirstList(int query_id) throws InterruptedException {
    long start;
    long time;
    int limit = -1;
    int nodeCount = 4;

    String querygraph_path = String.format("%s/%d.txt", querygraphDir, nodeCount);
    ArrayList<Query_Graph> queryGraphs = Util.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
    Query_Graph query_Graph = queryGraphs.get(query_id);

    String result_detail_path = null, result_avg_path = null;
    switch (systemName) {
      case Ubuntu:
        result_detail_path = String.format("%s/spa_first_list_%d_API.txt", resultDir, query_id);
        result_avg_path = String.format("%s/spa_first_list_%d_API_avg.txt", resultDir, query_id);
        break;
      case Windows:
        result_detail_path = String.format("%s\\spa_first_list_%d_API.txt", resultDir, query_id);
        result_avg_path = String.format("%s\\spa_first_list_%d_API_avg.txt", resultDir, query_id);
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

    double selectivity = startSelectivity;
    int times = 10;
    while (selectivity <= endSelectivity) {
      int name_suffix = (int) (selectivity * spaCount);

      String queryrect_path = null;
      switch (systemName) {
        case Ubuntu:
          queryrect_path = String.format("%s/queryrect_%d.txt", spaPredicateDir, name_suffix);
          break;
        case Windows:
          queryrect_path = String.format("%s\\queryrect_%d.txt", spaPredicateDir, name_suffix);
          break;
      }

      write_line = selectivity + "\n" + head_line;
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
      SpatialFirst_List spa_First_list =
          new SpatialFirst_List(db_path, dataset, graph_pos_map_list);

      ArrayList<Long> range_query_time = new ArrayList<Long>();
      ArrayList<Long> time_get_iterator = new ArrayList<Long>();
      ArrayList<Long> time_iterate = new ArrayList<Long>();
      ArrayList<Long> total_time = new ArrayList<Long>();
      ArrayList<Long> count = new ArrayList<Long>();
      ArrayList<Long> access = new ArrayList<Long>();

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

          start = System.currentTimeMillis();
          spa_First_list.query(query_Graph, -1);
          time = System.currentTimeMillis() - start;

          time_get_iterator.add(spa_First_list.get_iterator_time);
          time_iterate.add(spa_First_list.iterate_time);
          total_time.add(time);
          count.add(spa_First_list.result_count);
          access.add(spa_First_list.page_hit_count);
          range_query_time.add(spa_First_list.range_query_time);

          write_line = String.format("%d\t%d\t", count.get(i), range_query_time.get(i));
          write_line += String.format("%d\t", time_get_iterator.get(i));
          write_line += String.format("%d\t%d\t", time_iterate.get(i), total_time.get(i));
          write_line += String.format("%d\n", access.get(i));
          if (!TEST_FORMAT)
            OwnMethods.WriteFile(result_detail_path, true, write_line);
        }
        spa_First_list.shutdown();
        OwnMethods.ClearCache(password);
        Thread.currentThread();
        Thread.sleep(5000);
        spa_First_list.dbservice =
            new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      }
      spa_First_list.shutdown();
      OwnMethods.ClearCache(password);

      write_line = String.valueOf(selectivity) + "\t";
      write_line +=
          String.format("%d\t%d\t", Util.Average(count), Util.Average(range_query_time));
      write_line += String.format("%d\t", Util.Average(time_get_iterator));
      write_line +=
          String.format("%d\t%d\t", Util.Average(time_iterate), Util.Average(total_time));
      write_line += String.format("%d\n", Util.Average(access));
      if (!TEST_FORMAT)
        OwnMethods.WriteFile(result_avg_path, true, write_line);

      long larger_time = Util.Average(total_time);
      if (larger_time * experimentCount > 450 * 1000)
        experimentCount =
            (int) (experimentCount * 0.5 / (larger_time * experimentCount / 450.0 / 1000.0));
      if (experimentCount < 1)
        experimentCount = 1;

      selectivity *= times;
    }
    OwnMethods.WriteFile(result_detail_path, true, "\n");
    OwnMethods.WriteFile(result_avg_path, true, "\n");
  }


  /**
   * use SpatialFirst
   * 
   * @param nodeCount
   * @param query_id
   * @throws Exception
   */
  public void SpatialFirst(int nodeCount, int query_id) throws Exception {
    long start;
    long time;
    int limit = -1;

    String querygraph_path = String.format("%s/%d.txt", querygraphDir, nodeCount);
    ArrayList<Query_Graph> queryGraphs = Util.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
    Query_Graph query_Graph = queryGraphs.get(query_id);

    String result_detail_path = null, result_avg_path = null;
    switch (systemName) {
      case Ubuntu:
        result_detail_path =
            String.format("%s/spa_first_%d_%d_API.txt", resultDir, nodeCount, query_id);
        result_avg_path =
            String.format("%s/spa_first_%d_%d_API_avg.txt", resultDir, nodeCount, query_id);
        break;
      case Windows:
        result_detail_path =
            String.format("%s\\spa_first_%d_%d_API.txt", resultDir, nodeCount, query_id);
        result_avg_path =
            String.format("%s\\spa_first_%d_%d_API_avg.txt", resultDir, nodeCount, query_id);
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

    double selectivity = startSelectivity;
    int times = 10;
    while (selectivity <= endSelectivity) {
      int name_suffix = (int) (selectivity * spaCount);
      String queryrect_path = null;
      switch (systemName) {
        case Ubuntu:
          queryrect_path = String.format("%s/queryrect_%d.txt", spaPredicateDir, name_suffix);
          break;
        case Windows:
          queryrect_path = String.format("%s\\queryrect_%d.txt", spaPredicateDir, name_suffix);
          break;
      }

      write_line = selectivity + "\n" + head_line;
      if (!TEST_FORMAT)
        OwnMethods.WriteFile(result_detail_path, true, write_line);

      ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
      Util.println(db_path);
      Util.println(dataset);
      SpatialFirst spa_First = new SpatialFirst(db_path, dataset);

      ArrayList<Long> range_query_time = new ArrayList<Long>();
      ArrayList<Long> time_get_iterator = new ArrayList<Long>();
      ArrayList<Long> time_iterate = new ArrayList<Long>();
      ArrayList<Long> total_time = new ArrayList<Long>();
      ArrayList<Long> count = new ArrayList<Long>();
      ArrayList<Long> access = new ArrayList<Long>();

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

          start = System.currentTimeMillis();
          spa_First.query(query_Graph, -1);
          time = System.currentTimeMillis() - start;

          time_get_iterator.add(spa_First.get_iterator_time);
          time_iterate.add(spa_First.iterate_time);
          total_time.add(time);
          count.add(spa_First.result_count);
          access.add(spa_First.page_hit_count);
          range_query_time.add(spa_First.range_query_time);

          write_line = String.format("%d\t%d\t", count.get(i), range_query_time.get(i));
          write_line += String.format("%d\t", time_get_iterator.get(i));
          write_line += String.format("%d\t%d\t", time_iterate.get(i), total_time.get(i));
          write_line += String.format("%d\n", access.get(i));
          if (!TEST_FORMAT)
            OwnMethods.WriteFile(result_detail_path, true, write_line);

          spa_First.shutdown();

          OwnMethods.ClearCache(password);
          Thread.currentThread();
          Thread.sleep(5000);

          spa_First.dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
        }
      }
      spa_First.shutdown();
      OwnMethods.ClearCache(password);

      write_line = String.valueOf(selectivity) + "\t";
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

      selectivity *= times;
    }
    OwnMethods.WriteFile(result_detail_path, true, "\n");
    OwnMethods.WriteFile(result_avg_path, true, "\n");
  }

}
