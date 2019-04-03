package experiment.KNN;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import commons.Config;
import commons.Entity;
import commons.MyRectangle;
import commons.OwnMethods;
import commons.Query_Graph;
import commons.Util;
import commons.Config.Datasets;
import commons.Config.system;
import graph.RisoTreeQueryPN;
import graph.SpatialFirst_List;

public class QueryNodeCount {

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
  public String queryrectCenterPath = null;
  public ArrayList<Entity> entities = null;
  public ArrayList<MyRectangle> queryrect = null;
  public long[] graph_pos_map_list;

  public int entityCount, spaCount;

  public boolean TEST_FORMAT;
  public int experimentCount = 5;
  public int K = 5;

  public QueryNodeCount() {
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
        spaPredicateDir = String
            .format("/mnt/hgfs/Google_Drive/Projects/risotree/query/spa_predicate/%s", dataset);
        queryrectCenterPath = String.format("%s/%s_centerids.txt", spaPredicateDir, dataset);
        resultDir =
            String.format("/mnt/hgfs/Google_Drive/Experiment_Result/Riso-Tree/KNN/%s", dataset);
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
        spaPredicateDir = String
            .format("D:\\Google_Drive\\Projects\\risotree\\query\\spa_predicate\\%s", dataset);
        queryrectCenterPath =
            String.format("%s/%s_centerids.txt", spaPredicateDir, dataset.split("_")[0]);
        resultDir =
            String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\KNN\\%s", dataset);
        break;
    }
    entities = OwnMethods.ReadEntity(entityPath);
    ArrayList<Integer> ids = OwnMethods.readIntegerArray(queryrectCenterPath);
    queryrect = new ArrayList<MyRectangle>();
    for (int id : ids) {
      Entity entity = entities.get(id);
      queryrect.add(new MyRectangle(entity.lon, entity.lat, entity.lon, entity.lat));
    }

    spaCount = OwnMethods.GetSpatialEntityCount(entityPath);
    entityCount = OwnMethods.getEntityCount(entityPath);

    Util.println("read map from " + graph_pos_map_path);
    graph_pos_map_list = OwnMethods.ReadMap(graph_pos_map_path, entityCount);
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    ArrayList<Integer> nodeCountList = new ArrayList<Integer>();
    // nodeCountList.add(3);
    // nodeCountList.add(5);
    nodeCountList.add(7);
    // nodeCountList.add(9);
    // nodeCountList.add(11);

    ArrayList<String> datasets = new ArrayList<String>();
    // datasets.add(Datasets.Patents_100_random_80.name());
    // datasets.add(Datasets.foursquare_100.name());
    // datasets.add(Datasets.go_uniprot_100_random_80.name());
    datasets.add(Datasets.wikidata_100.name());

    QueryNodeCount queryNodeCount = new QueryNodeCount();
    for (String dataset : datasets) {
      queryNodeCount.config.setDatasetName(dataset);
      queryNodeCount.initializeParameters();
      queryNodeCount.RisoTreePN(nodeCountList, 0);
      queryNodeCount.SpatialFirst(nodeCountList, 0);
    }

  }

  public void RisoTreePN(ArrayList<Integer> nodeCountList, int query_id) {
    try {
      long start;
      long time;

      String result_detail_path = null, result_avg_path = null;
      switch (systemName) {
        case Ubuntu:
          result_detail_path =
              String.format("%s/risotree_PN%d_%d.txt", resultDir, MAX_HOPNUM, query_id);
          result_avg_path =
              String.format("%s/risotree_PN%d_%d_avg.txt", resultDir, MAX_HOPNUM, query_id);
          break;
        case Windows:
          result_detail_path = String.format("%s\\risotree_PN%d_%d.txt", resultDir, query_id);
          result_avg_path = String.format("%s\\risotree_PN%d_%d_avg.txt", resultDir, query_id);
          break;
      }

      String write_line = String.format("%s\t%d\n", dataset, K);
      if (!TEST_FORMAT) {
        OwnMethods.WriteFile(result_detail_path, true, write_line);
        OwnMethods.WriteFile(result_avg_path, true, write_line);
      }

      String head_line =
          "visited_count\tqueue_time\tcheckPaths_time\tget_iterator_time\titerate_time\t"
              + "total_time\taccess_pages\n";
      if (!TEST_FORMAT)
        OwnMethods.WriteFile(result_avg_path, true, "nodeCount\t" + head_line);

      for (int nodeCount : nodeCountList) {
        String querygraph_path = String.format("%s/%d.txt", querygraphDir, nodeCount);
        ArrayList<Query_Graph> queryGraphs =
            Util.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
        Query_Graph query_Graph = queryGraphs.get(query_id);

        write_line = nodeCount + "\n" + head_line;
        if (!TEST_FORMAT)
          OwnMethods.WriteFile(result_detail_path, true, write_line);

        RisoTreeQueryPN risoTreeQueryPN =
            new RisoTreeQueryPN(db_path, dataset, graph_pos_map_list, MAX_HOPNUM);

        ArrayList<Long> time_queue = new ArrayList<Long>();
        ArrayList<Long> time_checkPaths = new ArrayList<Long>();
        ArrayList<Long> time_get_iterator = new ArrayList<Long>();
        ArrayList<Long> time_iterate = new ArrayList<Long>();
        ArrayList<Long> total_time = new ArrayList<Long>();
        ArrayList<Long> visited_spatial_count = new ArrayList<Long>();
        ArrayList<Long> page_hit = new ArrayList<Long>();

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
            Util.println(query_Graph);
            Util.println(String.format("%d : %s", i, rectangle.toString()));

            start = System.currentTimeMillis();
            risoTreeQueryPN.LAGAQ_KNN(query_Graph, K);
            time = System.currentTimeMillis() - start;

            time_get_iterator.add(risoTreeQueryPN.get_iterator_time);
            time_iterate.add(risoTreeQueryPN.iterate_time);
            total_time.add(time);
            visited_spatial_count.add((long) risoTreeQueryPN.visit_spatial_object_count);
            page_hit.add(risoTreeQueryPN.page_hit_count);
            Util.println("Page access:" + risoTreeQueryPN.page_hit_count);
            time_queue.add(risoTreeQueryPN.queue_time);
            time_checkPaths.add(risoTreeQueryPN.check_paths_time);

            write_line = String.format("%d\t%d\t", visited_spatial_count.get(i), time_queue.get(i));
            write_line +=
                String.format("%d\t%d\t", time_checkPaths.get(i), time_get_iterator.get(i));
            write_line += String.format("%d\t%d\t", time_iterate.get(i), total_time.get(i));
            write_line += String.format("%d\n", page_hit.get(i));
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

        write_line = String.valueOf(nodeCount) + "\t";
        write_line += String.format("%d\t%d\t", Util.Average(visited_spatial_count),
            Util.Average(time_queue));
        write_line += String.format("%d\t%d\t", Util.Average(time_checkPaths),
            Util.Average(time_get_iterator));
        write_line +=
            String.format("%d\t%d\t", Util.Average(time_iterate), Util.Average(total_time));
        write_line += String.format("%d\n", Util.Average(page_hit));
        if (!TEST_FORMAT)
          OwnMethods.WriteFile(result_avg_path, true, write_line);

      }
      OwnMethods.WriteFile(result_detail_path, true, "\n");
      OwnMethods.WriteFile(result_avg_path, true, "\n");
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public void SpatialFirst(ArrayList<Integer> nodeCountList, int query_id) {
    try {
      long start;
      long time;

      String result_detail_path = null, result_avg_path = null;
      switch (systemName) {
        case Ubuntu:
          result_detail_path = String.format("%s/spatialFirst_%d.txt", resultDir, query_id);
          result_avg_path = String.format("%s/spatialFirst_%d_avg.txt", resultDir, query_id);
          break;
        case Windows:
          result_detail_path = String.format("%s\\spatialFirst_%d.txt", resultDir, query_id);
          result_avg_path = String.format("%s\\spatialFirst_%d_avg.txt", resultDir, query_id);
          break;
      }

      String write_line = String.format("%s\t%d\n", dataset, K);
      if (!TEST_FORMAT) {
        OwnMethods.WriteFile(result_detail_path, true, write_line);
        OwnMethods.WriteFile(result_avg_path, true, write_line);
      }

      String head_line = "visited_count\tqueue_time\tget_iterator_time\titerate_time\t"
          + "total_time\taccess_pages\n";
      if (!TEST_FORMAT)
        OwnMethods.WriteFile(result_avg_path, true, "nodeCount\t" + head_line);

      for (int nodeCount : nodeCountList) {
        String querygraph_path = String.format("%s/%d.txt", querygraphDir, nodeCount);
        ArrayList<Query_Graph> queryGraphs =
            Util.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
        Query_Graph query_Graph = queryGraphs.get(query_id);

        write_line = nodeCount + "\n" + head_line;
        if (!TEST_FORMAT)
          OwnMethods.WriteFile(result_detail_path, true, write_line);

        SpatialFirst_List spatialFirst =
            new SpatialFirst_List(db_path, dataset, graph_pos_map_list);

        ArrayList<Long> time_queue = new ArrayList<Long>();
        ArrayList<Long> time_get_iterator = new ArrayList<Long>();
        ArrayList<Long> time_iterate = new ArrayList<Long>();
        ArrayList<Long> total_time = new ArrayList<Long>();
        ArrayList<Long> visited_spatial_count = new ArrayList<Long>();
        ArrayList<Long> page_hit = new ArrayList<Long>();

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
            Util.println(query_Graph);
            Util.println(String.format("%d : %s", i, rectangle.toString()));

            start = System.currentTimeMillis();
            spatialFirst.LAGAQ_KNN(query_Graph, K);
            time = System.currentTimeMillis() - start;

            time_get_iterator.add(spatialFirst.get_iterator_time);
            time_iterate.add(spatialFirst.iterate_time);
            total_time.add(time);
            visited_spatial_count.add((long) spatialFirst.visit_spatial_object_count);
            page_hit.add(spatialFirst.page_hit_count);
            Util.println("Page access:" + spatialFirst.page_hit_count);
            time_queue.add(spatialFirst.queue_time);

            write_line = String.format("%d\t%d\t", visited_spatial_count.get(i), time_queue.get(i));
            write_line += String.format("%d\t", time_get_iterator.get(i));
            write_line += String.format("%d\t%d\t", time_iterate.get(i), total_time.get(i));
            write_line += String.format("%d\n", page_hit.get(i));
            if (!TEST_FORMAT)
              OwnMethods.WriteFile(result_detail_path, true, write_line);
          }

          spatialFirst.dbservice.shutdown();

          OwnMethods.ClearCache(password);
          Thread.currentThread();
          Thread.sleep(5000);

          spatialFirst.dbservice =
              new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));

        }
        spatialFirst.dbservice.shutdown();

        write_line = String.valueOf(nodeCount) + "\t";
        write_line += String.format("%d\t%d\t", Util.Average(visited_spatial_count),
            Util.Average(time_queue));
        write_line += String.format("%d\t", Util.Average(time_get_iterator));
        write_line +=
            String.format("%d\t%d\t", Util.Average(time_iterate), Util.Average(total_time));
        write_line += String.format("%d\n", Util.Average(page_hit));
        if (!TEST_FORMAT)
          OwnMethods.WriteFile(result_avg_path, true, write_line);

      }
      OwnMethods.WriteFile(result_detail_path, true, "\n");
      OwnMethods.WriteFile(result_avg_path, true, "\n");
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

}
