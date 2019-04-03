package experiment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import commons.Config;
import commons.Config.system;
import commons.Query_Graph;
import commons.Util;

public class ExperimentLabelCount {

  static Config config = new Config();
  static system systemName = config.getSystemName();
  static String querygraphDir, dataDir;

  public static List<Integer> labelCountList = Arrays.asList(10, 25, 50);

  public static void initializeParameters() {
    switch (systemName) {
      case Ubuntu:
        // db_path =
        // String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db",
        // version, dataset);
        // graph_pos_map_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset +
        // "/node_map_RTree.txt";
        // entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt",
        // dataset);
        querygraphDir =
            String.format("/mnt/hgfs/Google_Drive/Projects/risotree/query/query_graph/labelCount/");
        dataDir = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/");
        // spaPredicateDir =
        // String.format("/mnt/hgfs/Google_Drive/Projects/risotree/query/spa_predicate/%s",
        // dataset);
        // resultDir = String.format("/mnt/hgfs/Google_Drive/Experiment_Result/Riso-Tree/%s",
        // dataset);
        //// resultDir =
        // String.format("/mnt/hgfs/Google_Drive/Experiment_Result/Riso-Tree/%s/switch_point",
        // dataset);
        break;
      case Windows:
        // db_path =
        // String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db",
        // dataset, version, dataset);
        // graph_pos_map_path = "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset +
        // "\\node_map_RTree.txt";
        // entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt",
        // dataset);
        querygraphDir =
            String.format("D:\\Google_Drive\\Projects\\risotree\\query\\query_graph\\labelCount\\");
        dataDir = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\");
        // spaPredicateDir = String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\spa_predicate\\%s",
        // dataset);
        // resultDir = String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s", dataset);
        break;
    }
  }

  public static void main(String[] args) {
    try {
      initializeParameters();
      convertQueryGraph();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * Already has query graph for 100 labels. Only need to convert it to other by changing label list
   * of the query graph
   */
  public static void convertQueryGraph() {
    String oriDataset = Config.Datasets.Gowalla_100.name();
    String oriQueryGraphPath = querygraphDir + oriDataset + "\\5.txt";
    // OwnMethods.Print(query_Graphs.size());
    for (int labelCount : labelCountList) {
      ArrayList<Query_Graph> query_Graphs = Util.ReadQueryGraph_Spa(oriQueryGraphPath, 10);
      String curDataset = "Gowalla_" + labelCount;
      String curQueryGraphPath = querygraphDir + curDataset + "\\5.txt";

      Random random = new Random();
      for (Query_Graph query_Graph : query_Graphs) {
        for (int i = 0; i < query_Graph.label_list.length; i++) {
          if (query_Graph.label_list[i] != 1)
            query_Graph.label_list[i] = random.nextInt(labelCount) + 2;
        }
      }
      Util.WriteQueryGraph(curQueryGraphPath, query_Graphs);
    }
  }

}
