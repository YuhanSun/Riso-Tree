package experiment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeSet;
import commons.Config;
import commons.OwnMethods;
import commons.Util;
import commons.Config.system;

public class DataProcess {

  static Config config = new Config();
  static String dataset = config.getDatasetName();
  static String version = config.GetNeo4jVersion();
  static system systemName = config.getSystemName();
  static int MAX_HOPNUM = config.getMaxHopNum();
  static int nonspatial_label_count = config.getNonSpatialLabelCount();

  static String db_path;
  static String graph_path;
  static String entityPath;
  static String label_list_path;
  static String log_path;
  static String dataDirectory;

  static ArrayList<Integer> labels;// all labels in the graph

  static void initParameters() {
    switch (systemName) {
      case Ubuntu:
        db_path = String.format(
            "/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version,
            dataset);
        graph_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
        entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
        label_list_path =
            String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/label.txt", dataset);
        log_path = String.format("/mnt/hgfs/Experiment_Result/Riso-Tree/%s/set_label.log", dataset);
        break;
      case Windows:
        dataDirectory = "D:\\Ubuntu_shared\\GeoMinHop\\data";
        db_path = String.format("%s\\%s\\%s_%s\\data\\databases\\graph.db", dataDirectory, dataset,
            version, dataset);
        graph_path = String.format("%s\\%s\\graph.txt", dataDirectory, dataset);
        entityPath = String.format("%s\\%s\\entity.txt", dataDirectory, dataset);
        label_list_path = String.format("%s\\%s\\label.txt", dataDirectory, dataset);
        log_path = String.format("%s\\%s\\set_label.log", dataDirectory, dataset);
      default:
        break;
    }

    if (nonspatial_label_count == 1)
      labels = new ArrayList<Integer>(Arrays.asList(0, 1));
    else {
      labels = new ArrayList<Integer>(1 + nonspatial_label_count);
      labels.add(1);
      for (int i = 0; i < nonspatial_label_count; i++)
        labels.add(i + 2);
    }
  }

  public static void main(String[] args) {
    initParameters();

    convertSingleToBidirectinalGraph();
  }

  public static void convertSingleToBidirectinalGraph() {
    // original version
    // String singleGraphPath = String.format("%s\\%s\\new_graph.txt", dataDirectory, dataset);
    // String bidirectionGraphPath = String.format("%s\\%s\\graph.txt", dataDirectory, dataset);

    // server version
    dataDirectory = "/hdd2/data/ysun138/RisoTree";
    String singleGraphPath = String.format("%s/%s/graph_single.txt", dataDirectory, dataset);
    String bidirectionGraphPath = String.format("%s/%s/graph.txt", dataDirectory, dataset);

    Util.println("read single graph from " + singleGraphPath);
    ArrayList<ArrayList<Integer>> graph = OwnMethods.ReadGraph(singleGraphPath);

    Util.println("generate bidirectional graph");
    ArrayList<TreeSet<Integer>> bidirectionalGraph =
        OwnMethods.singleDirectionalToBidirectionalGraph(graph);

    Util.println("output birectional graph to " + bidirectionGraphPath);
    OwnMethods.writeGraphTreeSet(bidirectionalGraph, bidirectionGraphPath);
  }

}
