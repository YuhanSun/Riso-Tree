package experiment;

import java.util.ArrayList;
import java.util.Arrays;
import commons.Config;
import commons.OwnMethods;
import commons.Util;
import commons.Config.system;
import graph.Construct_RisoTree;
import graph.LoadDataNoOSM;

public class ConstructionTime {
  static Config config = new Config();
  static String dataset = config.getDatasetName();
  static String version = config.GetNeo4jVersion();
  static system systemName = config.getSystemName();
  static int MAX_HOPNUM = config.getMaxHopNum();
  static int nonspatial_label_count = config.getNonSpatialLabelCount();

  static String db_path, graphPath, entityPath, containIDPath, PNPath;
  static ArrayList<Integer> labels;// all labels in the graph

  public static void initializeParameters() {
    switch (systemName) {
      case Ubuntu:
        db_path = String.format(
            "/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version,
            dataset);
        graphPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
        entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
        containIDPath =
            String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/containID.txt", dataset);
        PNPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/PathNeighbors", dataset);
        break;
      case Windows:
        db_path = String.format(
            "D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset,
            version, dataset);
        graphPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\graph.txt", dataset);
        entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
        containIDPath =
            String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\containID.txt", dataset);
        PNPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\PathNeighbors", dataset);
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

  static void initParametersServer() {
    systemName = Config.system.Ubuntu;
    dataset = Config.Datasets.wikidata_100.name();
    String dir = "/hdd2/data/ysun138/RisoTree/" + dataset;
    MAX_HOPNUM = config.getMaxHopNum();
    db_path = dir + "/neo4j-community-3.1.1/data/databases/graph.db";
    graphPath = dir + "/graph.txt";
    // label_list_path = dir + "/label.txt";
    // graph_node_map_path = dir + "/node_map_RTree.txt";
    containIDPath = dir + "/containID.txt";
    PNPath = dir + "/PathNeighbors";
    nonspatial_label_count = 100;
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
    initializeParameters();
    // LoadDataNoOSM loadDataNoOSM = new LoadDataNoOSM();
    // long RTreeTime = loadDataNoOSM.batchRTreeInsertTime();

    datasetConstructTime();
    // ratioConstructTime();


  }

  static public void ratioConstructTime() {
    try {

      ArrayList<String> dataset_a =
          new ArrayList<String>(Arrays.asList(Config.Datasets.Patents_100_random_80.name(),
              Config.Datasets.Patents_100_random_60.name(),
              Config.Datasets.Patents_100_random_40.name(),
              Config.Datasets.Patents_100_random_20.name()));
      // ArrayList<String> dataset_a = new
      // ArrayList<String>(Arrays.asList(Config.Datasets.Gowalla_50.name()));

      String resultPath =
          "D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\constructionTimeRatio.txt";
      for (String dataset : dataset_a) {
        Config config = new Config();
        config.setDatasetName(dataset);
        LoadDataNoOSM loadDataNoOSM = new LoadDataNoOSM(config);
        long RTreeTime = loadDataNoOSM.batchRTreeInsertTime();

        Construct_RisoTree construct_RisoTree = new Construct_RisoTree(config);
        // construct_RisoTree.generateContainSpatialID();
        ArrayList<Long> PNTime = construct_RisoTree.constructPNTime();

        String writeStr =
            String.format("%s\t%d\t%d\t%d\n", dataset, RTreeTime, PNTime.get(0), PNTime.get(1));
        OwnMethods.WriteFile(resultPath, true, writeStr);
      }

    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
      System.exit(-1);
    }
  }

  static public void datasetConstructTime() {
    try {

      ArrayList<String> dataset_a = new ArrayList<String>(Arrays.asList(
          // Config.Datasets.Patents_100_random_80.name(),
          // Config.Datasets.go_uniprot_100_random_80.name()
          // Config.Datasets.Gowalla_100.name(),
          // Config.Datasets.foursquare_100.name()
          Config.Datasets.Yelp_100.name()));

      String resultDir = "D:\\Google_Drive\\Experiment_Result\\Riso-Tree";
      String resultPath = resultDir + "\\constructionTimeDataset.txt";

      // String dir = "/hdd2/data/ysun138/RisoTree/result";
      // String resultPath = dir + "/constructionTimeDataset.txt";

      if (!OwnMethods.pathExist(resultDir)) {
        Util.println(resultDir + " does not exist!");
        System.exit(-1);
      }

      for (String dataset : dataset_a) {
        Config config = new Config();
        config.setDatasetName(dataset);
        long RTreeTime = 0;
        LoadDataNoOSM loadDataNoOSM = new LoadDataNoOSM(config, false);
        RTreeTime = loadDataNoOSM.batchRTreeInsertTime();

        Construct_RisoTree construct_RisoTree = new Construct_RisoTree(config, false);
        // construct_RisoTree.generateContainSpatialID();
        ArrayList<Long> PNTime = construct_RisoTree.constructPNTime();

        String writeStr = String.format("%s\t%d\t%d\n", dataset, RTreeTime, PNTime.get(0));
        // String writeStr = String.format("%s\t%d\t%d\t%d\n",
        // dataset, RTreeTime, PNTime.get(0), PNTime.get(1));
        OwnMethods.WriteFile(resultPath, true, writeStr);
      }

    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
      System.exit(-1);
    }
  }

}
