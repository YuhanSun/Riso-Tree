package commons;

public class Enums {

  public static enum system {
    Ubuntu, Windows, MacOS
  }

  public static enum Explain_Or_Profile {
    Explain, Profile, Nothing
  }

  public static enum Datasets {
    Patents_100_random_80, Patents_100_random_60, Patents_100_random_40, Patents_100_random_20,

    Patents_10_random_20, Patents_10_random_80, Patents_1_random_80, Patents_1_random_20, go_uniprot_100_random_80, foursquare, foursquare_10, foursquare_100, Gowalla, Gowalla_10, Gowalla_100, Gowalla_25, Gowalla_50, Yelp, Yelp_10, Yelp_100, //
    wikidata, wikidata_2, wikidata_100,
  }

  public static enum ClearCacheMethod {
    SINGLE, DOUBLE, NULL,
  }

  public static enum ExperimentMethod {
    NAIVE, RISOTREE, SPATIAL_FIRST,
  }

  public static enum QueryType {
    LAGAQ_RANGE, LAGAQ_JOIN, LAGAQ_KNN,
  }

  public static enum QueryStatistic {
    check_path_time, get_iterator_time, iterate_time, set_label_time, remove_label_time, result_count, overlap_leaf_node_count, candidate_count, run_time, page_hit_count, check_paths_time, visit_spatial_object_count, check_overlap_time, /////
    spatial_time, graph_time, overlap_object_count,
  }

  public static enum MaintenanceStatistic {
    runTime, getGraphNodePNTime, convertIdTime, updatePNTimeTotal, updateSafeNodesTime, getRTreeLeafNodeTime, updateLeafNodePNTime, createEdgeTime, commitTime
//    outString += "updateLeafNodeTimeMap: " + maintenance.updateLeafNodeTimeMap + "\n";
    , safeCaseHappenCount, safeCount, safeCountAfterExperiment, visitedNodeCount, updatePNCount
  }
}
