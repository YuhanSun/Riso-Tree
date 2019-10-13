package experiment;

import commons.Config.ClearCacheMethod;
import commons.Config.ExperimentMethod;

public class Alpha {
  public static void alphaExperiment(String dbPath, String dataset, ExperimentMethod method,
      int MAX_HOP, String queryPaths, int queryCount, String password, boolean clearCache,
      ClearCacheMethod clearCacheMethod, String outputPath) throws Exception {
    ExperimentUtil.runExperimentQueryPathList(dbPath, dataset, method, MAX_HOP, queryPaths,
        queryCount, password, clearCache, clearCacheMethod, outputPath);
  }

}
