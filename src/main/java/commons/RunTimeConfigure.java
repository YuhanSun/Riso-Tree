package commons;

import commons.Enums.ClearCacheMethod;

public class RunTimeConfigure {
  public static final String dataset = "wikidata";
  public static final String suffix = "node_edges";
  // public static final String suffix = "Gleenes_1.0_-1_new_version";
  // public static final String suffix = "Gleenes_0.9_-1_new_version";
  // public static final String suffix = "Gleenes_1.0_640_new_version";

  // String dbPath =
  // "D:\\Project_Data\\wikidata-20180308-truthy-BETA.nt\\neo4j-community-3.4.12_risotree_test\\data\\databases\\graph.db";
  // String dbPath =
  // "D:\\Project_Data\\wikidata-20180308-truthy-BETA.nt\\neo4j-community-3.4.12_Gleenes_1.0_40_new_version\\data\\databases\\graph.db";
  public static final String dbPath =
      String.format("D:/Project_Data/RisoTree/%s/neo4j-community-3.4.12_%s/data/databases/graph.db",
          dataset, suffix);

  public static final String queryDir =
      "D:\\Google_Drive\\Projects\\risotree\\cypher_query\\" + dataset;
  public static final String queryPath = queryDir + "\\2_0.000001";
  // String queryPath = queryDir + "\\4_0.0001";
  public static final int queryCount = 2;

  public static final boolean clearCache = false;
  public static final Enums.ClearCacheMethod clearMethod = Enums.ClearCacheMethod.DOUBLE;
  public static final String password = "syh19910205";

}
