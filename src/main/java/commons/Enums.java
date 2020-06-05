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

}
