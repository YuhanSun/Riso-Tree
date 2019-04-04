import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import commons.Config;
import commons.Util;
import experiment.DataProcess;
import graph.Construct_RisoTree;
import graph.LoadDataNoOSM;

public class Driver {

  // function names
  private static enum FunctionName {
    convertSingleToBidirectinalGraph, // data preprocess

    tree, containID, // tree construction
    LoadNonSpatialEntity, GetSpatialNodeMap, LoadGraphEdges, CalculateCount, LoadAll, // graph load
    constructPN, loadPN, // PN load

    /**
     * for wikidata
     */
    wikiGenerateContainSpatialID, // one time prepare
    wikiConstructRTree, wikiConstructPNTime, //
    wikiConstructPNTimeSingleHop, wikiLoadPN,//
  }

  private static FunctionName getFunctionEnum(String function) {
    for (FunctionName functionName : FunctionName.values()) {
      if (function.equals(functionName.name())) {
        return functionName;
      }
    }
    return null;
  }

  private String[] args = null;
  private Options options = new Options();

  private String help = "h";
  private String function = "f";
  private String graphPath = "gp";
  private String entityPath = "ep";
  private String labelListPath = "lp";
  private String dbPath = "dp";
  private String dataset = "d";
  private String dataDir = "dataDir";

  // Construct_RisoTree
  private String containIDPath = "c";
  private String labelStrMapPath = "labelStrMapPath";

  // Load data
  private String mapPath = "mapPath";

  private String MAX_HOPNUM = "MAX_HOPNUM";
  private String hop = "hop";
  private String PNPathAndPrefix = "PNPrefix";


  public Driver(String[] args) {
    this.args = args;
    options.addOption(help, "help", false, "show help.");
    options.addOption(function, "function", true, "function name");
    options.addOption(dataDir, "data directory", true, "the directory includes all data files");
    options.addOption(graphPath, "graph-path", true, "graph path");
    options.addOption(entityPath, "entity-path", true, "entity path");
    options.addOption(labelListPath, "labellist-path", true, "label list path");
    options.addOption(dbPath, "db-path", true, "db path");
    options.addOption(dataset, "dataset", true, "dataset for naming the layer");
    options.addOption(containIDPath, "containId-path", true, "path for containID.txt");
    options.addOption(mapPath, "map path", true, "path for the map from entity id to neo4j id");
    options.addOption(labelStrMapPath, "labelStrMapPath", true,
        "the map from graph id to String label (name)");


    options.addOption(MAX_HOPNUM, "MAX_HOPNUM", true, "MAX_HOPNUM of RisoTree");
    options.addOption(hop, "hop", true, "hop");
    options.addOption(PNPathAndPrefix, "PNPathAndPrefix", true, "Path Neighbor file path preffix");
  }

  public void parser() {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
      Option[] options = cmd.getOptions();
      for (Option option : options) {
        Util.println(String.format("<%s, %s>", option, option.getValue()));
      }

      if (cmd.hasOption("h")) {
        help();
      }

      if (cmd.hasOption(function)) {
        String functionNameString = cmd.getOptionValue(function);
        FunctionName functionName = getFunctionEnum(functionNameString);
        switch (functionName) {
          case convertSingleToBidirectinalGraph:
            DataProcess.convertSingleToBidirectinalGraph(cmd.getOptionValue(dataDir));
            break;
          case tree:
            new LoadDataNoOSM(new Config(), true).batchRTreeInsertOneHopAware(
                cmd.getOptionValue(dbPath), cmd.getOptionValue(dataset),
                cmd.getOptionValue(graphPath), cmd.getOptionValue(entityPath),
                cmd.getOptionValue(labelListPath));
            break;
          case containID:
            new Construct_RisoTree(new Config(), true).generateContainSpatialID(
                cmd.getOptionValue(dbPath), cmd.getOptionValue(dataset),
                cmd.getOptionValue(containIDPath));
            break;

          case LoadNonSpatialEntity:
            new LoadDataNoOSM(new Config(), true).LoadNonSpatialEntity(
                cmd.getOptionValue(entityPath), cmd.getOptionValue(labelListPath),
                cmd.getOptionValue(dbPath), cmd.getOptionValue(mapPath));
            break;
          case GetSpatialNodeMap:
            new LoadDataNoOSM(new Config(), true).GetSpatialNodeMap(cmd.getOptionValue(dbPath),
                cmd.getOptionValue(mapPath));
            break;
          case LoadGraphEdges:
            new LoadDataNoOSM(new Config(), true).LoadGraphEdges(cmd.getOptionValue(mapPath),
                cmd.getOptionValue(dbPath), cmd.getOptionValue(graphPath));
            break;
          case CalculateCount:
            new LoadDataNoOSM(new Config(), true).CalculateCount(cmd.getOptionValue(dbPath),
                cmd.getOptionValue(dataset));
            break;
          case LoadAll: // load all cases in graph load
            new LoadDataNoOSM(new Config(), true).LoadNonSpatialEntity(
                cmd.getOptionValue(entityPath), cmd.getOptionValue(labelListPath),
                cmd.getOptionValue(dbPath), cmd.getOptionValue(mapPath));
            new LoadDataNoOSM(new Config(), true).GetSpatialNodeMap(cmd.getOptionValue(dbPath),
                cmd.getOptionValue(mapPath));
            new LoadDataNoOSM(new Config(), true).LoadGraphEdges(cmd.getOptionValue(mapPath),
                cmd.getOptionValue(dbPath), cmd.getOptionValue(graphPath));
            new LoadDataNoOSM(new Config(), true).CalculateCount(cmd.getOptionValue(dbPath),
                cmd.getOptionValue(dataset));
            break;

          case constructPN:
            new Construct_RisoTree(new Config(), true).constructPNTime(
                cmd.getOptionValue(containIDPath), cmd.getOptionValue(dbPath),
                cmd.getOptionValue(graphPath), cmd.getOptionValue(labelListPath),
                Integer.parseInt(cmd.getOptionValue(MAX_HOPNUM)),
                cmd.getOptionValue(PNPathAndPrefix));
            break;
          case loadPN:
            new Construct_RisoTree(new Config(), true).LoadPN(cmd.getOptionValue(PNPathAndPrefix),
                Integer.parseInt(cmd.getOptionValue(MAX_HOPNUM)), cmd.getOptionValue(dbPath));
            break;

          /**
           * for wikidata
           */
          case wikiConstructRTree:
            new LoadDataNoOSM(new Config(), true).wikiConstructRTree(cmd.getOptionValue(dbPath),
                cmd.getOptionValue(dataset), cmd.getOptionValue(entityPath));
            break;
          case wikiGenerateContainSpatialID:
            Construct_RisoTree.wikiGenerateContainSpatialID(cmd.getOptionValue(dbPath),
                cmd.getOptionValue(dataset), cmd.getOptionValue(containIDPath));
            break;
          case wikiConstructPNTime:
            Construct_RisoTree.wikiConstructPNTime(cmd.getOptionValue(containIDPath),
                cmd.getOptionValue(dbPath), cmd.getOptionValue(graphPath),
                cmd.getOptionValue(labelListPath), cmd.getOptionValue(labelStrMapPath),
                Integer.parseInt(cmd.getOptionValue(MAX_HOPNUM)),
                cmd.getOptionValue(PNPathAndPrefix));
            break;
          case wikiConstructPNTimeSingleHop:
            Construct_RisoTree.wikiConstructPNSingleHop(cmd.getOptionValue(containIDPath),
                cmd.getOptionValue(dbPath), cmd.getOptionValue(graphPath),
                cmd.getOptionValue(labelListPath), cmd.getOptionValue(labelStrMapPath),
                Integer.parseInt(cmd.getOptionValue(hop)), cmd.getOptionValue(PNPathAndPrefix));
            break;
          case wikiLoadPN:
            Construct_RisoTree.wikiLoadPN(cmd.getOptionValue(PNPathAndPrefix),
                Integer.parseInt(cmd.getOptionValue(hop)), cmd.getOptionValue(dbPath));
            break;
          default:
            Util.println(String.format("Function %s does not exist!", functionNameString));
            break;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  private void help() {
    HelpFormatter formater = new HelpFormatter();
    formater.printHelp("Main", options);
    System.exit(0);
  }


  public static void main(String[] args) {
    // args = new String[] {"-h"};
    // construct rtree.
    // args = new String[] {"-f", "constructRTreeWikidata", "-dp",
    // "D:/Neo4jData/neo4jDatabases/database-ae5a632c-076d-42a6-ac8d-61f8f72af7f9/installation-3.4.12/data/databases/graph.db",
    // "-d", "wikidata", "-ep", "D:/Project_Data/wikidata-20180308-truthy-BETA.nt/entity.txt"};

    // run only once.
    // DataProcess.convertSingleToBidirectinalGraph();
    Util.println(Arrays.toString(args));
    Driver driver = new Driver(args);
    driver.parser();

  }

}
