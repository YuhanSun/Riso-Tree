import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import commons.Config;
import commons.Enums.ClearCacheMethod;
import commons.Enums.ExperimentMethod;
import commons.GraphUtil;
import commons.Util;
import dataprocess.Wikidata;
import experiment.Alpha;
import experiment.Analyze;
import experiment.DataProcess;
import experiment.ExperimentSpaceSelectivity;
import experiment.MaintenanceExperiment;
import experiment.MaxPNSize;
import experiment.Prepare;
import graph.Construct_RisoTree;
import graph.LoadDataNoOSM;

public class Driver {

  // function names
  private static enum FunctionName {
    convertSingleToBidirectinalGraph, // data preprocess
    refineGraphPropertyEdge, // wikidata preprocess

    tree, containID, // tree construction
    LoadNonSpatialEntity, GetSpatialNodeMap, LoadGraphEdges, loadGraphEdgesNoMap, CalculateCount, LoadAll, // graph
                                                                                                           // load
    constructPN, loadPN, // PN load

    /**
     * analyze
     */
    getSpatialIndexSize, // rtree spatial index
    getPNSizeDistribution, getPNNonEmptyCount, // PN
    overlapAnalysis, areaAnalysis, treeNodesAvgArea, // area
    degreeSD, degreeAvg, // graph degree
    visualizeLeafNodes,

    /**
     * for wikidata
     */
    wikidataLoadGraph, wikiLoadEdges, // load graph nodes and edges and spatial attributes
    wikisetZeroOneHopPNForSpatialNodes, wikigenerateZeroOneHopPNForSpatialNodes, //

    wikiGenerateContainSpatialID, generateSafeNodes, // one time prepare
    wikiConstructRTree, wikiConstructPNTime, //
    wikiConstructPNTimeSingleHop, wikiLoadPN, wikiLoadAllHopPN, //
    wikiConstructPNTimeSingleHopNoGraphDb,

    /**
     * experiment
     */
    generateRandomLabel, singleLabelListToLabelGraph, // for single label graph prepare
    generateQuery, // prepare
    maxPNSizeRisoTreeQuery, maxPNSizeRisoTreeQueryMultiple, // query
    alphaExperiment, // alpha
    selectivityExperiment, // selectivity
    selectivityExperimentSingleMethod,
    knnCount,

    /**
     * add experiment
     */
    convertGraphToEdgeFormat, sampleFile, generateRandomAddEdgeSafeNodes, removeEdgesFromGraphFile, removeEdgesFromDb, // one
                                                                                                                       // time
    // prepare
    addEdgeExperiment,
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

  private static final String help = "h";
  private static final String function = "f";
  private static final String graphPath = "gp";
  private static final String entityPath = "ep";
  private static final String labelListPath = "lp";
  private static final String dbPath = "dp";
  private static final String dataset = "d";
  private static final String dataDir = "dataDir";

  // Construct_RisoTree
  private static final String containIDPath = "c";
  private static final String labelStrMapPath = "labelStrMapPath";
  private static final String spatialNodePNPath = "spatialNodePNPath";

  // Load data
  private static final String mapPath = "mapPath";
  private static final String entityStringLabelMapPath = "entityStringLabelMapPath";
  private static final String graphPropertyEdgePath = "graphPropertyEdgePath";
  private static final String propertyMapPath = "propertyMapPath";



  private static final String MAX_HOPNUM = "MAX_HOPNUM";
  private static final String hop = "hop";
  private static final String hopListStr = "hopListStr";
  private static final String PNPathAndPrefix = "PNPrefix";

  private static final String maxPNSize = "maxPNSize";
  private static final String alpha = "alpha";

  // Analyze
  private static final String inputPath = "inputPath";
  private static final String outputPath = "outputPath";

  private static final String input1 = "input1";
  private static final String input2 = "input2";

  // Experiment
  private static final String labelCount = "labelCount"; // for single-label graph
  private static final String nodeCount = "nodeCount";
  // private String startSelectivity = "startSelectivity";
  // private String endSelectivity = "endSelectivity";
  private static final String selectivitiesStr = "selectivitiesStr";
  private static final String queryCount = "queryCount";
  private static final String queryPath = "queryPath";
  private static final String queryId = "queryId";
  private static final String password = "password";
  private static final String method = "method";
  private static final String clearCache = "clearCache";
  private static final String clearCacheMethod = "clearCacheMethod";

  private static final String ratio = "ratio";
  private static final String edgePath = "edgePath";
  private static final String safeNodesPath = "safeNodesPath";


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
    options.addOption(spatialNodePNPath, "spatialNodePNPath", true, "spatialNodePNPath");
    options.addOption(entityStringLabelMapPath, "entityStringLabelMapPath", true,
        "the map from entity id to String label (name)");
    options.addOption(graphPropertyEdgePath, "graphPropertyEdgePath", true,
        "graphPropertyEdgePath");

    options.addOption(MAX_HOPNUM, "MAX_HOPNUM", true, "MAX_HOPNUM of RisoTree");
    options.addOption(hop, "hop", true, "hop");
    options.addOption(PNPathAndPrefix, "PNPathAndPrefix", true, "Path Neighbor file path preffix");
    options.addOption(hopListStr, "hopListStr", true, "hop list sep comma");


    options.addOption(maxPNSize, "maxPNSize", true, "Path Neighbor maximum size");
    options.addOption(alpha, "alpha", true, "alpha");

    // Analyze
    options.addOption(outputPath, "outputPath", true, "The output path for analyze");
    options.addOption(inputPath, "inputPath", true, "The input path for analyze");
    options.addOption(input1, "inputPath1", true, "The input 1 for analyze");
    options.addOption(input2, "inputPath2", true, "The input 2 for analyze");

    // Experiment
    options.addOption(labelCount, "labelCount", true,
        "the number of labels in graphs with random-generated label");
    options.addOption(nodeCount, "nodeCount", true, "The node count in the query graph");
    // options.addOption(startSelectivity, "startSelectivity", true, "start selectivity");
    // options.addOption(endSelectivity, "endSelectivity", true, "end selectivity");
    options.addOption(selectivitiesStr, "selectivities string", true,
        "separated by comma without []");
    options.addOption(queryCount, "queryCount", true, "the number of queries to be generated");
    options.addOption(queryPath, "queryPath", true, "path of the query file");
    options.addOption(queryId, "queryId", true, "id of the query in the query file");
    options.addOption(password, "password", true, "password");
    options.addOption(method, "method", true, "experiment method");
    options.addOption(clearCache, "clearCache", true, "whether to clear the cache or not");
    options.addOption(clearCacheMethod, "clearCacheMethod", true, "clearCacheMethod");

    options.addOption(ratio, "ratio", true, "sampling ratio");
    options.addOption(edgePath, "edgePath", true, "edge path");
    options.addOption(safeNodesPath, "safeNodesPath", true, "safeNodesPath");
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
          case refineGraphPropertyEdge:
            Wikidata.refineGraphPropertyEdge(cmd.getOptionValue(inputPath),
                cmd.getOptionValue(graphPath), cmd.getOptionValue(outputPath));
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
          case loadGraphEdgesNoMap:
            LoadDataNoOSM.loadGraphEdgesNoMap(cmd.getOptionValue(dbPath),
                cmd.getOptionValue(graphPath));
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
           * analyze
           */
          case getSpatialIndexSize:
            Analyze.getSpatialIndexSize(cmd.getOptionValue(dbPath), cmd.getOptionValue(dataset),
                cmd.getOptionValue(outputPath));
            break;
          case getPNSizeDistribution:
            Analyze.getPNSizeDistribution(cmd.getOptionValue(dbPath), cmd.getOptionValue(dataset),
                cmd.getOptionValue(outputPath));
            break;
          case getPNNonEmptyCount:
            Analyze.getPNNonEmptyCount(cmd.getOptionValue(inputPath),
                cmd.getOptionValue(outputPath));
            break;
          case overlapAnalysis:
            Analyze.leafNodesOverlapAnalysisInMemory(cmd.getOptionValue(dbPath),
                cmd.getOptionValue(dataset), cmd.getOptionValue(outputPath));
            break;
          case areaAnalysis:
            Analyze.leafNodesAvgArea(cmd.getOptionValue(dbPath), cmd.getOptionValue(dataset),
                cmd.getOptionValue(outputPath));
            break;
          case treeNodesAvgArea:
            Analyze.treeNodesAvgArea(cmd.getOptionValue(dbPath), cmd.getOptionValue(dataset),
                cmd.getOptionValue(outputPath));
            break;
          case degreeAvg:
            Analyze.degreeAvg(cmd.getOptionValue(graphPath), cmd.getOptionValue(entityPath),
                cmd.getOptionValue(outputPath));
            break;
          case visualizeLeafNodes:
            Analyze.visualizeLeafNodes(cmd.getOptionValue(dbPath), cmd.getOptionValue(dataset),
                cmd.getOptionValue(input1), cmd.getOptionValue(input2),
                cmd.getOptionValue(outputPath));
            break;
          case degreeSD:
            Analyze.degreeSD(cmd.getOptionValue(graphPath), cmd.getOptionValue(outputPath));
            break;
          /**
           * for wikidata
           */
          case wikidataLoadGraph:
            Wikidata.loadAllEntities(cmd.getOptionValue(entityPath),
                cmd.getOptionValue(labelListPath), cmd.getOptionValue(entityStringLabelMapPath),
                cmd.getOptionValue(dbPath));
            break;
          case wikiLoadEdges:
            Wikidata.loadEdges(cmd.getOptionValue(graphPropertyEdgePath),
                cmd.getOptionValue(propertyMapPath), cmd.getOptionValue(dbPath));
            break;
          case wikisetZeroOneHopPNForSpatialNodes:
            Wikidata.setZeroOneHopPNForSpatialNodes(cmd.getOptionValue(dbPath),
                cmd.getOptionValue(graphPath), cmd.getOptionValue(labelListPath),
                cmd.getOptionValue(entityStringLabelMapPath),
                Integer.parseInt(cmd.getOptionValue(maxPNSize)));
            break;
          case wikigenerateZeroOneHopPNForSpatialNodes:
            Wikidata.generateZeroOneHopPNForSpatialNodes(cmd.getOptionValue(graphPath),
                cmd.getOptionValue(labelListPath), cmd.getOptionValue(entityPath),
                cmd.getOptionValue(entityStringLabelMapPath),
                Integer.parseInt(cmd.getOptionValue(maxPNSize)),
                Integer.parseInt(cmd.getOptionValue(MAX_HOPNUM)), cmd.getOptionValue(outputPath));
            break;
          case wikiConstructRTree:
            // new LoadDataNoOSM(new Config(), true).wikiConstructRTree(cmd.getOptionValue(dbPath),
            // cmd.getOptionValue(dataset), cmd.getOptionValue(entityPath));
            new LoadDataNoOSM(new Config(), true).wikiConstructRTree(cmd.getOptionValue(dbPath),
                cmd.getOptionValue(dataset), cmd.getOptionValue(entityPath),
                cmd.getOptionValue(spatialNodePNPath),
                Double.parseDouble(cmd.getOptionValue(alpha)),
                Integer.parseInt(cmd.getOptionValue(maxPNSize)));
            break;
          case wikiGenerateContainSpatialID:
            Construct_RisoTree.wikiGenerateContainSpatialID(cmd.getOptionValue(dbPath),
                cmd.getOptionValue(dataset), cmd.getOptionValue(containIDPath));
            break;
          case generateSafeNodes:
            Construct_RisoTree.generateSafeNodes(cmd.getOptionValue(PNPathAndPrefix),
                Integer.parseInt(cmd.getOptionValue(MAX_HOPNUM)), cmd.getOptionValue(entityPath),
                cmd.getOptionValue(outputPath));
            break;
          // case wikiConstructPNTime:
          // Construct_RisoTree.wikiConstructPNTime(cmd.getOptionValue(containIDPath),
          // cmd.getOptionValue(dbPath), cmd.getOptionValue(graphPath),
          // cmd.getOptionValue(labelListPath), cmd.getOptionValue(labelStrMapPath),
          // Integer.parseInt(cmd.getOptionValue(MAX_HOPNUM)),
          // cmd.getOptionValue(PNPathAndPrefix),
          // Integer.parseInt(cmd.getOptionValue(maxPNSize)));
          // break;
          case wikiConstructPNTimeSingleHop:
            Construct_RisoTree.wikiConstructPNSingleHop(cmd.getOptionValue(containIDPath),
                cmd.getOptionValue(dbPath), cmd.getOptionValue(graphPath),
                cmd.getOptionValue(labelListPath), cmd.getOptionValue(labelStrMapPath),
                Integer.parseInt(cmd.getOptionValue(hop)), cmd.getOptionValue(PNPathAndPrefix),
                Integer.parseInt(cmd.getOptionValue(maxPNSize)));
            break;
          case wikiConstructPNTimeSingleHopNoGraphDb:
            Construct_RisoTree.wikiConstructPNSingleHopNoGraphDb(cmd.getOptionValue(containIDPath),
                cmd.getOptionValue(graphPath), cmd.getOptionValue(labelListPath),
                cmd.getOptionValue(labelStrMapPath), Integer.parseInt(cmd.getOptionValue(hop)),
                cmd.getOptionValue(PNPathAndPrefix),
                Integer.parseInt(cmd.getOptionValue(maxPNSize)));
            break;
          case wikiLoadPN:
            Construct_RisoTree.wikiLoadPN(cmd.getOptionValue(PNPathAndPrefix),
                Integer.parseInt(cmd.getOptionValue(hop)), cmd.getOptionValue(dbPath));
            break;
          case wikiLoadAllHopPN:
            Construct_RisoTree.wikiLoadAllHopPN(cmd.getOptionValue(PNPathAndPrefix),
                cmd.getOptionValue(hopListStr), cmd.getOptionValue(dbPath),
                cmd.getOptionValue(containIDPath));
            break;
          // experiment
          case singleLabelListToLabelGraph:
            Prepare.singleLabelListToLabelGraph(cmd.getOptionValue(labelListPath),
                cmd.getOptionValue(outputPath), cmd.getOptionValue(labelStrMapPath));
            break;
          case generateRandomLabel:
            Prepare.generateRandomLabel(Integer.parseInt(cmd.getOptionValue(labelCount)),
                cmd.getOptionValue(entityPath), cmd.getOptionValue(labelListPath));
            break;
          case generateQuery:
            Prepare.generateExperimentCypherQuery(cmd.getOptionValue(graphPath),
                cmd.getOptionValue(entityPath), cmd.getOptionValue(labelListPath),
                cmd.getOptionValue(labelStrMapPath), cmd.getOptionValue(selectivitiesStr),
                Integer.parseInt(cmd.getOptionValue(queryCount)),
                Integer.parseInt(cmd.getOptionValue(nodeCount)), cmd.getOptionValue(outputPath));
            break;
          case maxPNSizeRisoTreeQuery:
            MaxPNSize.maxPNSizeRisoTreeQuery(cmd.getOptionValue(dbPath),
                cmd.getOptionValue(dataset), Integer.parseInt(cmd.getOptionValue(MAX_HOPNUM)),
                cmd.getOptionValue(queryPath), Integer.parseInt(cmd.getOptionValue(queryId)),
                cmd.getOptionValue(outputPath));
            break;
          case maxPNSizeRisoTreeQueryMultiple:
            MaxPNSize.maxPNSizeRisoTreeQueryMultiple(cmd.getOptionValue(dbPath),
                cmd.getOptionValue(dataset), Integer.parseInt(cmd.getOptionValue(MAX_HOPNUM)),
                cmd.getOptionValue(queryPath), Integer.parseInt(cmd.getOptionValue(queryCount)),
                cmd.getOptionValue(outputPath));
            break;
          case alphaExperiment:
            Alpha.alphaExperiment(cmd.getOptionValue(dbPath), cmd.getOptionValue(dataset),
                Integer.parseInt(cmd.getOptionValue(MAX_HOPNUM)), cmd.getOptionValue(queryPath),
                Integer.parseInt(cmd.getOptionValue(queryCount)), cmd.getOptionValue(password),
                Boolean.parseBoolean(cmd.getOptionValue(clearCache)),
                ClearCacheMethod.valueOf(cmd.getOptionValue(clearCacheMethod)),
                cmd.getOptionValue(outputPath));
            break;
          case selectivityExperiment:
            ExperimentSpaceSelectivity.selectivityExperiment(cmd.getOptionValue(dbPath),
                cmd.getOptionValue(dataset), Integer.parseInt(cmd.getOptionValue(MAX_HOPNUM)),
                cmd.getOptionValue(queryPath), Integer.parseInt(cmd.getOptionValue(queryCount)),
                cmd.getOptionValue(password), Boolean.parseBoolean(cmd.getOptionValue(clearCache)),
                ClearCacheMethod.valueOf(cmd.getOptionValue(clearCacheMethod)),
                cmd.getOptionValue(outputPath));
            break;
          case selectivityExperimentSingleMethod:
            ExperimentSpaceSelectivity.selectivityExperimentSingleMethod(
                ExperimentMethod.valueOf(cmd.getOptionValue(method)), cmd.getOptionValue(dbPath),
                cmd.getOptionValue(dataset), Integer.parseInt(cmd.getOptionValue(MAX_HOPNUM)),
                cmd.getOptionValue(queryPath), Integer.parseInt(cmd.getOptionValue(queryCount)),
                cmd.getOptionValue(password), Boolean.parseBoolean(cmd.getOptionValue(clearCache)),
                ClearCacheMethod.valueOf(cmd.getOptionValue(clearCacheMethod)),
                cmd.getOptionValue(outputPath));
            break;
          // add prepare one time run
          case convertGraphToEdgeFormat:
            GraphUtil.convertGraphToEdgeFormat(cmd.getOptionValue(graphPath),
                cmd.getOptionValue(graphPropertyEdgePath));
            break;
          case sampleFile:
            MaintenanceExperiment.sampleFile(cmd.getOptionValue(inputPath),
                Double.parseDouble(cmd.getOptionValue(ratio)), cmd.getOptionValue(outputPath));
            break;
          case generateRandomAddEdgeSafeNodes:
            MaintenanceExperiment.generateRandomAddEdgeSafeNodes(cmd.getOptionValue(graphPath),
                cmd.getOptionValue(safeNodesPath), Integer.parseInt(cmd.getOptionValue(nodeCount)),
                cmd.getOptionValue(outputPath));
            break;
          case removeEdgesFromGraphFile:
            MaintenanceExperiment.removeEdgesFromGraphFile(cmd.getOptionValue(graphPath),
                cmd.getOptionValue(edgePath), cmd.getOptionValue(outputPath));
            break;
          case removeEdgesFromDb:
            MaintenanceExperiment.removeEdgesFromDb(cmd.getOptionValue(dbPath),
                cmd.getOptionValue(edgePath));
            break;
          // maintenance experiment
          case addEdgeExperiment:
            MaintenanceExperiment.addEdgeExperiment(cmd.getOptionValue(dbPath),
                Integer.parseInt(cmd.getOptionValue(MAX_HOPNUM)),
                Integer.parseInt(cmd.getOptionValue(maxPNSize)), cmd.getOptionValue(edgePath),
                Integer.parseInt(cmd.getOptionValue(queryCount)), cmd.getOptionValue(safeNodesPath),
                cmd.getOptionValue(outputPath));
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
    // String dataDir = "D:\\Project_Data\\wikidata-20180308-truthy-BETA.nt";
    //
    // // Analyze
    // args = new String[] {"-f", "getPNSizeDistribution", "-dp",
    // dataDir + "\\neo4j-community-3.4.12_risotree\\data\\databases\\graph.db", "-d", "wikidata",
    // "-outputPath", dataDir + "/PNdistribution.txt"};

    // load 1-hop pathneighbors.
    // args = new String[] {"-f", FunctionName.wikiLoadPN.name(), "-dp",
    // dataDir + "\\neo4j-community-3.4.12_risotree\\data\\databases\\graph.db", "-c",
    // dataDir + "\\containID.txt", "-gp", dataDir + "\\graph.txt", "-labelStrMapPath",
    // dataDir + "\\entity_string_label.txt", "-lp", dataDir + "\\graph_label.txt", "-hop", "0",
    // "-PNPrefix", dataDir + "PathNeighbors", "-maxPNSize", "100"};

    // run only once.
    // DataProcess.convertSingleToBidirectinalGraph();
    Util.println(Arrays.toString(args));
    Driver driver = new Driver(args);
    driver.parser();

  }

}
