import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import commons.Config;
import commons.Utility;
import graph.Construct_RisoTree;
import graph.LoadDataNoOSM;

public class Driver {

  private String[] args = null;
  private Options options = new Options();

  private String help = "h";
  private String function = "f";
  private String graphPath = "gp";
  private String entityPath = "ep";
  private String labelListPath = "lp";
  private String dbPath = "dp";
  private String dataset = "d";

  // Construct_RisoTree
  private String containIDPath = "c";

  // function names
  private String risoTreeSkeleton = "tree";
  private String containID = "containID";

  public Driver(String[] args) {
    this.args = args;
    options.addOption(help, "help", false, "show help.");
    options.addOption(function, "function", true, "function name");
    options.addOption(graphPath, "graph-path", true, "graph path");
    options.addOption(entityPath, "entity-path", true, "entity path");
    options.addOption(labelListPath, "labellist-path", true, "label list path");
    options.addOption(dbPath, "db-path", true, "db path");
    options.addOption(dataset, "dataset", true, "dataset for naming the layer");
    options.addOption(containIDPath, "containId-path", true, "path for containID.txt");
  }

  public void parser() {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
      Option[] options = cmd.getOptions();
      for (Option option : options) {
        Utility.print(String.format("<%s, %s>", option, option.getValue()));
      }

      if (cmd.hasOption("h")) {
        help();
      }

      if (cmd.hasOption(function)) {
        String functionName = cmd.getOptionValue(function);
        if (functionName.equals(risoTreeSkeleton)) {
          String graphPathVal = cmd.getOptionValue(graphPath);
          String entityPathVal = cmd.getOptionValue(entityPath);
          String labelListPathVal = cmd.getOptionValue(labelListPath);
          String dbPathVal = cmd.getOptionValue(dbPath);
          String datasetVal = cmd.getOptionValue(dataset);

          LoadDataNoOSM loadDataNoOSM = new LoadDataNoOSM(new Config(), true);
          loadDataNoOSM.batchRTreeInsertOneHopAware(dbPathVal, datasetVal, graphPathVal,
              entityPathVal, labelListPathVal);
        } else if (functionName.equals(containID)) {
          Construct_RisoTree construct_RisoTree = new Construct_RisoTree(new Config(), true);
          construct_RisoTree.generateContainSpatialID(cmd.getOptionValue(dbPath),
              cmd.getOptionValue(dataset), cmd.getOptionValue(containIDPath));
        }
      }
    } catch (

    Exception e) {
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
    Utility.print(Arrays.toString(args));
    Driver driver = new Driver(args);
    driver.parser();
  }

}
