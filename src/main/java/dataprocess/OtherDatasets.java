package dataprocess;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import commons.GraphUtil;

public class OtherDatasets {
  public static void spatialLabelRelabel(String sourceLabelPath, String labelListString,
      String outputPath) throws Exception {
    ArrayList<ArrayList<Integer>> graphLabels = GraphUtil.ReadGraph(sourceLabelPath);
    String[] labelList = labelListString.split(",");
    int newLabelCount = labelList.length;
    List<Integer> intLabels = new ArrayList<>(newLabelCount);
    for (String labelString : labelList) {
      intLabels.add(Integer.parseInt(labelString));
    }

    spatialLabelRelabel(graphLabels, intLabels);
    GraphUtil.writeGraphArrayList(graphLabels, outputPath);
  }

  public static void spatialLabelRelabel(ArrayList<ArrayList<Integer>> graphLabels,
      List<Integer> intLabels) throws Exception {
    Random random = new Random();
    int newLabelCount = intLabels.size();
    int i = 0;
    for (ArrayList<Integer> nodeLabels : graphLabels) {
      if (nodeLabels.size() != 1) {
        throw new Exception(String.format("Node %d should have only 1 label, rather than %d!", i,
            nodeLabels.size()));
      }
      if (nodeLabels.get(0) == 1) {
        int newLabel = intLabels.get((int) (random.nextDouble() * newLabelCount));
        nodeLabels.set(0, newLabel);
      }
      i++;
    }
  }

}
