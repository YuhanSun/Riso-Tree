package dataprocess;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import commons.Util;

public class OtherDatasetsTest {

  @Test
  public void spatialLabelRelabelTest() throws Exception {
    ArrayList<ArrayList<Integer>> graphLabels = new ArrayList<>();
    graphLabels.add(new ArrayList<>(Arrays.asList(1)));
    graphLabels.add(new ArrayList<>(Arrays.asList(1)));
    graphLabels.add(new ArrayList<>(Arrays.asList(1)));
    graphLabels.add(new ArrayList<>(Arrays.asList(1)));
    graphLabels.add(new ArrayList<>(Arrays.asList(1)));
    graphLabels.add(new ArrayList<>(Arrays.asList(1)));
    graphLabels.add(new ArrayList<>(Arrays.asList(1)));
    List<Integer> intLabels = Arrays.asList(10, 11, 12, 13);
    OtherDatasets.spatialLabelRelabel(graphLabels, intLabels);
    for (ArrayList<Integer> labels : graphLabels) {
      Util.println(labels);
    }
  }

}
