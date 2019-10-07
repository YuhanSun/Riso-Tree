package commons;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import com.vividsolutions.jts.index.strtree.STRtree;
import commons.Config.system;

public class UtilityTest {

  static Config config = new Config();
  static String dataset = config.getDatasetName();
  static system systemName = config.getSystemName();
  static String entityPath;
  static String homeDir = null;

  @Before
  public void setUp() throws Exception {
    switch (systemName) {
      case Ubuntu:
        entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
        break;
      case Windows:
        String dataDirectory = "D:\\Ubuntu_shared\\GeoMinHop\\data";
        entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
      default:
        break;
    }

    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource("").getFile());
    homeDir = file.getAbsolutePath();
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void getBatchInserterTest() throws Exception {
    String dbPath = homeDir + "/graph.db";
    BatchInserter inserter = Util.getBatchInserter(dbPath);
    // Map<String, String> config = new HashMap<String, String>();
    // config.put("dbms.pagecache.memory", "100g");
    // BatchInserter inserter = BatchInserters.inserter(new File(dbPath).getAbsoluteFile(), config);
    Util.close(inserter);
  }

  @Test
  public void arraysDifferenceCountTest() {
    int[] arraySource = new int[] {0, 2, 5, 8, 9, 11, 12};
    int[] arrayTarget = new int[] {1, 3, 5, 7, 9};
    assertEquals(5, Util.arraysDifferenceCount(arraySource, arrayTarget));
  }

  @Test
  public void arraysDifferenceTest() {
    int[] A = new int[] {0, 2, 3};
    int[] B = new int[] {0, 1, 4};
    int[] diff = Util.arraysDifference(A, B);
    assertEquals(diff.length, 2);
    assertEquals(diff[0], 2);
    assertEquals(diff[1], 3);

    A = new int[] {0, 2, 3, 4};
    diff = Util.arraysDifference(A, B);
    assertEquals(diff.length, 2);

    A = new int[] {0, 2, 3, 5, 6};
    diff = Util.arraysDifference(A, B);
    assertEquals(diff.length, 4);
  }

  @Test
  public void distanceConvertTest() {
    float lat1 = 10, lat = 12, lon1 = 0, lon2 = 2;
    Util.println("Distance:" + Util.distFrom(lat1, lon1, lat, lon2));
  }

  @Test
  public void distanceTest() {
    // double x = 0, y = 0;
    double x = 1.5, y = 1.5;
    MyRectangle rectangle = new MyRectangle(1, 1, 2, 2);
    Util.println(Util.distance(x, y, rectangle));
  }

  @Test
  public void distanceRRTest() {
    MyRectangle rectangle1 = new MyRectangle(1, 1, 2, 2);
    MyRectangle rectangle2 = new MyRectangle(0, 3, 0, 3);
    Util.println(Util.distance(rectangle1, rectangle2));
  }

  @Test
  public void groupSumTest() {
    ArrayList<Integer> input = new ArrayList<Integer>(Arrays.asList(0, 1, 2));
    ArrayList<Integer> result = Util.groupSum(0, 2, input, 3);
    Util.println(result);
  }

  @Test
  public void distanceQueryTest() {
    Util.println(entityPath);
    ArrayList<Entity> entities = OwnMethods.ReadEntity(entityPath);
    double distance = 10;

    STRtree stRtree = OwnMethods.constructSTRTree(entities);
    long start = System.currentTimeMillis();
    MyPoint point = new MyPoint(500, 500);
    LinkedList<Entity> result = Util.distanceQuery(stRtree, point, distance);
    long time = System.currentTimeMillis() - start;
    Util.println(String.format("time using index: %d", time));
    Util.println("result size: " + result.size());

    start = System.currentTimeMillis();
    int count = 0;
    for (Entity entity : entities)
      if (Util.distance(entity.lon, entity.lat, point.x, point.y) <= distance)
        count++;
    time = System.currentTimeMillis() - start;
    Util.println("time no index: " + time);
    Util.println("result size " + count);
  }

  @Test
  public void sortedListIntersectTest() {
    ArrayList<Integer> l1 = new ArrayList<>(Arrays.asList(1, 2, 4));
    int[] l2 = {3};
    Util.println(Util.sortedListIntersect(l1, l2));

    l1 = new ArrayList<>();
    l1.add(1);
    int[] l3 = {1, 2, 3, 4};
    Util.println(Util.sortedListIntersect(l1, l3));
  }

  @Test
  public void isSortedIntersectTest() {
    ArrayList<Integer> l1 = new ArrayList<>(Arrays.asList(1, 2, 4));
    ArrayList<Integer> l2 = new ArrayList<>(Arrays.asList(3));
    Util.println(Util.isSortedIntersect(l1, l2));

    l1 = new ArrayList<>(Arrays.asList(1, 2, 4));
    l2 = new ArrayList<>(Arrays.asList(2));
    Util.println(Util.isSortedIntersect(l1, l2));
  }

  /**
   * Should output: null, [], [0, 1]
   */
  @Test
  public void arrayListToArrayIntTest() {
    ArrayList<Integer> arrayList = null;
    Util.println(Util.arrayListToArrayInt(arrayList));

    arrayList = new ArrayList<>();
    Util.println(Arrays.toString(Util.arrayListToArrayInt(arrayList)));

    arrayList = new ArrayList<>(Arrays.asList(0, 1));
    Util.println(Arrays.toString(Util.arrayListToArrayInt(arrayList)));
  }

  /**
   * [0, 2, 4], [0, 1, 2, 4], [0, 1, 2, 4, 5]
   */
  @Test
  public void sortedArrayMergeTest() {
    int[] i1 = new int[] {0, 2, 4};
    int[] i2 = new int[] {0, 2, 4};
    Util.println(Arrays.toString(Util.sortedArrayMerge(i1, i2)));

    i2 = new int[] {0, 1, 2};
    Util.println(Arrays.toString(Util.sortedArrayMerge(i1, i2)));

    i2 = new int[] {1, 4, 5};
    Util.println(Arrays.toString(Util.sortedArrayMerge(i1, i2)));
  }

  @Test
  public void sortedListMergeTest() {
    List<Integer> l1 = new ArrayList<>(Arrays.asList(0, 2, 4));
    List<Integer> l2 = new ArrayList<>(Arrays.asList(0, 2, 4));
    assertTrue(Util.sortedListMerge(l1, l2).equals(new ArrayList<>(Arrays.asList(0, 2, 4))));

    l2 = new ArrayList<>(Arrays.asList(0, 1, 2));
    assertTrue(Util.sortedListMerge(l1, l2).equals(new ArrayList<>(Arrays.asList(0, 1, 2, 4))));

    l2 = new ArrayList<>(Arrays.asList(1, 4, 5));
    assertTrue(Util.sortedListMerge(l1, l2).equals(new ArrayList<>(Arrays.asList(0, 1, 2, 4, 5))));

    l1 = new ArrayList<>(Arrays.asList(6347, 27907, 78651, 161887, 219053, 277652, 280405, 290182,
        399072, 409047, 437711, 446848, 551414));
    l2 = new ArrayList<>(Arrays.asList(6347, 97667, 275395, 398112, 446734));
    assertTrue(Util.sortedListMerge(l1, l2)
        .equals(new ArrayList<>(Arrays.asList(6347, 27907, 78651, 97667, 161887, 219053, 275395,
            277652, 280405, 290182, 398112, 399072, 409047, 437711, 446734, 446848, 551414))));
  }

}
