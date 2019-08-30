package commons;

import static org.junit.Assert.assertTrue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class MyRectangleTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Test
  public void insertTest() {
    MyRectangle queryRect = new MyRectangle("(-73.969601, 40.762531, -73.969601, 40.762531)");
    MyRectangle bbox = new MyRectangle("[-73.971444, 40.762827, -73.971444, 40.762827]");
    MyRectangle intersectRect = bbox.intersect(queryRect);
    if (intersectRect == null)
      Util.println("Null");
    else
      Util.println(intersectRect);
  }

  @Test
  public void isSameTest() {
    MyRectangle r1 = new MyRectangle(0.00001, 0.00002, 1, 2);
    MyRectangle r2 = new MyRectangle(0.00001, 0.00002, 1.0, 2);
    assertTrue(r1.isSame(r2));

    r1 = new MyRectangle(0.00001, 0.00002, 1, 2);
    r2 = new MyRectangle(0.00001, 0.00002, 1.00, 2);
    assertTrue(r1.isSame(r2));

    r1 = new MyRectangle(0.00001001, 0.00002, 1, 2);
    r2 = new MyRectangle(0.00001, 0.00002, 1.00, 2);
    assertTrue(!r1.isSame(r2));
  }

}
