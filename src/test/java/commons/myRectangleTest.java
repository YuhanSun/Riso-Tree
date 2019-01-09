package commons;

import static org.junit.Assert.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import commons.MyRectangle;

public class myRectangleTest {

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
      Utility.print("Null");
    else
      Utility.print(intersectRect);
  }

  @Test
  public void test() {

  }

}
