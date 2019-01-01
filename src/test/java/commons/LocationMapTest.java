package commons;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class LocationMapTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Test
  public void test() {
    fail("Not yet implemented");
  }

  @Test
  public void mapTest() {
    MyPoint fromPoint = new MyPoint(1.0, 2.0);
    MyRectangle fromExtend = new MyRectangle(0, 0, 1, 2);
    MyRectangle toExtend = new MyRectangle(0, 0, 2, 4);
    LocationMap mapper = new LocationMap(fromExtend, toExtend);
    MyPoint toPoint = mapper.map(fromPoint);
    assertEquals(2.0, toPoint.x, 0.000001);
    assertEquals(4.0, toPoint.y, 0.000001);

    fromPoint = new MyPoint(0.5, 1);
    toPoint = mapper.map(fromPoint);
    assertEquals(1.0, toPoint.x, 0.000001);
    assertEquals(2.0, toPoint.y, 0.000001);

  }

}
