package commons;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SaveRectanglesAsImageTest {

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void saveAsJPGTest() throws IOException {
    List<MyRectangle> rectangles = new ArrayList<>();
    rectangles.add(new MyRectangle("(1,1,500,500)"));
    rectangles.add(new MyRectangle("(2,2,500.7,500.7)"));
    // rectangles.add(new MyRectangle("(0,0,2,2)"));
    MyRectangle rectanglesExtend = new MyRectangle("(0,0,1000,1000)");
    MyRectangle imageExtend = new MyRectangle("(0,0,1000,1000)");
    String outputPath = "/Users/zhouyang/Documents/tmp/myimage.jpg";
    SaveRectanglesAsImage.saveAsJPG(rectangles, rectanglesExtend, imageExtend, outputPath);
  }

}
