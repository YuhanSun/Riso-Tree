package commons;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.List;
import javax.imageio.ImageIO;

public class SaveRectanglesAsImage {

  public static void saveAsJPG(List<MyRectangle> rectangles, String rectanglesExtend,
      String imageExtend, String outputPath) throws IOException {
    saveAsJPG(rectangles, new MyRectangle(rectanglesExtend), new MyRectangle(imageExtend),
        outputPath);
  }

  public static void saveAsJPG(List<MyRectangle> rectangles, MyRectangle rectanglesExtend,
      MyRectangle imageExtend, String outputPath) throws IOException {
    int imageWidth = (int) (imageExtend.max_x - imageExtend.min_x);
    int imageHeight = (int) (imageExtend.max_y - imageExtend.min_y);

    // Constructs a BufferedImage of one of the predefined image types.
    BufferedImage bufferedImage =
        new BufferedImage(imageWidth, imageHeight, BufferedImage.TYPE_INT_RGB);

    // Create a graphics which can be used to draw into the buffered image
    Graphics2D g2d = bufferedImage.createGraphics();

    // fill all the image with white
    g2d.setColor(Color.white);
    g2d.fillRect(0, 0, imageWidth, imageHeight);

    g2d.setColor(Color.black);
    LocationMap locationMap = new LocationMap(rectanglesExtend, imageExtend);
    for (MyRectangle rectangle : rectangles) {
      MyRectangle drawRect = locationMap.map(rectangle);
      double width = drawRect.max_x - drawRect.min_x;
      double height = drawRect.max_y - drawRect.min_y;
      g2d.draw(new Rectangle2D.Double(drawRect.min_x, drawRect.min_y, width, height));
    }

    // Disposes of this graphics context and releases any system resources that it is using.
    g2d.dispose();

    // Save as JPEG
    File file = new File(outputPath);
    ImageIO.write(bufferedImage, "jpg", file);
  }
}
