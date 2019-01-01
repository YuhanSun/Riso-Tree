package commons;

import java.awt.Graphics;
import java.util.List;
import javax.swing.JPanel;

public class DrawRectangles extends JPanel {

  private List<MyRectangle> rectangles;
  private MyRectangle spaceExtend;
  private MyRectangle drawExtend;

  public DrawRectangles(MyRectangle spaceExtend, MyRectangle drawExtend,
      List<MyRectangle> rectangles) {
    // TODO Auto-generated constructor stub
    this.rectangles = rectangles;
    this.spaceExtend = spaceExtend;
    this.drawExtend = drawExtend;
    repaint();
  }

  public void paint(Graphics g) {
    LocationMap LocationMapper = new LocationMap(spaceExtend, drawExtend);
    for (MyRectangle rectangle : rectangles) {
      MyPoint fromLeftBottom = new MyPoint(rectangle.min_x, rectangle.min_y);
      MyPoint fromRightTop = new MyPoint(rectangle.max_x, rectangle.max_y);
      MyPoint toLeftBottom = LocationMapper.map(fromLeftBottom);
      MyPoint toRightTop = LocationMapper.map(fromRightTop);
      double width = toRightTop.x - toLeftBottom.x;
      double height = toRightTop.y - toLeftBottom.y;
      g.drawRect((int) toLeftBottom.x, (int) toLeftBottom.y, (int) width, (int) height);
    }
  }

}
