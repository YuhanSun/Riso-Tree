package commons;

/**
 * Map the point in {@code fromExtend} to {@code toExtend}. The mapping follows a linear scaling way
 * from the (0,0) origin.
 *
 * @author yuhan sun
 *
 */
public class LocationMap {
  MyRectangle fromExtend;
  MyRectangle toExtend;
  double widthRatio;
  double heightRatio;

  public LocationMap(MyRectangle fromExtend, MyRectangle toExtend) {
    this.fromExtend = fromExtend;
    this.toExtend = toExtend;
    this.widthRatio = (toExtend.max_x - toExtend.min_x) / (fromExtend.max_x - fromExtend.min_x);
    this.heightRatio = (toExtend.max_y - toExtend.min_y) / (fromExtend.max_y - fromExtend.min_y);
  }

  public MyPoint map(MyPoint fromPoint) {
    double x = toExtend.min_x + (fromPoint.x - fromExtend.min_x) * widthRatio;
    double y = toExtend.min_y + (fromPoint.y - fromExtend.min_y) * heightRatio;
    return new MyPoint(x, y);
  }

  public MyRectangle map(MyRectangle rectangle) {
    MyPoint fromLeftBottom = new MyPoint(rectangle.min_x, rectangle.min_y);
    MyPoint fromRightTop = new MyPoint(rectangle.max_x, rectangle.max_y);
    MyPoint toLeftBottom = map(fromLeftBottom);
    MyPoint toRightTop = map(fromRightTop);
    return new MyRectangle(toLeftBottom.x, toLeftBottom.y, toRightTop.x, toRightTop.y);
  }
}
