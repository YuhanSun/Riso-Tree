package commons;

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
}
