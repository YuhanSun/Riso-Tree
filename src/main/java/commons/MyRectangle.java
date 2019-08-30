package commons;

public class MyRectangle {
  public double min_x;
  public double min_y;
  public double max_x;
  public double max_y;

  public MyRectangle(double[] bbox) {
    this.min_x = bbox[0];
    this.min_y = bbox[1];
    this.max_x = bbox[2];
    this.max_y = bbox[3];
  }

  public MyRectangle(double p_min_x, double p_min_y, double p_max_x, double p_max_y) {
    this.min_x = p_min_x;
    this.min_y = p_min_y;
    this.max_x = p_max_x;
    this.max_y = p_max_y;
  }

  public MyRectangle() {
    this.min_x = 0.0;
    this.min_y = 0.0;
    this.max_x = 0.0;
    this.max_y = 0.0;
  }

  public MyRectangle(String str) {
    str = str.substring(1, str.length() - 1);
    String[] liStrings = str.split(",");
    this.min_x = Double.parseDouble(liStrings[0]);
    this.min_y = Double.parseDouble(liStrings[1]);
    this.max_x = Double.parseDouble(liStrings[2]);
    this.max_y = Double.parseDouble(liStrings[3]);
  }

  @Override
  public String toString() {
    String string = "";
    string += "(" + Double.toString(min_x);
    string += ", " + Double.toString(min_y);
    string += ", " + Double.toString(max_x);
    string += ", " + Double.toString(max_y) + ")";
    return string;
  }

  public double area() {
    return (max_x - min_x) * (max_y - min_y);
  }

  /**
   * return intersect rectangle given an input rectangle min_x = max_x is considered to be a valid
   * rectangle with zero area
   * 
   * @param rectangle the input rectangle
   * @return the intersect rectangle. null means no intersection
   */
  public MyRectangle intersect(MyRectangle rectangle) {
    double left = Math.max(this.min_x, rectangle.min_x);
    double right = Math.min(this.max_x, rectangle.max_x);
    double bottom = Math.max(this.min_y, rectangle.min_y);
    double top = Math.min(this.max_y, rectangle.max_y);

    if (left <= right && bottom <= top)
      return new MyRectangle(left, bottom, right, top);
    else
      return null;
  }

  public boolean isSame(MyRectangle other) {
    double epsilon = Math.pow(0.1, 12);
    return Math.abs(min_x - other.min_x) < epsilon && Math.abs(min_y - other.min_y) < epsilon
        && Math.abs(max_x - other.max_x) < epsilon && Math.abs(max_y - other.max_y) < epsilon;
  }
}
