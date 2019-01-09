package commons;

public class MyPoint {

  public double x;
  public double y;

  public MyPoint() {
    // TODO Auto-generated constructor stub
  }

  public MyPoint(double p_x, double p_y) {
    this.x = p_x;
    this.y = p_y;
  }

  public MyPoint(String string) {
    string = string.substring(1, string.length() - 1);
    String[] stringList = string.split(",");
    this.x = Double.parseDouble(stringList[0]);
    this.y = Double.parseDouble(stringList[1]);
  }
}
