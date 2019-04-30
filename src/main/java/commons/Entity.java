package commons;

public class Entity {
  public boolean IsSpatial;
  public double lon;
  public double lat;
  public int id;

  /**
   * Empty entity
   */
  Entity() {
    this.IsSpatial = false;
    this.lon = 0.0;
    this.lat = 0.0;
    this.id = -1;
  }

  /**
   * Non-spatial entity
   * 
   * @param id
   * @param lon
   * @param lat
   */
  public Entity(int id) {
    this.IsSpatial = false;
    this.lon = 0.0;
    this.lat = 0.0;
    this.id = id;
  }

  /**
   * Spatial entity
   * 
   * @param id
   * @param lon
   * @param lat
   */
  Entity(int id, double lon, double lat) {
    this.IsSpatial = true;
    this.lon = lon;
    this.lat = lat;
    this.id = id;
  }

  @Override
  public String toString() {
    String string = "";
    string += id + " ";
    string += "(" + Boolean.valueOf(IsSpatial) + ", ";
    string += Double.toString(lon);
    string += " ,";
    string += Double.toString(lat) + ")";
    return string;
  }
}
