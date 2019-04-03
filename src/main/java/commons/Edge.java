package commons;

public class Edge {
  public long start;
  public long end;

  public Edge(long start, long end) {
    this.start = start;
    this.end = end;
  }

  @Override
  public String toString() {
    return String.format("%d,%d", start, end);
  }
}
