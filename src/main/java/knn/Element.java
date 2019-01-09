package knn;

import org.neo4j.graphdb.Node;

public class Element {
  public Node node;
  public double distance;

  public Element(Node node, double distance) {
    this.node = node;
    this.distance = distance;
  }
}
