package knn;

import org.neo4j.graphdb.Node;
import commons.MyRectangle;

/**
 * The structure for join algorithm queue element
 * 
 * @author ysun138
 *
 */
public class NodeAndRec {
  public Node node;
  public MyRectangle rectangle;

  public NodeAndRec(Node node, MyRectangle rectangle) {
    this.node = node;
    this.rectangle = rectangle;
  }
}
