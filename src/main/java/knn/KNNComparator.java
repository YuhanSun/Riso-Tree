package knn;

import java.util.Comparator;

public class KNNComparator implements Comparator<Element> {

  public int compare(Element o1, Element o2) {
    // TODO Auto-generated method stub
    if (o1.distance < o2.distance)
      return -1;
    else if (o1.distance > o2.distance)
      return 1;
    else
      return 0;
  }

}
