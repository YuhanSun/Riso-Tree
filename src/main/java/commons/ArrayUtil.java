package commons;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ArrayUtil {


  public static <T extends Comparable<T>> boolean isSortedListEqual(List<T> l1, List<T> l2) {
    return l1.size() == l2.size() && l1.size() == sortedListIntersect(l1, l2).size();
  }

  public static int[] iterableToIntArray(Iterable<Integer> iterable) {
    List<Integer> list = iterableToList(iterable);
    return listToArrayInt(list);
  }

  public static <T> List<T> iterableToList(Iterable<T> iterable) {
    List<T> res = new LinkedList<>();
    for (T t : iterable) {
      res.add(t);
    }
    return res;
  }

  public static <T extends Comparable<T>> List<T> sortedListIntersect(List<T> l1, List<T> l2) {
    List<T> res = new ArrayList<>(l1.size() + l2.size());
    int i = 0, j = 0;
    while (i < l1.size() && j < l2.size()) {
      if (l1.get(i).compareTo(l2.get(j)) < 0)
        i++;
      else {
        if (l1.get(i).compareTo(l2.get(j)) > 0)
          j++;
        else {
          res.add(l1.get(i));
          i++;
          j++;
        }
      }
    }
    return res;
  }

  /**
   * Intersect two sorted list
   * 
   * @param l1
   * @param l2
   * @return
   */
  public static List<Integer> sortedListIntersect(List<Integer> l1, int[] l2) {
    List<Integer> res = new ArrayList<>(l1.size() + l2.length);
    int i = 0, j = 0;
    while (i < l1.size() && j < l2.length) {
      if (l1.get(i) < l2[j])
        i++;
      else {
        if (l1.get(i) > l2[j])
          j++;
        else {
          res.add(l2[j]);
          i++;
          j++;
        }
      }
    }
    return res;
  }

  /**
   * Convert an List to int[]. Because ArrayList.toArray() cannot work for int type.
   *
   * @param arrayList
   * @return
   */
  public static int[] listToArrayInt(List<Integer> arrayList) {
    if (arrayList == null) {
      return null;
    }
    int[] res = new int[arrayList.size()];
    int i = 0;
    for (int val : arrayList) {
      res[i] = val;
      i++;
    }
    return res;
  }

  public static long Average(List<Long> arraylist) {
    if (arraylist.size() == 0)
      return -1;
    long sum = 0;
    for (long element : arraylist)
      sum += element;
    return sum / arraylist.size();
  }
}
