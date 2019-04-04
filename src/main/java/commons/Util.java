package commons;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.strtree.STRtree;

public class Util {

  private static final Logger LOGGER = Logger.getLogger(Util.class.getName());

  public static void close(BufferedReader reader) throws Exception {
    LOGGER.info("close reader...");
    if (reader != null) {
      reader.close();
    }
    LOGGER.info("reader is closed.");
  }

  public static void close(FileWriter writer) throws Exception {
    LOGGER.info("close writer...");
    if (writer != null) {
      writer.close();
    }
    LOGGER.info("writer is closed.");
  }

  public static void close(BatchInserter inserter) {
    LOGGER.info("shut down batch inserter...");
    if (inserter != null) {
      inserter.shutdown();
    }
    LOGGER.info("shut down is done.");
  }

  public static void close(GraphDatabaseService service) throws Exception {
    LOGGER.info("shut down graph database service...");
    if (service != null) {
      service.shutdown();
    }
    LOGGER.info("service is shut down successfully.");
  }

  public static boolean pathExist(String path) {
    File file = new File(path);

    if (file.exists())
      return true;
    else
      return false;
  }

  /**
   * Compute the how many elements in source but not in target. Both arrays are sorted.
   *
   * @param arraySource
   * @param arrayTarget
   * @return
   */
  public static int arraysDifferenceCount(int[] arraySource, int[] arrayTarget) {
    int count = 0;
    int i = 0, j = 0;
    while (i < arraySource.length && j < arrayTarget.length) {
      if (arraySource[i] < arrayTarget[j]) {
        count++;
        i++;
      } else {
        if (arraySource[i] == arrayTarget[j]) {
          i++;
          j++;
        } else {
          j++;
        }
      }
    }

    if (i < arraySource.length) {
      count += arraySource.length - i;
    }

    return count;
  }

  /**
   * Decide whether two list has intersection.
   * 
   * @param l1
   * @param l2
   * @return
   */
  public static boolean isSortedIntersect(ArrayList<Integer> l1, ArrayList<Integer> l2) {
    int i = 0, j = 0;
    while (true) {
      if (i >= l1.size())
        return false;
      if (j >= l2.size())
        return false;
      if (l1.get(i) < l2.get(j))
        i++;
      else if (l1.get(i) > l2.get(j))
        j++;
      else
        return true;
    }
  }

  /**
   * Intersect two sorted list
   * 
   * @param l1
   * @param l2
   * @return
   */
  public static ArrayList<Integer> sortedListIntersect(ArrayList<Integer> l1, int[] l2) {
    ArrayList<Integer> res = new ArrayList<>(l1.size() + l2.length);
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
   * Merge two array. Expanded or not can be decided by the length of the array.
   *
   * @param i1
   * @param i2
   * @return
   */
  public static int[] sortedArrayMerge(int[] i1, int[] i2) {
    if (i1 == null) {
      return i2;
    }

    if (i2 == null) {
      return i1;
    }

    ArrayList<Integer> arrayList = new ArrayList<>(i1.length + i2.length);
    int i = 0, j = 0;
    while (i < i1.length && j < i2.length) {
      if (i1[i] < i2[j]) {
        arrayList.add(i1[i]);
        i++;
      } else {
        if (i1[i] > i2[j]) {
          arrayList.add(i2[j]);
          j++;
        } else {
          arrayList.add(i2[j]);
          i++;
          j++;
        }
      }
    }

    while (i < i1.length) {
      arrayList.add(i1[i]);
      i++;
    }

    while (j < i2.length) {
      arrayList.add(i2[j]);
      j++;
    }

    return arrayListToArrayInt(arrayList);
  }

  /**
   * Convert an ArrayList to int[]. Because ArrayList.toArray() cannot work for int type.
   *
   * @param arrayList
   * @return
   */
  public static int[] arrayListToArrayInt(ArrayList<Integer> arrayList) {
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

  /**
   * For a given query point and a radius, to find all the spatial objects in the STRTree.
   * 
   * @param stRtree the input STRtree
   * @param point the input query location
   * @param distance the search radius
   * @return all the spatial objects within the distance from the query point
   */
  public static LinkedList<Entity> distanceQuery(STRtree stRtree, MyPoint point, double distance) {
    Envelope searchEnv = new Envelope(point.x - distance, point.x + distance, point.y - distance,
        point.y + distance);
    List<Entity> rangeResult = stRtree.query(searchEnv);
    LinkedList<Entity> result = new LinkedList<Entity>();
    for (Entity entity : rangeResult) {
      if (Util.distance(entity.lon, entity.lat, point.x, point.y) <= distance)
        result.add(entity);
    }
    return result;
  }

  public static ArrayList<Integer> groupSum(double min, double max, ArrayList<Integer> input,
      int binCount) {
    ArrayList<Integer> result = new ArrayList<Integer>(binCount);
    for (int i = 0; i < binCount; i++)
      result.add(0);
    try {
      double interval = (max - min) / binCount;

      for (int element : input) {
        int index = (int) (element / interval);
        if (index > binCount)
          throw new Exception(String.format("Value %f is larger than max value %f", element, max));
        else if (index == binCount)
          index = binCount - 1;
        result.set(index, result.get(index) + 1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
    return result;
  }

  /**
   * Distance between two rectangles. Rather than using centering point, this method considers
   * boundaries of rectangles. So compute closest distance between two rectangle boundaries.
   *
   * @param rectangle1
   * @param rectangle2
   * @return
   */
  public static double distance(MyRectangle rectangle1, MyRectangle rectangle2) {
    double minx1 = rectangle1.min_x, miny1 = rectangle1.min_y, maxx1 = rectangle1.max_x,
        maxy1 = rectangle1.max_y;
    double minx2 = rectangle2.min_x, miny2 = rectangle2.min_y, maxx2 = rectangle2.max_x,
        maxy2 = rectangle2.max_y;

    if (maxx1 < minx2) {
      if (maxy1 < miny2)
        return Math.sqrt((maxx1 - minx2) * (maxx1 - minx2) + (maxy1 - miny2) * (maxy1 - miny2));
      else if (miny1 > maxy2)
        return Math.sqrt((maxx1 - minx2) * (maxx1 - minx2) + (miny1 - maxy2) * (miny1 - maxy2));
      else
        return minx2 - maxx1;
    } else if (minx1 > maxx2) {
      if (maxy1 < miny2)
        return Math.sqrt((minx1 - maxx2) * (minx1 - maxx2) + (maxy1 - miny2) * (maxy1 - miny2));
      else if (miny1 > maxy2)
        return Math.sqrt((minx1 - maxx2) * (minx1 - maxx2) + (miny1 - maxy2) * (miny1 - maxy2));
      else
        return minx1 - maxx2;
    } else {
      if (maxy1 < miny2)
        return miny2 - maxy1;
      else if (miny1 > maxy2)
        return miny1 - maxy2;
      else
        return 0;
    }
  }

  public static double distance(double x, double y, MyRectangle rectangle) {
    double minx = rectangle.min_x, miny = rectangle.min_y, maxx = rectangle.max_x,
        maxy = rectangle.max_y;
    if (x < minx) {
      if (y < miny)
        return Math.sqrt((x - minx) * (x - minx) + (y - miny) * (y - miny));
      else if (y > maxy)
        return Math.sqrt((x - minx) * (x - minx) + (y - maxy) * (y - maxy));
      else
        return minx - x;
    } else if (x > maxx) {
      if (y < miny)
        return Math.sqrt((x - maxx) * (x - maxx) + (y - miny) * (y - miny));
      else if (y > maxy)
        return Math.sqrt((x - maxx) * (x - maxx) + (y - maxy) * (y - maxy));
      else
        return x - maxx;
    } else {
      if (y < miny)
        return miny - y;
      else if (y > maxy)
        return y - maxy;
      else
        return 0;
    }
  }

  public static double distance(MyPoint location, MyRectangle rectangle) {
    return distance(location.x, location.y, rectangle);
  }

  public static double distance(MyPoint l1, MyPoint l2) {
    return Math.sqrt((l1.x - l2.x) * (l1.x - l2.x) + (l1.y - l2.y) * (l1.y - l2.y));
  }

  public static double distance(double x1, double y1, double x2, double y2) {
    return Math.sqrt((x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2));
  }

  public static double earthRadius = 6371000; // meters

  public static float distFrom(float lat1, float lng1, float lat2, float lng2) {
    double dLat = Math.toRadians(lat2 - lat1);
    double dLng = Math.toRadians(lng2 - lng1);
    double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(Math.toRadians(lat1))
        * Math.cos(Math.toRadians(lat2)) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    float dist = (float) (earthRadius * c);

    return dist;
  }

  /**
   * Distance to delta longitude
   * 
   * @param dist Unit meter
   * @return unit is angle
   */
  public static double distToLongitude(double dist) {
    return dist / earthRadius * 180;
  }

  public static double distToLatitude(double dist, double lon, double lat) {
    double curRadius = earthRadius * Math.cos(lat / 180 * Math.PI);
    return dist / curRadius * 180;
  }

  public static long Average(ArrayList<Long> arraylist) {
    if (arraylist.size() == 0)
      return -1;
    long sum = 0;
    for (long element : arraylist)
      sum += element;
    return sum / arraylist.size();
  }

  public static int Comparator(int a, int b) {
    if (a < b)
      return -1;
    else if (a > b)
      return 1;
    else
      return 0;
  }

  // public static int lower_bound(ArrayList<Label_Degree_Node> label_degree_nodes, int low, int
  // high, int value)
  // {
  // if ( low < 0) return 0;
  // if (low>=high )
  // {
  // if ( value <= label_degree_nodes.get(low).degree ) return low;
  // return low+1;
  // }
  // int mid=(low+high)/2;
  // if ( value> label_degree_nodes.get(mid).degree)
  // return lower_bound(label_degree_nodes, mid+1,high,value);
  // return lower_bound(label_degree_nodes, low, mid, value);
  // }

  /**
   * read transfer table from a file
   * 
   * @param table_path
   * @return
   */
  public static HashMap<Integer, Integer> Read_Transfer_Table(String table_path) {
    HashMap<Integer, Integer> transfer_table = new HashMap<Integer, Integer>();
    BufferedReader reader = null;
    String line = null;
    try {
      reader = new BufferedReader(new FileReader(new File(table_path)));
      while ((line = reader.readLine()) != null) {
        String[] line_list = line.split("\t");
        int ori_label = Integer.parseInt(line_list[0]);
        int transfer_label = Integer.parseInt(line_list[1]);
        if (transfer_label == 0)
          continue;
        transfer_table.put(ori_label, transfer_label);
      }
      reader.close();
    } catch (Exception e) {
      Util.println(line);
      e.printStackTrace();
      return null;
    }
    return transfer_table;
  }

  /**
   * not used now
   * 
   * @param datagraph_path
   * @return
   */
  public static HashMap<Integer, Integer> Preprocess_DataGraph(String datagraph_path) {
    BufferedReader reader = null;
    String line = null;
    HashMap<Integer, Integer> label_cardinality = new HashMap<Integer, Integer>();
    try {
      reader = new BufferedReader(new FileReader(new File(datagraph_path)));
      line = reader.readLine();
      String[] line_list = line.split(" ");
      if (line_list.length != 3) {
        Util.println(String.format("%s first line parameters number mismatch!", datagraph_path));
        return null;
      }
      if (line_list[0].equals("t") == false) {
        Util.println(String.format("%s format no 't' at the beginning!", datagraph_path));
        return null;
      }
      int node_count = Integer.parseInt(line_list[2]);// first line contains total number of nodes
      for (int i = 0; i < node_count; i++) {
        line = reader.readLine();
        line_list = line.split(" ");
        if (line_list.length != 3) {
          Util.println(String.format("node line parameters number mismatch!"));
          return null;
        }
        if (line_list[0].equals("v") == false) {
          Util.println("node line does not start with 'v'");
        }
        int label = Integer.parseInt(line_list[2]);
        if (label_cardinality.containsKey(label))
          label_cardinality.put(label, label_cardinality.get(label) + 1);
        else
          label_cardinality.put(label, 1);
      }
      reader.close();
    } catch (Exception e) {
      Util.println(line);
      e.printStackTrace();
      if (reader != null)
        try {
          reader.close();
        } catch (IOException e1) {
          e1.printStackTrace();
        }
    }
    return label_cardinality;
  }

  /**
   * read query graphs from a file with specific number no transfer table
   * 
   * @param querygraph_path
   * @param read_count
   * @return
   */
  public static ArrayList<Query_Graph> ReadQueryGraphs(String querygraph_path, int read_count) {
    ArrayList<Query_Graph> query_Graphs = new ArrayList<Query_Graph>();
    BufferedReader reader = null;
    String line = null;
    try {
      reader = new BufferedReader(new FileReader(new File(querygraph_path)));
      for (int current_read_count = 0; current_read_count < read_count; current_read_count++) {
        line = reader.readLine();
        String[] line_list = line.split(" ");
        if (line_list.length != 4) {
          Util.println(String.format("query graph first line parameters number mismatch!"));
          return null;
        }
        if (line_list[0].equals("t") == false) {
          Util.println("query graph first line does begin with 't'!");
          return null;
        }
        int node_count = Integer.parseInt(line_list[2]);
        Query_Graph query_Graph = new Query_Graph(node_count);
        for (int i = 0; i < node_count; i++) {
          line = reader.readLine();
          line_list = line.split(" ");
          int node_id = Integer.parseInt(line_list[0]);
          if (node_id != i) {
            Util.println(String.format("node_id not consistent with line index at %d", i));
            Util.println(line);
            return null;
          }

          int node_label = Integer.parseInt(line_list[1]);
          int degree = Integer.parseInt(line_list[2]);

          query_Graph.label_list[i] = node_label;
          ArrayList<Integer> neighbors = query_Graph.graph.get(i);
          for (int j = 0; j < degree; j++) {
            int neighbor_id = Integer.parseInt(line_list[j + 3]);
            neighbors.add(neighbor_id);
          }
        }
        query_Graphs.add(query_Graph);
      }
      reader.close();
    } catch (Exception e) {
      Util.println(line);
      e.printStackTrace();
    }
    return query_Graphs;
  }

  /**
   * Read query graph with spatial predicate indicator
   * 
   * @param querygraph_path
   * @param read_count
   */
  public static ArrayList<Query_Graph> ReadQueryGraph_Spa(String querygraph_path, int read_count) {
    ArrayList<Query_Graph> query_Graphs = new ArrayList<Query_Graph>();
    BufferedReader reader = null;
    String line = null;
    try {
      reader = new BufferedReader(new FileReader(new File(querygraph_path)));
      for (int current_read_count = 0; current_read_count < read_count; current_read_count++) {
        line = reader.readLine();
        String[] line_list = line.split(" ");
        if (line_list.length != 4)
          throw new Exception("query graph first line parameters number mismatch!");
        if (line_list[0].equals("t") == false)
          throw new Exception("query graph first line does begin with 't'!");
        int node_count = Integer.parseInt(line_list[2]);
        int edge_count = Integer.parseInt(line_list[3]);
        Query_Graph query_Graph = new Query_Graph(node_count);
        for (int i = 0; i < node_count; i++) {
          line = reader.readLine();
          line_list = line.split(" ");
          int node_id = Integer.parseInt(line_list[0]);
          if (node_id != i)
            throw new Exception(String.format("node_id not consistent with line index at %d", i));

          int node_label = Integer.parseInt(line_list[1]);
          int degree = Integer.parseInt(line_list[2]);

          query_Graph.label_list[i] = node_label;
          ArrayList<Integer> neighbors = query_Graph.graph.get(i);
          for (int j = 0; j < degree; j++) {
            int neighbor_id = Integer.parseInt(line_list[j + 3]);
            neighbors.add(neighbor_id);
          }
          query_Graph.Has_Spa_Predicate[i] = Boolean.valueOf(line_list[line_list.length - 1]);
        }
        query_Graphs.add(query_Graph);
      }
      reader.close();
    } catch (Exception e) {
      Util.println(line);
      e.printStackTrace();
    }
    return query_Graphs;
  }

  /**
   * Write generated query graphs to file
   * 
   * @param querygraph_path
   * @param query_Graphs
   */
  public static void WriteQueryGraph(String querygraph_path, ArrayList<Query_Graph> query_Graphs) {
    FileWriter fileWriter = null;
    String line = "";

    try {
      fileWriter = new FileWriter(new File(querygraph_path));

      for (int i = 0; i < query_Graphs.size(); i++) {
        Query_Graph query_Graph = query_Graphs.get(i);
        int edge_count = 0;
        for (ArrayList<Integer> neighbors : query_Graph.graph)
          edge_count += neighbors.size();
        line = String.format("t %d %d %d\n", i, query_Graph.graph.size(), edge_count);
        fileWriter.write(line);

        for (int j = 0; j < query_Graph.graph.size(); j++) {
          ArrayList<Integer> neighbors = query_Graph.graph.get(j);
          line = String.format("%d %d %d", j, query_Graph.label_list[j], neighbors.size());
          for (int neighbor : neighbors)
            line += String.format(" %d", neighbor);
          line += " " + String.valueOf(query_Graph.Has_Spa_Predicate[j]) + "\n";
          fileWriter.write(line);
        }
      }
      fileWriter.close();
    } catch (Exception e) {
      Util.println(line);
      e.printStackTrace();
    }
  }

  /**
   * read query graphs from a file with specific number
   * 
   * @param querygraph_path
   * @param transfer_table
   * @param read_count
   * @return
   */
  public static ArrayList<Query_Graph> ReadQueryGraphs(String querygraph_path,
      HashMap<Integer, Integer> transfer_table, int read_count) {
    ArrayList<Query_Graph> query_Graphs = new ArrayList<Query_Graph>();
    BufferedReader reader = null;
    String line = null;
    try {
      reader = new BufferedReader(new FileReader(new File(querygraph_path)));
      for (int current_read_count = 0; current_read_count < read_count; current_read_count++) {
        line = reader.readLine();
        String[] line_list = line.split(" ");
        if (line_list.length != 4) {
          Util.println(String.format("query graph first line parameters number mismatch!"));
          return null;
        }
        if (line_list[0].equals("t") == false) {
          Util.println("query graph first line does begin with 't'!");
          return null;
        }
        int node_count = Integer.parseInt(line_list[2]);
        Query_Graph query_Graph = new Query_Graph(node_count);
        for (int i = 0; i < node_count; i++) {
          line = reader.readLine();
          line_list = line.split(" ");
          int node_id = Integer.parseInt(line_list[0]);
          if (node_id != i) {
            Util.println(String.format("node_id not consistent with line index at %d", i));
            Util.println(line);
            return null;
          }

          int node_label = Integer.parseInt(line_list[1]);
          int transfer_label = transfer_table.get(node_label);
          int degree = Integer.parseInt(line_list[2]);

          query_Graph.label_list[i] = transfer_label;
          ArrayList<Integer> neighbors = query_Graph.graph.get(i);
          for (int j = 0; j < degree; j++) {
            int neighbor_id = Integer.parseInt(line_list[j + 3]);
            neighbors.add(neighbor_id);
          }
        }
        query_Graphs.add(query_Graph);
      }
      reader.close();
    } catch (Exception e) {
      Util.println(line);
      e.printStackTrace();
    }
    return query_Graphs;
  }

  /**
   * read all query graphs from a file
   * 
   * @param querygraph_path
   * @param transfer_table
   * @return
   */
  public static ArrayList<Query_Graph> ReadQueryGraphs(String querygraph_path,
      HashMap<Integer, Integer> transfer_table) {
    ArrayList<Query_Graph> query_Graphs = new ArrayList<Query_Graph>();
    BufferedReader reader = null;
    String line = null;
    try {
      reader = new BufferedReader(new FileReader(new File(querygraph_path)));
      while ((line = reader.readLine()) != null) {
        String[] line_list = line.split(" ");
        if (line_list.length != 4) {
          Util.println(String.format("query graph first line parameters number mismatch!"));
          return null;
        }
        if (line_list[0].equals("t") == false) {
          Util.println("query graph first line does begin with 't'!");
          return null;
        }
        int node_count = Integer.parseInt(line_list[2]);
        Query_Graph query_Graph = new Query_Graph(node_count);
        for (int i = 0; i < node_count; i++) {
          line = reader.readLine();
          line_list = line.split(" ");
          int node_id = Integer.parseInt(line_list[0]);
          if (node_id != i) {
            Util.println(String.format("node_id not consistent with line index at %d", i));
            Util.println(line);
            return null;
          }

          int node_label = Integer.parseInt(line_list[1]);
          int transfer_label = transfer_table.get(node_label);
          int degree = Integer.parseInt(line_list[2]);

          query_Graph.label_list[i] = transfer_label;
          ArrayList<Integer> neighbors = query_Graph.graph.get(i);
          for (int j = 0; j < degree; j++) {
            int neighbor_id = Integer.parseInt(line_list[j + 3]);
            neighbors.add(neighbor_id);
          }
        }
        query_Graphs.add(query_Graph);
      }
    } catch (Exception e) {
      Util.println(line);
      e.printStackTrace();
    }
    return query_Graphs;
  }

  public static String FormCypherQuery(Query_Graph query_Graph, String lon_name, String lat_name) {
    String query = "match ";

    // label
    query += String.format("(a0:GRAPH_%d)", query_Graph.label_list[0]);
    for (int i = 1; i < query_Graph.graph.size(); i++) {
      query += String.format(",(a%d:GRAPH_%d)", i, query_Graph.label_list[i]);
    }

    // edge
    for (int i = 0; i < query_Graph.graph.size(); i++) {
      for (int j = 0; j < query_Graph.graph.get(i).size(); j++) {
        int neighbor = query_Graph.graph.get(i).get(j);
        if (neighbor > i)
          query += String.format(",(a%d)--(a%d)", i, neighbor);
      }
    }

    // spatial predicate
    int i = 0;
    for (; i < query_Graph.label_list.length; i++)
      if (query_Graph.spa_predicate[i] != null) {
        MyRectangle qRect = query_Graph.spa_predicate[i];
        query += String.format(" where %f < a%d.%s < %f ", qRect.min_x, i, lon_name, qRect.max_x);
        query += String.format("and %f < a%d.%s < %f", qRect.min_y, i, lat_name, qRect.max_y);
        i++;
        break;
      }
    for (; i < query_Graph.label_list.length; i++)
      if (query_Graph.spa_predicate[i] != null) {
        MyRectangle qRect = query_Graph.spa_predicate[i];
        query += String.format(" and %f < a%d.%s < %f ", qRect.min_x, i, lon_name, qRect.max_x);
        query += String.format("and %f < a%d.%s < %f", qRect.min_y, i, lat_name, qRect.max_y);
      }

    // return
    query += " return id(a0)";
    for (i = 1; i < query_Graph.graph.size(); i++)
      query += String.format(",id(a%d)", i);

    return query;
  }

  /**
   * for the cypher query for profile or explain with given query graph
   * 
   * @param query_Graph
   * @param limit
   * @param Explain_Or_Profile set to 1 if profile, -1 if Explain, otherwise 0
   * @return
   */
  public static String FormCypherQuery(Query_Graph query_Graph, int limit, int Explain_Or_Profile,
      String lon_name, String lat_name) {
    String query = "";
    if (Explain_Or_Profile == 1)
      query += "profile match ";
    else if (Explain_Or_Profile == -1) {
      query += "explain match ";
    } else {
      query += "match ";
    }

    // label
    query += String.format("(a0:GRAPH_%d)", query_Graph.label_list[0]);
    for (int i = 1; i < query_Graph.graph.size(); i++) {
      query += String.format(",(a%d:GRAPH_%d)", i, query_Graph.label_list[i]);
    }

    // edge
    for (int i = 0; i < query_Graph.graph.size(); i++) {
      for (int j = 0; j < query_Graph.graph.get(i).size(); j++) {
        int neighbor = query_Graph.graph.get(i).get(j);
        if (neighbor > i)
          query += String.format(",(a%d)--(a%d)", i, neighbor);
      }
    }

    // spatial predicate
    int i = 0;
    for (; i < query_Graph.label_list.length; i++)
      if (query_Graph.spa_predicate[i] != null) {
        MyRectangle qRect = query_Graph.spa_predicate[i];
        query += String.format(" where %f <= a%d.%s <= %f ", qRect.min_x, i, lon_name, qRect.max_x);
        query += String.format("and %f <= a%d.%s <= %f", qRect.min_y, i, lat_name, qRect.max_y);
        i++;
        break;
      }
    for (; i < query_Graph.label_list.length; i++)
      if (query_Graph.spa_predicate[i] != null) {
        MyRectangle qRect = query_Graph.spa_predicate[i];
        query += String.format(" and %f <= a%d.%s <= %f ", qRect.min_x, i, lon_name, qRect.max_x);
        query += String.format("and %f <= a%d.%s <= %f", qRect.min_y, i, lat_name, qRect.max_y);
      }

    // return
    query += " return a0";
    for (i = 1; i < query_Graph.graph.size(); i++)
      query += String.format(",a%d", i);

    if (limit != -1)
      query += String.format(" limit %d", limit);

    return query;
  }

  public static void println(Object o) {
    System.out.println(o);
  }

  /**
   * Compute the set difference set2 - set1.
   *
   * @param set1
   * @param set2
   * @return a set contains all elements in set2 that are not in set1
   */
  public static <T> Set<T> setDiff(Set<T> set1, Set<T> set2) {
    Set<T> diff = new HashSet<>();
    for (T element : set2) {
      if (!set1.contains(element)) {
        diff.add(element);
      }
    }
    return diff;
  }


}
