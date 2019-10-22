package commons;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;
import java.util.logging.Logger;

public class GraphUtil {

  private static final Logger LOGGER = Logger.getLogger(GraphUtil.class.getName());

  public static void extractSpatialEntities(String entityPath, String outputPath) throws Exception {
    ArrayList<Entity> entities = ReadEntity(entityPath);
    FileWriter writer = new FileWriter(outputPath);
    for (Entity entity : entities) {
      if (entity.IsSpatial) {
        writer.write(String.format("%d,1,%s,%s\n", entity.id, String.valueOf(entity.lon),
            String.valueOf(entity.lat)));
      }
    }
    writer.close();
  }

  /**
   * Convert a bidirectional graph to bidirectional edge format.
   *
   * @param graphPath
   * @param edgePath
   * @throws Exception
   */
  public static void convertGraphToEdgeFormat(String graphPath, String edgePath) throws Exception {
    ArrayList<ArrayList<Integer>> graph = ReadGraph(graphPath);
    FileWriter writer = new FileWriter(edgePath);
    int id = 0;
    for (ArrayList<Integer> neighbors : graph) {
      for (int neighbor : neighbors) {
        writer.write(String.format("%d,%d\n", id, neighbor));
      }
      id++;
    }
    writer.close();
  }

  public static List<Edge> readEdges(String edgePath) throws Exception {
    LOGGER.info("read edges from " + edgePath);
    List<Edge> edges = new LinkedList<>();
    BufferedReader reader = Util.getBufferedReader(edgePath);
    String string = null;
    while ((string = reader.readLine()) != null) {
      int[] startEnd = getEdgeStartEnd(string);
      Edge edge = new Edge(startEnd[0], startEnd[1]);
      edges.add(edge);
    }
    reader.close();
    return edges;
  }

  public static int getEdgeCount(List<Collection<Integer>> graph) {
    int count = 0;
    for (Collection<Integer> neighbors : graph) {
      count += neighbors.size();
    }
    return count;
  }

  public static int getEdgeCount(ArrayList<ArrayList<Integer>> graph) {
    int count = 0;
    for (Collection<Integer> neighbors : graph) {
      count += neighbors.size();
    }
    return count;
  }

  /**
   * Read graph from a file.
   *
   * @param graphPath
   * @return
   */
  public static ArrayList<ArrayList<Integer>> ReadGraph(String graphPath) {
    LOGGER.info("read graph from " + graphPath);
    ArrayList<ArrayList<Integer>> graph = null;
    BufferedReader reader = null;
    String str = "";
    try {
      reader = new BufferedReader(new FileReader(new File(graphPath)));
      str = reader.readLine();
      int nodeCount = Integer.parseInt(str);
      graph = new ArrayList<ArrayList<Integer>>(nodeCount);
      int index = -1;
      while ((str = reader.readLine()) != null) {
        index++;
        String[] l_str = str.split(",");
        int id = Integer.parseInt(l_str[0]);
        if (id != index)
          throw new Exception(
              String.format("this line has id %d, but the index should be %d!", id, index));
        int neighbor_count = Integer.parseInt(l_str[1]);
        ArrayList<Integer> line = new ArrayList<Integer>(neighbor_count);
        if (neighbor_count == 0) {
          graph.add(line);
          continue;
        }
        int i = 2;
        while (i < l_str.length) {
          line.add(Integer.parseInt(l_str[i]));
          ++i;
        }
        graph.add(line);
      }
      reader.close();
      if (nodeCount != index + 1)
        throw new Exception(String.format(
            "first line shows node count is %d, but only has %d lines!", nodeCount, index + 1));
    } catch (Exception e) {
      Util.println(str);
      if (reader != null)
        try {
          reader.close();
        } catch (IOException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
      e.printStackTrace();
      System.exit(-1);
    }
    return graph;
  }

  /**
   * Convert from a Collection graph to ArrayList.
   *
   * @param graph
   * @return
   */
  public static ArrayList<ArrayList<Integer>> convertCollectionGraphToArrayList(
      List<Collection<Integer>> graph) {
    ArrayList<ArrayList<Integer>> res = new ArrayList<>(graph.size());
    for (Collection<Integer> neighbors : graph) {
      res.add(new ArrayList<>(neighbors));
    }
    return res;
  }

  /**
   * Convert from a list graph to treeset graph.
   *
   * @param listGraph
   * @return
   */
  public static ArrayList<TreeSet<Integer>> convertListGraphToTreeSetGraph(
      ArrayList<ArrayList<Integer>> listGraph) {
    LOGGER.info("convert from list graph to treeset graph...");
    ArrayList<TreeSet<Integer>> graph = new ArrayList<>(listGraph.size());
    for (List<Integer> neighbors : listGraph) {
      graph.add(new TreeSet<>(neighbors));
    }
    return graph;
  }

  /**
   * Read entities
   * 
   * @param entityPath
   * @return
   */
  public static ArrayList<Entity> ReadEntity(String entityPath) {
    LOGGER.info("read entities from " + entityPath);
    ArrayList<Entity> entities = null;
    BufferedReader reader = null;
    String str = null;
    int id = 0;
    try {
      if (!Util.pathExist(entityPath))
        throw new Exception(entityPath + " does not exist");

      reader = new BufferedReader(new FileReader(new File(entityPath)));
      str = reader.readLine();
      int node_count = Integer.parseInt(str);
      entities = new ArrayList<Entity>(node_count);
      while ((str = reader.readLine()) != null) {
        Entity entity;
        String[] str_l = str.split(",");
        int flag = Integer.parseInt(str_l[1]);
        if (flag == 0) {
          entity = new Entity(id);
          entities.add(entity);
        } else {
          entity = new Entity(id, Double.parseDouble(str_l[2]), Double.parseDouble(str_l[3]));
          entities.add(entity);
        }
        ++id;
      }
      reader.close();
    } catch (Exception e) {
      Util.println(String.format("error happens in entity id %d", id));
      e.printStackTrace();
      System.exit(-1);
    }
    return entities;
  }

  /**
   * Write a graph to a file.
   * 
   * @param graph
   * @param graphPath
   * @throws Exception
   */
  public static void writeGraphArrayList(ArrayList<ArrayList<Integer>> graph, String graphPath)
      throws Exception {
    FileWriter writer = new FileWriter(new File(graphPath));
    writer.write(String.format("%d\n", graph.size())); // Write node count in the graph.
    for (int i = 0; i < graph.size(); i++) {
      ArrayList<Integer> neighborList = graph.get(i);
      writer.write(String.format("%d,%d", i, neighborList.size()));
      for (int neighbor : neighborList)
        writer.write(String.format(",%d", neighbor));
      writer.write("\n");
    }
    writer.close();
  }

  /**
   * Write a graph to a file.
   * 
   * @param graph
   * @param graphPath
   */
  public static void writeGraphTreeSet(ArrayList<TreeSet<Integer>> graph, String graphPath) {
    try {
      FileWriter writer = new FileWriter(new File(graphPath));
      writer.write(String.format("%d\n", graph.size())); // Write node count in the graph.
      for (int i = 0; i < graph.size(); i++) {
        Collection<Integer> neighborList = graph.get(i);
        writer.write(String.format("%d,%d", i, neighborList.size()));
        for (int neighbor : neighborList)
          writer.write(String.format(",%d", neighbor));
        writer.write("\n");
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static void writeEntityToFile(ArrayList<Entity> entities, String entityPath) {
    FileWriter writer = null;
    try {
      writer = new FileWriter(new File(entityPath));
      writer.write(entities.size() + "\n");
      for (Entity entity : entities) {
        writer.write(entity.id + ",");
        if (entity.IsSpatial)
          writer.write(
              String.format("1,%s,%s\n", String.valueOf(entity.lon), String.valueOf(entity.lat)));
        else
          writer.write("0\n");
      }
      writer.close();
    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static int[] getEdgeStartEnd(String edgeString) {
    String[] strings = edgeString.split(",");
    int start = 0, end = 0;
    if (strings.length == 3) {
      start = Integer.parseInt(strings[0]);
      end = Integer.parseInt(strings[2]);
    } else if (strings.length == 2) {
      start = Integer.parseInt(strings[0]);
      end = Integer.parseInt(strings[1]);
    } else {
      throw new RuntimeException(edgeString + " is not edge-format!");
    }
    return new int[] {start, end};
  }

}
