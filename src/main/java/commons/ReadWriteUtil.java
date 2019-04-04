package commons;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Logger;

public class ReadWriteUtil {

  private static final Logger LOGGER = Logger.getLogger(ReadWriteUtil.class.getName());

  /**
   * Read a map <Integer, String> from file and return as a String[]. Non-exist keys will have value
   * null. The purpose is to save storage overhead and accelerate the speed of the get operation.
   *
   * @param filepath
   * @param size should be >= than all integer keys in the map
   * @return
   * @throws Exception
   */
  public static String[] readMapAsArray(String filepath, int size) throws Exception {
    LOGGER.info(String.format("read Label map from %s with size %d", filepath, size));
    BufferedReader reader = new BufferedReader(new FileReader(filepath));
    String line = null;
    String[] map = new String[size];
    Arrays.fill(map, null);
    while ((line = reader.readLine()) != null) {
      String[] strings = line.split(",");
      int graphId = Integer.parseInt(strings[0]);
      if (map[graphId] == null) {
        map[graphId] = strings[1];
      }
    }
    reader.close();
    return map;
  }

  public static void writeEdges(Iterable<Edge> edges, String path, boolean app) throws IOException {
    FileWriter writer = new FileWriter(path, app);
    for (Edge edge : edges) {
      writer.write(edge.toString() + "\n");
    }
    writer.close();
  }

  public static <T> void WriteArray(String filename, List<T> arrayList) {
    FileWriter fileWriter = null;
    try {
      fileWriter = new FileWriter(new File(filename));
      for (T line : arrayList)
        fileWriter.write(line.toString() + "\n");
      fileWriter.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * read integer arraylist
   * 
   * @param path
   * @return
   */
  public static ArrayList<Integer> readIntegerArray(String path) {
    String line = null;
    ArrayList<Integer> arrayList = new ArrayList<Integer>();
    try {
      BufferedReader reader = new BufferedReader(new FileReader(new File(path)));
      while ((line = reader.readLine()) != null) {
        int x = Integer.parseInt(line);
        arrayList.add(x);
      }
      reader.close();
    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
    }
    return arrayList;
  }

  /**
   * Read map from file
   * 
   * @param filename
   * @return
   */
  public static HashMap<String, String> ReadMap(String filename) {
    try {
      HashMap<String, String> map = new HashMap<String, String>();
      BufferedReader reader = new BufferedReader(new FileReader(new File(filename)));
      String line = null;
      while ((line = reader.readLine()) != null) {
        String[] liStrings = line.split(",");
        map.put(liStrings[0], liStrings[1]);
      }
      reader.close();
      return map;
    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
    }
    Util.println("nothing in ReadMap(" + filename + ")");
    return null;
  }

  /**
   * Read a map to a long[] array. So the key has to be consecutive in the [0, count-1].
   *
   * @param filename
   * @return
   */
  public static long[] readMapAsArray(String filename) {
    HashMap<String, String> graph_pos_map = ReadMap(filename);
    long[] graph_pos_map_list = new long[graph_pos_map.size()];
    for (String key_str : graph_pos_map.keySet()) {
      int key = Integer.parseInt(key_str);
      int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
      graph_pos_map_list[key] = pos_id;
    }
    return graph_pos_map_list;
  }

  /**
   * write map to file
   * 
   * @param filename
   * @param app append or not
   * @param map
   */
  public static void WriteMap(String filename, boolean app, Map<Object, Object> map) {
    try {
      FileWriter fWriter = new FileWriter(filename, app);
      Set<Entry<Object, Object>> set = map.entrySet();
      Iterator<Entry<Object, Object>> iterator = set.iterator();
      while (iterator.hasNext()) {
        Entry<Object, Object> element = iterator.next();
        fWriter.write(
            String.format("%s,%s\n", element.getKey().toString(), element.getValue().toString()));
      }
      fWriter.close();
    } catch (Exception e) {
      // TODO: handle exception
    }
  }

  public static void WriteFile(String filename, boolean app, String str) {
    try {
      FileWriter fw = new FileWriter(filename, app);
      fw.write(str);
      fw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void WriteFile(String filename, boolean app, List<String> lines) {
    try {
      FileWriter fw = new FileWriter(filename, app);
      int i = 0;
      while (i < lines.size()) {
        fw.write(String.valueOf(lines.get(i)) + "\n");
        ++i;
      }
      fw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void WriteFile(String filename, boolean app, Set<String> lines) {
    try {
      FileWriter fw = new FileWriter(filename, app);
      for (String line : lines)
        fw.write(line + "\n");
      fw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static List<MyRectangle> ReadQueryRectangle(String filepath) {
    ArrayList<MyRectangle> queryrectangles;
    queryrectangles = new ArrayList<MyRectangle>();
    BufferedReader reader = null;
    File file = null;
    try {
      file = new File(filepath);
      reader = new BufferedReader(new FileReader(file));
      String temp = null;
      while ((temp = reader.readLine()) != null) {
        if (temp.contains("%"))
          continue;
        String[] line_list = temp.split("\t");
        MyRectangle rect =
            new MyRectangle(Double.parseDouble(line_list[0]), Double.parseDouble(line_list[1]),
                Double.parseDouble(line_list[2]), Double.parseDouble(line_list[3]));
        queryrectangles.add(rect);
      }
      reader.close();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (reader != null)
        try {
          reader.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
    }
    return queryrectangles;
  }

}
