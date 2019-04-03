package graph;

import java.io.File;
import java.util.LinkedList;
import java.util.Queue;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import commons.Config;
import commons.Labels;
import commons.MyRectangle;
import commons.Query_Graph;
import commons.Util;

public class HMBR {

  public GraphDatabaseService dbservice;

  public static Config config = new Config();
  public String lon_name = config.GetLongitudePropertyName();
  public String lat_name = config.GetLatitudePropertyName();

  // HMBR
  public int MAX_HMBRHOPNUM = config.getMaxHMBRHopNum();
  String minx_name = config.GetRectCornerName()[0];
  String miny_name = config.GetRectCornerName()[1];
  String maxx_name = config.GetRectCornerName()[2];
  String maxy_name = config.GetRectCornerName()[3];

  public HMBR(String dbPath) {
    dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbPath));
  }

  public static int[][] Ini_Minhop(Query_Graph query_Graph) {
    int query_node_count = query_Graph.graph.size();
    int[][] minhop_index = new int[query_node_count][];

    for (int i = 0; i < query_node_count; i++) {
      if (query_Graph.spa_predicate[i] == null)
        minhop_index[i] = null;
      else
        minhop_index[i] = new int[query_node_count];
    }

    for (int i = 0; i < query_node_count; i++) {
      if (query_Graph.spa_predicate[i] != null) {
        boolean[] visited = new boolean[query_node_count];
        visited[i] = true;
        minhop_index[i][i] = -1;

        Queue<Integer> queue = new LinkedList<Integer>();
        queue.add(i);
        int pre_level_count = 1;
        int cur_level_count = 0;
        int level_index = 1;

        while (queue.isEmpty() == false) {
          for (int j = 0; j < pre_level_count; j++) {
            int node = queue.poll();
            for (int k = 0; k < query_Graph.graph.get(node).size(); k++) {
              int neighbor = query_Graph.graph.get(node).get(k);
              if (visited[neighbor] == false) {
                minhop_index[i][neighbor] = level_index;
                visited[neighbor] = true;
                cur_level_count += 1;
                queue.add(neighbor);
              }
            }
          }
          level_index++;
          pre_level_count = cur_level_count;
          cur_level_count = 0;
        }
      }
    }
    return minhop_index;
  }

  /**
   * form cypher query with minhop check
   * 
   * @param query_Graph
   * @param limit
   * @param minhop_index
   * @param Profile_Or_Explain Profile --> true, Explain --> false
   * @return
   */
  public String FormCypherQuery(Query_Graph query_Graph, int limit, int[][] minhop_index,
      boolean Profile_Or_Explain) {
    int query_node_count = query_Graph.graph.size();
    String query = "";
    if (Profile_Or_Explain)
      query += "profile match ";
    else
      query += "explain match ";

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

    // hmbr prune
    for (i = 0; i < query_node_count; i++) {
      if (query_Graph.spa_predicate[i] != null) {
        MyRectangle cur_rect = query_Graph.spa_predicate[i];
        for (int j = 0; j < query_node_count; j++) {
          int minhop = minhop_index[i][j];
          if (minhop != -1 && minhop <= MAX_HMBRHOPNUM) {
            query +=
                String.format(" and a%d.HMBR_%d_%s <= %f", j, minhop, minx_name, cur_rect.max_x);
            query +=
                String.format(" and a%d.HMBR_%d_%s <= %f", j, minhop, miny_name, cur_rect.max_y);
            query +=
                String.format(" and a%d.HMBR_%d_%s >= %f", j, minhop, maxx_name, cur_rect.min_x);
            query +=
                String.format(" and a%d.HMBR_%d_%s >= %f", j, minhop, maxy_name, cur_rect.min_y);
          }
        }
      }
    }

    // return
    query += " return a0";
    for (i = 1; i < query_Graph.graph.size(); i++)
      query += String.format(",a%d", i);

    if (limit != -1)
      query += String.format(" limit %d", limit);

    return query;
  }

  /**
   * Query using graph database service.
   * 
   * @param query_Graph
   * @param limit
   * @return
   */
  public Result Query(Query_Graph query_Graph, int limit) {
    int[][] minhop_index = Ini_Minhop(query_Graph);

    String query = FormCypherQuery(query_Graph, limit, minhop_index, true);

    Util.println(query);
    Result result = dbservice.execute(query);
    return result;
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
