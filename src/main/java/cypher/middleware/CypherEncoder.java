package cypher.middleware;

import commons.Config;
import commons.Config.Explain_Or_Profile;
import commons.MyRectangle;
import commons.Query_Graph;
import commons.Util;

public class CypherEncoder {

  public static String lon_name = new Config().GetLongitudePropertyName();
  public static String lat_name = new Config().GetLatitudePropertyName();

  public static String getMatchPrefix(Explain_Or_Profile explain_Or_Profile) {
    switch (explain_Or_Profile) {
      case Profile:
        return "profile match";
      case Explain:
        return "explain match";
      case Nothing:
        return "match";
      default:
        throw new RuntimeException(
            "Explain_Or_Profile unknown value: " + explain_Or_Profile.toString());
    }
  }

  public static String getMatchNodeLabelString(Query_Graph query_Graph) {
    String query = "";
    switch (query_Graph.labelType) {
      case INT:
        query += String.format("(a0:GRAPH_%d)", query_Graph.label_list[0]);
        for (int i = 1; i < query_Graph.graph.size(); i++) {
          query += String.format(",(a%d:GRAPH_%d)", i, query_Graph.label_list[i]);
        }
        break;
      case STRING:
        query += String.format("(a0:`%s`)", query_Graph.label_list_string[0]);
        for (int i = 1; i < query_Graph.graph.size(); i++) {
          query += String.format(",(a%d:`%s`)", i, query_Graph.label_list_string[i]);
        }
        break;
      default:
        throw new RuntimeException(String.format("query_Graph label %s", query_Graph.labelType));
    }
    return query;
  }

  public static String getMatchEdgeString(Query_Graph query_Graph) {
    String query = "";
    for (int i = 0; i < query_Graph.graph.size(); i++) {
      for (int j = 0; j < query_Graph.graph.get(i).size(); j++) {
        int neighbor = query_Graph.graph.get(i).get(j);
        if (neighbor > i)
          query += String.format(",(a%d)--(a%d)", i, neighbor);
      }
    }
    return query;
  }

  public static String getMatchGraphSkeletonString(Query_Graph query_Graph) {
    return getMatchNodeLabelString(query_Graph) + getMatchEdgeString(query_Graph);
  }

  public static String getMatchReturnString(Query_Graph query_Graph) {
    String query = "return id(a0)";
    for (int i = 1; i < query_Graph.graph.size(); i++)
      query += String.format(",id(a%d)", i);
    return query;
  }

  public static String getMatchRangePredicateString(Query_Graph query_Graph) {
    String query = "";
    int i = 0;
    for (; i < query_Graph.graph.size(); i++) {
      if (query_Graph.spa_predicate[i] != null) {
        MyRectangle qRect = query_Graph.spa_predicate[i];
        query += String.format("%f <= a%d.%s <= %f ", qRect.min_x, i, lon_name, qRect.max_x);
        query += String.format("and %f <= a%d.%s <= %f", qRect.min_y, i, lat_name, qRect.max_y);
        i++;
        break;
      }
    }
    for (; i < query_Graph.graph.size(); i++) {
      if (query_Graph.spa_predicate[i] != null) {
        MyRectangle qRect = query_Graph.spa_predicate[i];
        query += String.format(" and %f <= a%d.%s <= %f ", qRect.min_x, i, lon_name, qRect.max_x);
        query += String.format("and %f <= a%d.%s <= %f", qRect.min_y, i, lat_name, qRect.max_y);
      }
    }
    return query;
  }

  public static String getLimit(int limit) {
    return limit == -1 ? "" : String.format("limit %d", limit);
  }

  /**
   * Form the cypher query for profile or explain with given query graph.
   *
   * @param query_Graph
   * @param limit
   * @param Explain_Or_Profile set to 1 if profile, -1 if Explain, otherwise 0
   * @return
   */
  public static String formCypherQuery(Query_Graph query_Graph, int limit,
      Explain_Or_Profile explain_Or_Profile) {
    String query = getMatchPrefix(explain_Or_Profile) + " ";
    query += getMatchGraphSkeletonString(query_Graph);

    // where range predicate
    String rangePredicateString = getMatchRangePredicateString(query_Graph);
    Util.println(rangePredicateString);
    if (rangePredicateString != "") {
      query += " where " + rangePredicateString;
    }

    // return
    query += " " + getMatchReturnString(query_Graph);

    // limit
    if (limit != -1) {
      query += " " + getLimit(limit);
    }

    return query;
  }
}
