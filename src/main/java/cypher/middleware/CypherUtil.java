package cypher.middleware;

import org.apache.commons.lang3.StringUtils;

public class CypherUtil {
  public static String removeWhere(String query) {
    String[] strings = StringUtils.splitByWholeSeparator(query, " where ");
    String matchPartString = strings[0];
    String afterWherePartString = strings[1];
    String returnPartString =
        StringUtils.splitByWholeSeparator(afterWherePartString, " return ")[1];
    return matchPartString + " return " + returnPartString;
  }
}
