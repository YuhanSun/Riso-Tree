package commons;

import java.util.Map;

public class QueryUtil {
  public static long[] getResultRowInArray(String[] columnNames, Map<String, Object> row)
      throws Exception {
    long[] ids = new long[columnNames.length];
    for (int i = 0; i < ids.length; i++) {
      String columnName = columnNames[i];
      if (!row.containsKey(columnName)) {
        throw new Exception(
            String.format("column %s not found in the returned result row!", columnName));
      }
      ids[i] = ((Long) row.get(columnNames[i])).longValue();
    }
    return ids;
  }
}
