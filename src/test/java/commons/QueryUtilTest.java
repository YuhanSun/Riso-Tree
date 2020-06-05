package commons;

import static org.junit.Assert.assertArrayEquals;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import commons.QueryUtil;

public class QueryUtilTest {

  @Test
  public void getResultRowInArray() throws Exception {
    String[] columnNames = new String[] {"id(a0)", "id(a1)", "id(a2)"};
    Map<String, Object> row = new HashMap<>();
    long[] baseline = new long[] {(long) 0, (long) 1, (long) 2};
    row.put("id(a0)", baseline[0]);
    row.put("id(a1)", baseline[1]);
    row.put("id(a2)", baseline[2]);
    long[] ids = QueryUtil.getResultRowInArray(columnNames, row);
    assertArrayEquals(ids, baseline);
  }

}
