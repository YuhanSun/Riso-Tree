package CypherMiddleWare;

import static org.junit.Assert.assertTrue;
import org.junit.Test;
import commons.Config.Explain_Or_Profile;
import commons.Query_Graph;
import commons.TestUtils;
import commons.Util;
import cypher.middleware.CypherEncoder;

public class CypherEncoderTest {

  @Test
  public void formCypherQueryTest() {
    Query_Graph query_Graph = TestUtils.getExampleGraph();
    String queryString = CypherEncoder.formCypherQuery(query_Graph, -1, Explain_Or_Profile.Explain);
    Util.println(queryString);
    String baselineString =
        "explain match (a0:`0`),(a1:`1`),(a2:`2`),(a0)--(a1),(a0)--(a2) where 0.000000 <= a0.lon <= 2.000000 and 1.000000 <= a0.lat <= 3.000000 return id(a0),id(a1),id(a2)";
    assertTrue(queryString.equals(baselineString));
  }

}
