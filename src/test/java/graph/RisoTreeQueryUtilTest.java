package graph;

import static org.junit.Assert.assertTrue;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import commons.Util;

public class RisoTreeQueryUtilTest {

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void formIdConstraintTest() {
    Map<Integer, Collection<Long>> candidateSets = new HashMap<>();
    Set<Long> collection = new HashSet<>();
    collection.add((long) 0);
    collection.add((long) 1);
    candidateSets.put(0, collection);

    Set<Long> collection2 = new HashSet<>();
    collection2.add((long) 10);
    collection2.add((long) 20);
    candidateSets.put(1, collection2);

    String[] nodeVariables = new String[] {"a", "b"};

    Util.println(RisoTreeQueryPN.formIdConstraint(candidateSets, nodeVariables));
  }

  @Test
  public void formQueryWithIgnoreTest() {
    Map<Integer, Collection<Long>> candidateSets = new HashMap<>();
    Set<Long> collection = new HashSet<>();
    collection.add((long) 0);
    collection.add((long) 1);
    candidateSets.put(0, collection);

    Set<Long> collection2 = new HashSet<>();
    collection2.add((long) 10);
    collection2.add((long) 20);
    candidateSets.put(1, collection2);

    String[] nodeVariables = new String[] {"a", "b"};
    String query =
        "explain match (a:A)-[]->(b:B) where 20 < a.lon < 30 and 10 < a.lat < 20 return *";
    String queryAfterRewrite =
        RisoTreeQueryPN.formQueryWithIgnore(query, candidateSets, nodeVariables);
    Util.println(queryAfterRewrite);
    assertTrue(queryAfterRewrite.equals(
        "explain match (a:A)-[]->(b:B) where (id(a) in [0, 1] or id(b) in [20, 10]) and 20 < a.lon < 30 and 10 < a.lat < 20 return *"));
  }

}
