package commons;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.strtree.STRtree;
import dataprocess.Wikidata;

public class OwnMethodsTest {

  String dataDir;

  @Before
  public void setUp() throws Exception {
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource("").getFile());
    dataDir = file.getAbsolutePath() + "/data";
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void test() {
    fail("Not yet implemented");
  }

  @Test
  public void constructSTreeWithEntitiesTest() {
    List<Entity> entities = new LinkedList<>();
    Entity e1 = new Entity(0, 0, 0);
    Entity e2 = new Entity(1, 1, 1);
    entities.add(e1);
    entities.add(e2);

    STRtree stRtree = OwnMethods.constructSTRTreeWithEntities(entities);
    List<?> result = stRtree.query(new Envelope(-0.5, 0.5, -0.5, 0.5));
    assertTrue(result.size() == 1);
    Entity entity = (Entity) result.get(0);
    assertTrue(entity.id == 0);

    result = stRtree.query(new Envelope(-0.5, 1.5, -0.5, 1.5));
    assertTrue(result.size() == 2);
  }

  @Test
  public void kNearestDistanceTest() {
    ArrayList<Entity> entities = new ArrayList<>();
    Entity e1 = new Entity(0, 0, 0);
    Entity e2 = new Entity(1, 1, 1);
    Entity e3 = new Entity(2, 2, 2);
    entities.add(e1);
    entities.add(e2);
    entities.add(e3);

    STRtree stRtree = OwnMethods.constructSTRTree(entities);
    double radius = OwnMethods.kNearestDistance(stRtree, 0, 0, 2);
    assertEquals(radius, Math.sqrt(2.0), 0.00000001);
  }

  @Test
  public void GenerateRandomGraphStringLabelTest() throws Exception {
    String smallGraphDir = dataDir + "/smallGraph";
    ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(dataDir + "/smallGraph/graph.txt");
    ArrayList<ArrayList<Integer>> labels =
        GraphUtil.ReadGraph(dataDir + "/smallGraph/label_graph.txt");
    String[] labelStringMap = Wikidata.readLabelMap(smallGraphDir + "/label_string.txt");
    ArrayList<Entity> entities = GraphUtil.ReadEntity(smallGraphDir + "/entity.txt");
    int node_count = 3;
    int startSpatialId = 4;
    Query_Graph query_Graph = OwnMethods.GenerateRandomGraphStringLabel(graph, labels,
        labelStringMap, entities, node_count, startSpatialId);
    // Util.println(dataDir);
    // String[] files = new File(dataDir).list();
    // for (String file : files) {
    // Util.println(file);
    // }
    Util.println(query_Graph.toString());
  }

}
