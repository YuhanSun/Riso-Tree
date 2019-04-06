package commons;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.neo4j.cypher.internal.frontend.v3_4.ast.Match;
import org.neo4j.cypher.internal.frontend.v3_4.ast.Query;
import org.neo4j.cypher.internal.frontend.v3_4.ast.SingleQuery;
import org.neo4j.cypher.internal.frontend.v3_4.ast.Statement;
import org.neo4j.cypher.internal.frontend.v3_4.parser.CypherParser;
import org.neo4j.cypher.internal.v3_4.expressions.EveryPath;
import org.neo4j.cypher.internal.v3_4.expressions.Pattern;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.index.strtree.STRtree;
import commons.Config.system;
import commons.Labels.GraphLabel;
import graph.SpatialFirst;
import osm.OSM_Utility;
import scala.collection.JavaConversions;

public class App {

  static Config config = new Config();
  static String dataset = config.getDatasetName();
  static String lon_name = config.GetLongitudePropertyName();
  static String lat_name = config.GetLatitudePropertyName();
  static String password = config.getPassword();

  static String db_path;
  static String querygraph_path;
  static String graph_pos_map_path;
  static String log_path;
  static int query_id;
  static ArrayList<Query_Graph> queryGraphs;
  static Query_Graph query_Graph;
  static MyRectangle queryRectangle;

  public static void initVariablesForTest() {
    system systemName = config.getSystemName();
    String neo4jVersion = config.GetNeo4jVersion();
    switch (systemName) {
      case Ubuntu:
        db_path = String.format(
            "/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db",
            neo4jVersion, dataset);
        querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph.txt";
        graph_pos_map_path =
            "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/node_map_RTree.txt";
        log_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/test.log";
        break;
      case Windows:
        db_path = String.format(
            "D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset,
            neo4jVersion, dataset);
        querygraph_path = "D:\\Ubuntu_shared\\GeoMinHop\\query\\query_graph.txt";
        graph_pos_map_path =
            "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset + "\\node_map_RTree.txt";
        log_path = "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset + "\\test.log";
      default:
        break;
    }
    query_id = 5;
    // queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
    // query_Graph = queryGraphs.get(query_id);
    queryRectangle = new MyRectangle(-80.115808, 26.187365, -80.115808, 26.187365);// 1
    // queryRectangle = new MyRectangle(-80.119514, 26.183659, -80.112102, 26.191071);//10
    // queryRectangle = new MyRectangle(-80.133549, 26.169624, -80.098067, 26.205106);//100
    // queryRectangle = new MyRectangle(-84.468680, 33.879658, -84.428434, 33.919904);//100
    // queryRectangle = new MyRectangle(-80.200353, 26.102820, -80.031263, 26.271910);//1000
    // queryRectangle = new MyRectangle(-98.025157, 29.953977, -97.641747, 30.337387);//10000
    // queryRectangle = new MyRectangle(-91.713778, 14.589395, -68.517838, 37.785335);//100000
  }


  public static void main(String[] args) {

    ServiceTest();
    // CypherQueryTest();
    // initVariablesForTest();
    // Naive();
    // test();
    // batchRTreeInsert();
    // generateLabelList();
    // rangeQueryCompare();
    // rangeQueryCountCompare();
    // graphCompare();
    // cliqueTest();
    // propertyPageAccessTest();
    // matchOnDifferentLabelCountDatabase();
    // getAllLabels();
    // restartNeo4jJava();
    // getLeafStatisticalInfo();
  }

  public static void ServiceTest() {
    String query3 = "match (a:A)-->(b:B)<--(c:TEST) where a.type = 0 return a, b, c limit 10";
    String dbPath = "D:\\temp/graph.db";
    GraphDatabaseService service = Neo4jGraphUtility.getDatabaseServiceNotExistCreate(dbPath);
    Result result = service.execute("explain " + query3);
    // while (result.hasNext()) {
    // Util.println(result.next());
    // }
    ExecutionPlanDescription executionPlanDescription = result.getExecutionPlanDescription();
    Util.println(executionPlanDescription);
    List<ExecutionPlanDescription> plans =
        ExecutionPlanDescriptionUtil.getRequired(executionPlanDescription);
    for (ExecutionPlanDescription planDescription : plans) {
      Util.println(planDescription.getArguments());
      Util.println(planDescription.getIdentifiers());
      Util.println(planDescription.getName());;
    }
    service.shutdown();
  }

  public static void CypherQueryTest() {
    CypherParser parser = new CypherParser();
    String query1 = "start a = node(*) match p = (a)--(b)--(c),q = (c)--(d) return p,q limit 10";
    String query2 =
        "start a = node(*) match (a)--(b)--(c:TEST) where a.type = 0 return a, b, c limit 10";
    String query3 = "match (a:A)--(b:B)--(c:TEST) where a.type = 0 return a, b, c limit 10";

    Statement statement = parser.parse(query3, null);
    Query query = (Query) statement;
    Util.println(query);
    // query.

    SingleQuery singleQuery = (SingleQuery) query.part();
    Util.println(singleQuery);
    Object object = singleQuery.productElement(0);
    Util.println(object.getClass());

    Collection collection = JavaConversions.asJavaCollection((scala.collection.Iterable) object);
    Util.println(collection);
    Util.println(collection.getClass());

    Iterator iterator = collection.iterator();
    while (iterator.hasNext()) {
      Object object2 = iterator.next();
      if (object2.getClass().equals(Match.class)) {
        Match match = (Match) object2;
        Pattern pattern = match.pattern();
        Util.println(pattern);
        List list = JavaConversions.seqAsJavaList(pattern.patternParts());
        for (Object object3 : list) {
          if (object3.getClass().equals(EveryPath.class)) {
            EveryPath everyPath = (EveryPath) object3;
          }
          Util.println(object3.getClass());
        }

        break;
      }

    }

    // Util.println(collection);
    // QueryPart queryPart = query.part().productElement(0);
    // Util.println(queryPart.);

    Util.println(statement.returnColumns());
  }

  public static void getLeafStatisticalInfo() throws Exception {
    GraphDatabaseService databaseService =
        new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
    Transaction tx = databaseService.beginTx();
    List<Node> nodes = RTreeUtility.getRTreeLeafLevelNodes(databaseService, dataset);
    ArrayList<Integer> statistic = new ArrayList<Integer>();
    for (Node node : nodes) {
      int count = 0;
      Iterable<String> keys = node.getPropertyKeys();
      for (String key : keys) {
        // if ( key.matches("PN_\\d+_\\d+_size$"))
        if (key.matches("PN_\\d+_\\d+$")) {
          count++;
          // OwnMethods.Print(key);
        }
      }
      statistic.add(count);
    }
    tx.success();
    tx.close();
    Util.println(statistic);
    databaseService.shutdown();
    ArrayList<Integer> result = Util.groupSum(0, 10201, statistic, 10201);
    Util.println(result);
    Util.println(statistic.size());
  }

  public static void test() {
    // Utility.print(80/100.0);
    String string = "PN_0";
    // String[] l = string.split("PN");
    // Utility.print(l);
    // string = l[1];
    string = string.replaceFirst("PN", "");
    Util.println(string);

    String[] l = string.split("_");
    Util.println(l);
    Util.println(l.length);
    // ArrayList<Integer> test = new ArrayList<>();
    // test.toString();

    // double distance = 0.000001;
    // String string = String.format("%s", String.valueOf(distance * distance));
    // OwnMethods.Print(string);

    // String string = String.format("%1$d", 1);
    // OwnMethods.Print(string);

    // GraphDatabaseService databaseService = new GraphDatabaseFactory()
    // .newEmbeddedDatabase(new File(db_path));
    // Transaction tx = databaseService.beginTx();
    //// Node node = databaseService.getNodeById(1294918);
    // Node node = databaseService.getNodeById(7);
    //
    // OwnMethods.Print(node.getAllProperties());
    //// Iterable<Relationship> rels = node.getRelationships();
    //// for ( Relationship relationship : rels)
    //// {
    ////// OwnMethods.Print(relationship);
    //// Node geom = relationship.getEndNode();
    //// OwnMethods.Print(geom.getAllProperties());
    //// break;
    //// }
    //
    // tx.success();
    // tx.close();
    // databaseService.shutdown();
  }

  public static void restartNeo4jJava() {
    GraphDatabaseService databaseService =
        new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
    databaseService.shutdown();
  }

  public static void matchOnDifferentLabelCountDatabase() {
    String dbpath1 =
        "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-3.1.1_Patents_2_random_20/data/databases/graph.db";
    String dbpath2 =
        "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-3.1.1_Patents_100_random_20/data/databases/graph.db";

    OwnMethods.ClearCache(password);
    GraphDatabaseService databaseService =
        new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbpath1));
    long start = System.currentTimeMillis();
    String query = "profile match (a0:GRAPH_0)-->(a1:GRAPH_1) return id(a0), id(a1)";
    Result result = databaseService.execute(query);
    while (result.hasNext())
      result.next();
    long time = System.currentTimeMillis() - start;
    Util.println(time);
    Util.println(result.getExecutionPlanDescription());
    databaseService.shutdown();

    OwnMethods.ClearCache(password);
    databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbpath2));
    query = "profile match (a0:GRAPH_2)-->(a1:GRAPH_1) return id(a0), id(a1)";
    result = databaseService.execute(query);
    while (result.hasNext())
      result.next();
    time = System.currentTimeMillis() - start;
    Util.println(time);
    Util.println(result.getExecutionPlanDescription());
    databaseService.shutdown();
  }


  public static void propertyPageAccessTest() {
    GraphDatabaseService databaseService =
        new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
    // String query = "profile match (n:GRAPH_1) return id(n)";
    // String query = "profile match (n:GRAPH_1) where 10 < n.lat < 100 return id(n)";
    // String query = "profile match (n:GRAPH_1) where 10 < n.lon < 100 return id(n)";
    // String query = "profile match (n:GRAPH_2) where 10 < n.HMBR_1_minx return id(n)";
    String query = "explain match (n:GRAPH_2)--(a:GRAPH_1) where 10 < a.lat return id(n)";

    Transaction tx = databaseService.beginTx();
    Result result = databaseService.execute(query);
    // while (result.hasNext())
    // OwnMethods.Print(result.next());
    ExecutionPlanDescription description = result.getExecutionPlanDescription();
    Util.println(description.toString());
    tx.success();
    tx.close();
    databaseService.shutdown();
  }

  private static void cliqueTest() {
    GraphDatabaseService databaseService =
        new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
    String query =
        "match (a)--(b), (a)--(c), (b)--(c) return labels(a), labels(b), labels(c) limit 100";
    Transaction tx = databaseService.beginTx();
    Result result = databaseService.execute(query);
    while (result.hasNext())
      Util.println(result.next());
    tx.success();
    tx.close();
    databaseService.shutdown();

  }

  private static void graphCompare() {
    String graphPath1 =
        String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
    String graphPath2 = String
        .format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph_entity_newformat.txt", dataset);

    ArrayList<ArrayList<Integer>> graph1 = GraphUtil.ReadGraph(graphPath1);
    ArrayList<ArrayList<Integer>> graph2 = GraphUtil.ReadGraph(graphPath2);

    for (int i = 0; i < graph1.size(); i++)
      if (graph1.get(i).size() != graph2.get(i).size())
        Util.println(String.format("line %d different!", i));
  }

  private static void selectivityTest() {
    String entityPath =
        String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
    ArrayList<Entity> entities = OwnMethods.ReadEntity(entityPath);
    STRtree stRtree = OwnMethods.ConstructSTRee(entities);

    String queryRectPath = String.format(
        "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_%d.txt", dataset, 114);
    ArrayList<MyRectangle> queryRectangles = OwnMethods.ReadQueryRectangle(queryRectPath);

    int index = 0, countSum = 0;
    for (MyRectangle rect : queryRectangles) {
      List<Entity> results =
          stRtree.query(new Envelope(rect.min_x, rect.max_x, rect.min_y, rect.max_y));
      Util.println(results.size());
      countSum += results.size();
      index++;
    }
    Util.println(String.format("Average selectivity:%d", countSum / index));
  }

  /**
   * compare the range query result count of my method and GeoPipe defined in SpatialFirst class
   */
  public static void rangeQueryCountCompare() {
    String layerName = dataset;
    String queryRectanglePath = String
        .format("D:\\Ubuntu_shared\\GeoMinHop\\query\\spa_predicate\\%s\\queryrect_1.txt", dataset);
    // String queryRectanglePath =
    // String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_1.txt",
    // dataset);
    try {
      Util.println(db_path);
      GraphDatabaseService databaseService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      SpatialDatabaseService spatialDatabaseService = new SpatialDatabaseService(databaseService);
      Layer layer = spatialDatabaseService.getLayer(layerName);

      ArrayList<MyRectangle> queryRectangles = OwnMethods.ReadQueryRectangle(queryRectanglePath);
      long timeGeoPipe = 0, timeMyMethod = 0, countGeoPipe = 0, countMyMethod = 0;

      Transaction tx = databaseService.beginTx();
      Node root = RTreeUtility.getRTreeRoot(databaseService, layerName);
      for (MyRectangle queryRectangle : queryRectangles) {
        Envelope envelope = new Envelope(queryRectangle.min_x, queryRectangle.max_x,
            queryRectangle.min_y, queryRectangle.max_y);
        List<SpatialDatabaseRecord> resultGeoPipe = OSM_Utility.RangeQuery(layer, envelope);

        LinkedList<Node> resultMyMethod = SpatialFirst.rangeQuery(root, queryRectangle);

        Util.println(String.format("%d\t%d", resultGeoPipe.size(), resultMyMethod.size()));

        if (resultGeoPipe.size() != resultMyMethod.size()) {
          Util.println(queryRectangle);
          Util.println(String.format("GeoPile count:%d", resultGeoPipe.size()));
          Util.println(String.format("MyMethod count:%d", resultMyMethod.size()));

          Util.println("GeoPipe result:");
          for (SpatialDatabaseRecord record : resultGeoPipe) {
            Node node = record.getGeomNode();
            Util.println(node);
            double[] bbox = (double[]) node.getProperty("bbox");
            Util.println(Arrays.toString(bbox));
          }

          Util.println("\nMyMethod result:");
          for (Node node : resultMyMethod) {
            Util.println(node);
            double[] bbox = (double[]) node.getProperty("bbox");
            Util.println(Arrays.toString(bbox));
          }
          break;
        }
      }
      tx.success();
      tx.close();

      databaseService.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * compare the range query time of my method and GeoPipe defined in SpatialFirst class
   */
  public static void rangeQueryCompare() {
    String layerName = dataset;
    // String entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt",
    // dataset);
    String queryRectanglePath = String
        .format("D:\\Ubuntu_shared\\GeoMinHop\\query\\spa_predicate\\%s\\queryrect_1.txt", dataset);

    // String entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt",
    // dataset);
    // String dbPath =
    // "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-3.1.1/data/databases/graph.db";
    // String queryRectanglePath =
    // String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_1.txt",
    // dataset);
    try {
      Util.println(db_path);
      GraphDatabaseService databaseService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      SpatialDatabaseService spatialDatabaseService = new SpatialDatabaseService(databaseService);
      Layer layer = spatialDatabaseService.getLayer(layerName);

      ArrayList<MyRectangle> queryRectangles = OwnMethods.ReadQueryRectangle(queryRectanglePath);
      long timeGeoPipe = 0, timeMyMethod = 0, countGeoPipe = 0, countMyMethod = 0;

      long start = System.currentTimeMillis();
      Transaction tx = databaseService.beginTx();
      for (MyRectangle queryRectangle : queryRectangles) {
        Envelope envelope = new Envelope(queryRectangle.min_x, queryRectangle.max_x,
            queryRectangle.min_y, queryRectangle.max_y);
        List<SpatialDatabaseRecord> results = OSM_Utility.RangeQuery(layer, envelope);
        for (SpatialDatabaseRecord record : results) {
          record.getGeomNode().getId();
        }
        countGeoPipe += results.size();
      }
      tx.success();
      tx.close();
      timeGeoPipe += System.currentTimeMillis() - start;

      start = System.currentTimeMillis();
      tx = databaseService.beginTx();
      Node root = RTreeUtility.getRTreeRoot(databaseService, layerName);
      for (MyRectangle queryRectangle : queryRectangles) {
        LinkedList<Node> results = SpatialFirst.rangeQuery(root, queryRectangle);
        for (Node node : results)
          node.getId();
        countMyMethod += results.size();
      }
      tx.success();
      tx.close();
      timeMyMethod += System.currentTimeMillis() - start;

      databaseService.shutdown();

      Util.println(String.format("GeoPile count:%d", countGeoPipe));
      Util.println(String.format("GeoPipe Time:%d", timeGeoPipe));
      Util.println(String.format("MyMethod count:%d", countMyMethod));
      Util.println(String.format("MyMethod Time:%d", timeMyMethod));
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }


  public static void batchRTreeInsert() {
    try {
      String layerName = "test";
      String entityPath = "D:\\Ubuntu_shared\\GeoMinHop\\data\\Gowalla\\entity.txt";
      String dbPath = "D:\\Ubuntu_shared\\neo4j-community-3.1.1\\data\\databases\\graph.db";
      GraphDatabaseService databaseService =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbPath));

      SpatialDatabaseService spatialDatabaseService = new SpatialDatabaseService(databaseService);

      Transaction tx = databaseService.beginTx();
      // SimplePointLayer simplePointLayer =
      // spatialDatabaseService.createSimplePointLayer(layerName);
      // EditableLayer layer = spatialDatabaseService.getOrCreatePointLayer(layerName, "lon",
      // "lat");

      org.neo4j.gis.spatial.Layer layer = spatialDatabaseService.getLayer(layerName);

      ArrayList<Entity> entities = OwnMethods.ReadEntity(entityPath);
      ArrayList<Node> geomNodes = new ArrayList<Node>(entities.size());
      for (Entity entity : entities) {
        if (entity.IsSpatial) {
          Node node = databaseService.createNode(GraphLabel.GRAPH_1);
          node.setProperty(lon_name, entity.lon);
          node.setProperty(lat_name, entity.lat);
          geomNodes.add(node);
        }
      }

      // Node node = databaseService.createNode();
      // node.setProperty(lon_name, 22); node.setProperty(lat_name, 33);
      // geomNodes.add(node);
      //
      // node = databaseService.createNode();
      // node.setProperty(lon_name, 23); node.setProperty(lat_name, 34);
      // geomNodes.add(node);

      layer.addAll(geomNodes);

      // OwnMethods.Print(OSM_Utility.getRTreeRoot(databaseService, layerName));

      // RTreeIndex rtree = new RTreeIndex(
      // databaseService,
      // OSM_Utility.getRTreeRoot(databaseService, "test"),
      // new SimplePointEncoder()
      // );

      // ResourceIterable<Node> nodes = databaseService.getAllNodes();
      // for ( Node node1 : nodes)
      // {
      // OwnMethods.Print(node1);
      // OwnMethods.Print(String.format("node label:%s", node1.getLabels().toString()));
      // OwnMethods.Print(String.format("node property:%s", node1.getAllProperties()));
      //
      // Iterable<Relationship> rels = node1.getRelationships(Direction.OUTGOING);
      // for ( Relationship relationship : rels)
      // {
      // OwnMethods.Print(String.format("%s, type:%s, property:%s", relationship,
      // relationship.getType() ,relationship.getAllProperties()));
      // }
      // OwnMethods.Print("\n");
      // }

      tx.success();
      tx.close();
      spatialDatabaseService.getDatabase().shutdown();

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static void generateLabelList() {
    String dataset = "foursquare";
    String entityPath =
        String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
    String labelListPath =
        String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\label.txt", dataset);

    OwnMethods.getLabelListFromEntity(entityPath, labelListPath);
  }

  public static void getAllLabels() {
    // db_path = "D:\\Ubuntu_shared\\GeoMinHop\\data\\foursquare\\"
    // + "neo4j-community-3.1.1_foursquare_backup\\data\\databases\\graph.db";;
    Util.println("db path:" + db_path);
    GraphDatabaseService databaseService =
        new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
    Transaction tx = databaseService.beginTx();
    // Node node = databaseService.getNodeById(3743197);
    // Node node = databaseService.getNodeById(766033);
    // OwnMethods.Print(node.getAllProperties());
    ResourceIterable<Label> labels = databaseService.getAllLabels();
    for (Label label : labels)
      Util.println(label);

    ResourceIterable<RelationshipType> relationTypes = databaseService.getAllRelationshipTypes();
    for (RelationshipType type : relationTypes)
      Util.println(type);

    tx.success();
    tx.close();
    databaseService.shutdown();
  }

  public static void Naive() {
    String query = "";
    query += String.format(
        "profile match (a0:GRAPH_0),(a1:GRAPH_1),(a2:GRAPH_0),(a3:GRAPH_1),(a0)-[:GRAPH_LINK]-(a1),(a0)-[:GRAPH_LINK]-(a2),(a2)-[:GRAPH_LINK]-(a3) ")
        + String.format(" where %f <= a%d.%s <= %f ", queryRectangle.min_x, 1, lon_name,
            queryRectangle.max_x)
        + String.format("and %f <= a%d.%s <= %f", queryRectangle.min_y, 1, lat_name,
            queryRectangle.max_y);
    query += " return id(a0), id(a1), id(a2), id(a3)";
    Util.println(query);
    GraphDatabaseService databaseService =
        new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
    try {
      Transaction tx = databaseService.beginTx();
      Result result = databaseService.execute(query);
      while (result.hasNext())
        result.next();

      ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
      Util.println(planDescription.getProfilerStatistics().getRows());
      tx.close();
      tx.success();
      databaseService.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
