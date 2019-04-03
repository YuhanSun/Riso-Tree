package osm;

import java.io.File;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.index.LayerIndexReader;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.procedures.SpatialProcedures;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import commons.Util;

/**
 * load spatial entity into osm layer and build the R-tree index not used any more
 * 
 * @author yuhansun
 *
 */
public class LoadData {

  static String dataset = "foursquare";
  static String osm_filepath = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/entity.osm";
  // static String osm_filepath = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/test/map2.osm";
  static String layer_name = dataset;
  static String db_path = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-3.1.1_"
      + dataset + "/data/databases/graph.db";

  public static void main(String[] args) {
    LoadTest();
    // BuildIndex();
    // QueryTest();
  }

  public static void QueryTest() {
    try {
      GraphDatabaseService database =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      Transaction tx = database.beginTx();
      SpatialDatabaseService spatialService = new SpatialDatabaseService(database);
      Layer layer = spatialService.getLayer(layer_name);
      // SpatialIndexReader spatialIndex = layer.getIndex();

      LayerIndexReader spatialIndex = layer.getIndex();
      System.out.println(
          "Have " + spatialIndex.count() + " geometries in " + spatialIndex.getBoundingBox());

      // Envelope bbox = new Envelope(-80.133549, -80.098067, 26.169624, 26.205106);
      // Envelope bbox = new Envelope(12.90, 13.0064074, 55.59, 55.60);
      Envelope bbox = new Envelope(-80.133549, -80.098067, 26.169624, 26.205106);
      List<SpatialDatabaseRecord> results =
          GeoPipeline.startIntersectWindowSearch(layer, bbox).toSpatialDatabaseRecordList();

      int i = 0;
      for (SpatialDatabaseRecord record : results) {
        Geometry geometry = record.getGeometry();
        Node node = record.getGeomNode();
        System.out.println(geometry.toString());
        System.out.println(node.getAllProperties());
        Util.println(node.getId());
        i++;
      }
      tx.success();
      Util.println("count: " + i);
      // SearchFilter searchQuery = new SearchIntersectWindow(layer, new Envelope(0, 0, 1, 1));
      // spatialIndex.searchIndex(searchQuery);
      // SearchResults results = searchQuery.getResults();

      database.shutdown();
    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
    }
  }

  public static void LoadTest() {
    try {
      long start = System.currentTimeMillis();

      OSMImporter importer = new OSMImporter(layer_name);
      importer.setCharset(Charset.forName("UTF-8"));
      Map<String, String> config = new HashMap<String, String>();
      config.put("dbms.pagecache.memory", "6g");


      BatchInserter batchInserter =
          BatchInserters.inserter(new File(db_path).getAbsoluteFile(), config);
      importer.importFile(batchInserter, osm_filepath, false);
      batchInserter.shutdown();

      GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Procedures.class)
          .registerProcedure(SpatialProcedures.class);

      importer.reIndex(db, 100000);
      db.shutdown();

      System.out.println("Total Time: " + (System.currentTimeMillis() - start));
      // GraphDatabaseService graph = new GraphDatabaseFactory().newEmbeddedDatabase(new
      // File(db_path));
      // Transaction tx = graph.beginTx();
      // SpatialDatabaseService db = new SpatialDatabaseService(graph);
      // SimplePointLayer layer = (SimplePointLayer) db.getLayer("neo-text");
      //
      // // Add locations
      // Coordinate coordinate = new Coordinate(30, 30);
      // layer.add(coordinate);
      // Iterator<Geometry> iterator = (Iterator<Geometry>) layer.getAllGeometries();
      // while(iterator.hasNext())
      // System.out.println(iterator.next().toString());
      //
      // tx.success();
      // graph.shutdown();
    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
    }
  }

  public static void BuildIndex() {
    try {
      OSMImporter importer = new OSMImporter(layer_name);
      importer.setCharset(Charset.forName("UTF-8"));


      GraphDatabaseService graph =
          new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
      ((GraphDatabaseAPI) graph).getDependencyResolver().resolveDependency(Procedures.class)
          .registerProcedure(SpatialProcedures.class);

      importer.reIndex(graph);
      graph.shutdown();

      // Transaction tx = graph.beginTx();
    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
    }
  }


}
