package osm;

import java.io.File;
import java.util.List;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import com.vividsolutions.jts.geom.Envelope;
import commons.Labels.OSMLabel;
import commons.Labels.OSMRelation;
import commons.Util;


public class OSM_Utility {

  /**
   * (osm_root:OSMLabel.ReferenceNode)-[:OSM]->(osm_node{name:osm_name})
   * 
   * @param db
   * @param osm_name
   * @return osm_node with given osm_name
   */
  public static Node getOSMDatasetNode(GraphDatabaseService db, String osm_name) {
    Transaction tx = db.beginTx();
    try {
      ResourceIterator<Node> nodes = db.findNodes(OSMLabel.ReferenceNode);
      while (nodes.hasNext()) {
        Node head = nodes.next();
        Iterable<Relationship> relationships =
            head.getRelationships(OSMRelation.OSM, Direction.OUTGOING);
        for (Relationship relationship : relationships) {
          Node osm_node = relationship.getEndNode();
          if (osm_node.getProperty("name").equals(osm_name)) {
            tx.success();
            return osm_node;
          }
        }
      }

    } catch (Exception e) {
      // TODO: handle exception
    }
    Util.println("getOSMDatasetNode return null!");
    return null;
  }

  /**
   * (osm_node)-[:USERS]->()-[:OSM_USER]->()<-[:USER]-(changeset)
   * 
   * @param db
   * @param osm_node
   * @return all changeset nodes
   */
  public static Iterable<Node> getAllChangesetNodes(GraphDatabaseService db, Node osm_node) {
    TraversalDescription td =
        db.traversalDescription().depthFirst().relationships(OSMRelation.USERS, Direction.OUTGOING)
            .relationships(OSMRelation.OSM_USER, Direction.OUTGOING)
            .relationships(OSMRelation.USER, Direction.INCOMING)
            .evaluator(Evaluators.includeWhereLastRelationshipTypeIs(OSMRelation.USER));
    return td.traverse(osm_node).nodes();
  }

  /**
   * (osm_node)-[:USERS]->()-[:OSM_USER]->()<-[:USER]-(changeset)<-[:CHANGESET]-(osm_object)
   * 
   * @param db
   * @param osm_node
   * @return all osm_objects that belong to the osm_node
   */
  public static Iterable<Node> getAllPointNodes(GraphDatabaseService db, Node osm_node) {
    TraversalDescription td =
        db.traversalDescription().depthFirst().relationships(OSMRelation.USERS, Direction.OUTGOING)
            .relationships(OSMRelation.OSM_USER, Direction.OUTGOING)
            .relationships(OSMRelation.USER, Direction.INCOMING)
            .relationships(OSMRelation.CHANGESET, Direction.INCOMING)
            .evaluator(Evaluators.includeWhereLastRelationshipTypeIs(OSMRelation.CHANGESET));
    return td.traverse(osm_node).nodes();
  }

  /**
   * spatial range query
   * 
   * @param layer layer to be queried
   * @param bbox input spatial range
   * @return all the objects within bbox
   */
  public static List<SpatialDatabaseRecord> RangeQuery(Layer layer, Envelope bbox) {
    List<SpatialDatabaseRecord> results =
        GeoPipeline.startIntersectWindowSearch(layer, bbox).toSpatialDatabaseRecordList();
    return results;
  }

  static String dataset_test = "Gowalla";
  static String db_path = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-3.1.1_"
      + dataset_test + "/data/databases/graph.db";

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    GraphDatabaseService databaseService =
        new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
    // Node osm_node = getOSMDatasetNode(databaseService, dataset);
    // System.out.println(osm_node.toString());
    Transaction tx = databaseService.beginTx();
    try {
      /**
       * test for get changesetnode
       */
      // Iterable<Node> changesetNodes = getAllChangesetNodes(databaseService, osm_node);
      // for ( Node changesetNode : changesetNodes)
      // System.out.println(changesetNode.toString());

      /**
       * test for get spatial points
       */
      // Iterable<Node> spatial_nodes = getAllPointNodes(databaseService, osm_node);
      // int i = 0;
      // for (Node node : spatial_nodes)
      // i++;
      // System.out.println(i);

      /**
       * test for range query
       */
      SpatialDatabaseService spa_service = new SpatialDatabaseService(databaseService);
      Layer layer = spa_service.getLayer(dataset_test);
      List<SpatialDatabaseRecord> results =
          RangeQuery(layer, new Envelope(-80.133549, -80.098067, 26.169624, 26.205106));
      for (SpatialDatabaseRecord record : results) {
        // OwnMethods.Print(record.getNodeId());
        // OwnMethods.Print(record.getGeomNode().getId());
        // OwnMethods.Print(record.getGeomNode().getSingleRelationship(OSMRelation.GEOM,
        // Direction.INCOMING)
        // .getStartNode().getId());
        Util.println(record.getNodeId());
        Util.println(record.getGeomNode().getAllProperties());
      }
      Util.println(results.size());


      /**
       * test for rtree
       */
      // Node rtree_root_node = getRTreeRoot(databaseService, dataset);
      // OwnMethods.Print(rtree_root_node.getId());

      // Iterable<Node> geometry_nodes = getAllGeometries(databaseService, dataset);
      // int i = 0;
      // for (Node node : geometry_nodes)
      // {
      // i++;
      // OwnMethods.Print(node.getAllProperties());
      // }
      // OwnMethods.Print(i);


      tx.success();

    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
    }
    databaseService.shutdown();
  }

}
