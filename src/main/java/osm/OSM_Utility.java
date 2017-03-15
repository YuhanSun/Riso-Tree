package osm;

import java.io.File;

import org.neo4j.gis.spatial.osm.OSMDataset.OSMNode;
import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;

import commons.OSM.*;

public class OSM_Utility {
	

	public static Node getOSMDatasetNode(GraphDatabaseService db, String osm_name)
	{
		try(Transaction tx = db.beginTx()) 
		{
			ResourceIterator<Node> nodes = db.findNodes(OSMLabel.ReferenceNode);
			while(nodes.hasNext())
			{
				Node head = nodes.next();
				Iterable<Relationship> relationships = head.getRelationships(OSMRelation.OSM, Direction.OUTGOING);
				for ( Relationship relationship : relationships)
				{
					Node osm_node = relationship.getEndNode();
					if(osm_node.getProperty("name").equals(osm_name))
					{
						tx.success();
						return osm_node;
					}
				}
			}
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		return null;
	}
	
	public static Iterable<Node> getAllChangesetNodes(GraphDatabaseService db, Node osm_node)
	{
		TraversalDescription td = db.traversalDescription()
				.depthFirst()
				.relationships( OSMRelation.USERS, Direction.OUTGOING )
				.relationships( OSMRelation.OSM_USER, Direction.OUTGOING )
				.relationships( OSMRelation.USER, Direction.INCOMING )
				.evaluator( Evaluators.includeWhereLastRelationshipTypeIs( OSMRelation.USER ) );
		return td.traverse( osm_node ).nodes();
	}
	
	public static Iterable<Node> getAllPointNodes(GraphDatabaseService db, Node osm_node) 
	{
		TraversalDescription td = db.traversalDescription()
				.depthFirst()
				.relationships( OSMRelation.USERS, Direction.OUTGOING )
				.relationships( OSMRelation.OSM_USER, Direction.OUTGOING )
				.relationships( OSMRelation.USER, Direction.INCOMING )
				.relationships( OSMRelation.CHANGESET, Direction.INCOMING )
				.evaluator( Evaluators.includeWhereLastRelationshipTypeIs( OSMRelation.CHANGESET ) );
		return td.traverse( osm_node ).nodes();
	}
	
	static String dataset = "Gowalla";
	static String db_path = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-3.1.1_"+dataset+"/data/databases/graph.db";	
			
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
		Node osm_node = getOSMDatasetNode(databaseService, dataset);
//		System.out.println(osm_node.toString());
		
		try(Transaction tx = databaseService.beginTx()) {
//			Iterable<Node> changesetNodes = getAllChangesetNodes(databaseService, osm_node);
//			for ( Node changesetNode : changesetNodes)
//				System.out.println(changesetNode.toString());
			Iterable<Node> spatial_nodes = getAllPointNodes(databaseService, osm_node);
			int i = 0;
			for (Node node : spatial_nodes)
				i++;
			System.out.println(i);
			
			tx.success();
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		databaseService.shutdown();
	}

}
