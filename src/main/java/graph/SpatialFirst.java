package graph;

import java.awt.SystemColor;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.LayerIndexReader;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import osm.OSM_Utility;
import commons.Config;
import commons.MyRectangle;
import commons.OwnMethods;
import commons.Query_Graph;
import commons.Utility;

public class SpatialFirst {

	public GraphDatabaseService dbservice;
	public String dataset;
	
	public Config config = new Config();
	public String lon_name = config.GetLongitudePropertyName();
	public String lat_name = config.GetLatitudePropertyName();
	
	public SpatialFirst(String db_path, String p_dataset)
	{
		dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
		dataset = p_dataset;
	}
	
	public void shutdown()
	{
		dbservice.shutdown();
	}
	
	public LinkedList<Node> rangeQuery(Node root_node, MyRectangle query_rectangle)
	{
		try {
			LinkedList<Node> cur_list = new LinkedList<>();
			cur_list.add(root_node);
			
			int level_index = 0;
			while(cur_list.isEmpty() == false)
			{
				long start = System.currentTimeMillis();
//				OwnMethods.Print(String.format("level %d", level_index));
				LinkedList<Node> next_list = new LinkedList<>();
				LinkedList<Node> overlap_MBR_list = new LinkedList<Node>(); 
				
				for (Node node : cur_list)
				{
					if(node.hasProperty("bbox"))
					{
						double[] bbox = (double[]) node.getProperty("bbox");
						MyRectangle MBR = new MyRectangle(bbox[0], bbox[1], bbox[2], bbox[3]);
						if(query_rectangle.intersect(MBR) != null)
						{
							overlap_MBR_list.add(node);
							
							Iterable<Relationship> rels = node.getRelationships(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING);
							for ( Relationship relationship : rels)
								next_list.add(relationship.getEndNode());
						}
					}
					else
						throw new Exception(String.format("node %d does not has \"bbox\" property", node));
				}
				
				OwnMethods.Print(String.format("level %d time: %d", level_index, System.currentTimeMillis() - start));
				
				int located_in_count = 0;
				if( overlap_MBR_list.isEmpty() == false && next_list.isEmpty())
				{
					LinkedList<Node> result = new LinkedList<Node>(); 
					start = System.currentTimeMillis();
					for ( Node node : overlap_MBR_list)
						for ( Relationship relationship : node.getRelationships(
								Direction.OUTGOING, RTreeRelationshipTypes.RTREE_REFERENCE))
						{
							Node geom = relationship.getEndNode();
							double[] bbox = (double[]) geom.getProperty("bbox");
							MyRectangle bbox_rect = new MyRectangle(bbox);
								if ( query_rectangle.intersect(bbox_rect) != null)
								{
									result.add(geom);
									located_in_count++;
								}
						}
					level_index++;
					OwnMethods.Print(String.format("level %d time: %d", level_index, System.currentTimeMillis() - start));
					OwnMethods.Print(String.format("Located in nodes: %d", located_in_count));
					return result;
				}
				
				cur_list = next_list;
				level_index++;
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return null;
	}
	
	public String formSubgraphQuery(Query_Graph query_Graph, int limit, int Explain_Or_Profile, HashMap<Integer, MyRectangle> spa_predicates, int pos, long id)
	{
		String query = "";
		if(Explain_Or_Profile == 1)
			query += "profile match ";
		else if (Explain_Or_Profile == -1) {
			query += "explain match ";
		}
		else {
			query += "match ";
		}

		//label
		query += String.format("(a0:GRAPH_%d)", query_Graph.label_list[0]);
		for(int i = 1; i < query_Graph.graph.size(); i++)
		{
			query += String.format(",(a%d:GRAPH_%d)",i, query_Graph.label_list[i]);
		}

		//edge
		for(int i = 0; i<query_Graph.graph.size(); i++)
		{
			for(int j = 0;j<query_Graph.graph.get(i).size();j++)
			{
				int neighbor = query_Graph.graph.get(i).get(j);
				if(neighbor > i)
					query += String.format(",(a%d)--(a%d)", i, neighbor);
			}
		}

		query += " where";

		//spatial predicate
		for ( int key : spa_predicates.keySet())
		{
			MyRectangle qRect = spa_predicates.get(key);
			query += String.format(" %f <= a%d.%s <= %f ", qRect.min_x, key, lon_name, qRect.max_x);
			query += String.format("and %f <= a%d.%s <= %f", qRect.min_y, key, lat_name, qRect.max_y);
		} 
		
		//id
		query += String.format(" id(a%d) = %d", pos, id);
		//return
		query += " return a0";
		for(int i = 1; i<query_Graph.graph.size(); i++)
			query += String.format(",a%d", i);
		
		if(limit != -1)
			query += String.format(" limit %d", limit);
		
		return query;
	}
	
	public Result query(Query_Graph query_Graph)
	{
		try {
			long start = System.currentTimeMillis();
			Transaction tx = dbservice.beginTx();

			//<spa_id, rectangle>
			HashMap<Integer, MyRectangle> spa_predicates = new HashMap<>();
			for (int i = 0; i < query_Graph.Has_Spa_Predicate.length; i++)
				if(query_Graph.Has_Spa_Predicate[i])
					spa_predicates.put(i, query_Graph.spa_predicate[i]);
			
			int min_pos = 0;
			MyRectangle queryRectangle = null;
			for ( int key : spa_predicates.keySet())
				if(queryRectangle == null)
				{
					min_pos = key;
					queryRectangle = spa_predicates.get(key);
				}
				else {
					if ( spa_predicates.get(key).area() < queryRectangle.area())
					{
						min_pos = key;
						queryRectangle = spa_predicates.get(key);
					}
				}
			
			int count = 0;
			Node rootNode = OSM_Utility.getRTreeRoot(dbservice, dataset);
			LinkedList<Node> rangeQueryResult = this.rangeQuery(rootNode, queryRectangle);
			
			for (Node node: rangeQueryResult)
			{
				count++;
				long id = node.getId();
				String query = formSubgraphQuery(query_Graph, -1, 0, spa_predicates, min_pos, id);
				Result result = dbservice.execute(query);
			}
			tx.success();
			tx.close();
//			OwnMethods.Print(String.format("result size: %d", count));
			OwnMethods.Print(String.format("time: %d", System.currentTimeMillis() - start));
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	public static void rangeQueryTest()
	{
		String dataset = "Gowalla";
		String db_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\neo4j-community-3.1.1_%s\\data\\databases\\graph.db", dataset, dataset);
		try {
			SpatialFirst spatialFirst = new SpatialFirst(db_path, dataset);
			Transaction tx = spatialFirst.dbservice.beginTx();
			Node rootNode = OSM_Utility.getRTreeRoot(spatialFirst.dbservice, dataset);
			MyRectangle query_rectangle = new MyRectangle(-98.025157, 29.953977, -97.641747, 30.337387);
			LinkedList<Node> result = spatialFirst.rangeQuery(rootNode, query_rectangle);
			OwnMethods.Print(String.format("Result size: %d", result.size()));
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	public static void subgraphMatchQueryTest()
	{
		String dataset = "Gowalla";
		String db_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\neo4j-community-3.1.1_%s\\data\\databases\\graph.db", dataset, dataset);
		String querygraph_path = "D:\\Ubuntu_shared\\GeoMinHop\\query\\query_graph.txt";
		int query_id = 5;
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
		Query_Graph query_Graph = queryGraphs.get(query_id);
		SpatialFirst spatialFirst = new SpatialFirst(db_path, dataset);
		spatialFirst.query(query_Graph);
		spatialFirst.shutdown();
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		rangeQueryTest();
	}

}
