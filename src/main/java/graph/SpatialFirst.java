package graph;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.geotools.data.shapefile.dbf.DbaseFileReader.Row;
import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import osm.OSM_Utility;
import commons.Config;
import commons.Labels.OSMRelation;
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

	//query statistics
	public long range_query_time;
	public long get_iterator_time;
	public long iterate_time;
	public long result_count;
	public long page_hit_count;

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
		query += String.format(" and id(a%d) in [%d]", pos, id);
		//return
		query += " return id(a0)";
		for(int i = 1; i<query_Graph.graph.size(); i++)
			query += String.format(",id(a%d)", i);

		if(limit != -1)
			query += String.format(" limit %d", limit);

		return query;
	}

	public void query(Query_Graph query_Graph, int limit)
	{
		try {
			range_query_time = 0;
			get_iterator_time = 0;
			iterate_time = 0;
			result_count = 0;
			page_hit_count = 0;
			
			long start = System.currentTimeMillis();
			Transaction tx = dbservice.beginTx();

			//<spa_id, rectangle>
			HashMap<Integer, MyRectangle> spa_predicates = new HashMap<>();
			for (int i = 0; i < query_Graph.Has_Spa_Predicate.length; i++)
				if(query_Graph.Has_Spa_Predicate[i])
					spa_predicates.put(i, query_Graph.spa_predicate[i]);

			int min_pos = 0;
			MyRectangle min_queryRectangle = null;
			for ( int key : spa_predicates.keySet())
				if(min_queryRectangle == null)
				{
					min_pos = key;
					min_queryRectangle = spa_predicates.get(key);
				}
				else {
					if ( spa_predicates.get(key).area() < min_queryRectangle.area())
					{
						min_pos = key;
						min_queryRectangle = spa_predicates.get(key);
					}
				}
			spa_predicates.remove(min_pos, min_queryRectangle);

			long start_1 = System.currentTimeMillis();
			Node rootNode = OSM_Utility.getRTreeRoot(dbservice, dataset);
			LinkedList<Node> rangeQueryResult = this.rangeQuery(rootNode, min_queryRectangle);
			range_query_time = System.currentTimeMillis() - start_1;

			for (Node geom: rangeQueryResult)
			{
				start_1 = System.currentTimeMillis();
				Node node = geom.getSingleRelationship(OSMRelation.GEOM, Direction.INCOMING).getStartNode();
				long id = node.getId();
				String query = formSubgraphQuery(query_Graph, limit, 1, spa_predicates, min_pos, id);
				
				Result result = dbservice.execute(query);
				get_iterator_time += System.currentTimeMillis() - start_1;
				
				start_1 = System.currentTimeMillis();
				while( result.hasNext())
				{
					result.next();
					//					Map<String, Object> row = result.next();
					//					String str = row.toString();
					//					OwnMethods.Print(row.toString());
				}
				iterate_time += System.currentTimeMillis() - start_1;
				
				ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
				ExecutionPlanDescription.ProfilerStatistics profile = planDescription.getProfilerStatistics();
				result_count += profile.getRows();
				page_hit_count += OwnMethods.GetTotalDBHits(planDescription);
			}
			tx.success();
			tx.close();
//			OwnMethods.Print(String.format("result size: %d", result_count));
//			OwnMethods.Print(String.format("time: %d", System.currentTimeMillis() - start));
			//			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		//		return null;
	}

	public static void rangeQueryTest()
	{
		try	{
			SpatialFirst spatialFirst = new SpatialFirst(db_path_test, dataset_test);
			Transaction tx = spatialFirst.dbservice.beginTx();
			Node rootNode = OSM_Utility.getRTreeRoot(spatialFirst.dbservice, dataset_test);
			MyRectangle query_rectangle = new MyRectangle(-98.025157, 29.953977, -97.641747, 30.337387);
			LinkedList<Node> result = spatialFirst.rangeQuery(rootNode, query_rectangle);
			OwnMethods.Print(String.format("Result size: %d", result.size()));
			tx.success();
			tx.close();
			spatialFirst.dbservice.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void formSubgraphQueryTest()
	{
		try {
			SpatialFirst spatialFirst = new SpatialFirst(db_path_test, dataset_test);
			query_Graph.spa_predicate[1] = queryRectangle;

			HashMap<Integer, MyRectangle> spa_predicates = new HashMap<Integer, MyRectangle>();
			int pos = 1;
			long id = 100;
			String query = spatialFirst.formSubgraphQuery(query_Graph, -1, 0, spa_predicates, pos, id);
			OwnMethods.Print(query);
			spatialFirst.dbservice.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void subgraphMatchQueryTest()
	{
		SpatialFirst spatialFirst = new SpatialFirst(db_path_test, dataset_test);
		query_Graph.spa_predicate[1] = queryRectangle;

		spatialFirst.query(query_Graph, -1);
		spatialFirst.shutdown();
	}

	static String dataset_test = "Gowalla";
	static String db_path_test = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\neo4j-community-3.1.1_%s\\data\\databases\\graph.db", dataset_test, dataset_test);
	static String querygraph_path = "D:\\Ubuntu_shared\\GeoMinHop\\query\\query_graph.txt";
	static int query_id = 5;
	static ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
	static Query_Graph query_Graph = queryGraphs.get(query_id);
	static MyRectangle queryRectangle = new MyRectangle(-80.119514, 26.183659, -80.112102, 26.191071);//10
	//	static MyRectangle queryRectangle = new MyRectangle(-84.468680, 33.879658, -84.428434, 33.919904);//100
	//	static MyRectangle queryRectangle = new MyRectangle(-80.200353, 26.102820, -80.031263, 26.271910);//1000
	//	static MyRectangle queryRectangle = new MyRectangle(-98.025157, 29.953977, -97.641747, 30.337387);//10000
	//	static MyRectangle queryRectangle = new MyRectangle(-91.713778, 14.589395, -68.517838, 37.785335);//100000

	public static void main(String[] args) {
		//		rangeQueryTest();
		//		formSubgraphQueryTest();
//		subgraphMatchQueryTest();
	}

}
