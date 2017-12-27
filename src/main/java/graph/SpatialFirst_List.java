package graph;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

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
import commons.Config.*;
import commons.Labels;
import commons.MyPoint;
import commons.Labels.OSMRelation;
import knn.Element;
import knn.KNNComparator;
import commons.MyRectangle;
import commons.OwnMethods;
import commons.Query_Graph;
import commons.Utility;

/**
 * Implements SpatialFirst with NL_list.
 * Two versions are implemented, simple version and blocked version.
 * Simple version proves to be same with SpatialFirst class approach.
 * Blocked version is better than SpatialFirst approach. It is used in
 * our experiment.
 * @author yuhansun
 *
 */
public class SpatialFirst_List {

	public GraphDatabaseService dbservice;
	public String dataset;
	public long[] graph_pos_map_list;

	public Config config = new Config();
	public String lon_name = config.GetLongitudePropertyName();
	public String lat_name = config.GetLatitudePropertyName();
	public int MAX_HOPNUM = config.getMaxHopNum();
	public String graphLinkLabelName = Labels.GraphRel.GRAPH_LINK.name();
	public system systemName = config.getSystemName();

	//query statistics
	public long range_query_time;
	public long get_iterator_time;
	public long iterate_time;
	public long result_count;
	public long page_hit_count;
	
	public int visit_spatial_object_count;

	/**
	 * 
	 * @param db_path	database location
	 * @param p_dataset	used for osm and rtree entry
	 * @param p_graph_pos_map	map from graph id to neo4j pos id
	 */
	public SpatialFirst_List(String db_path, String p_dataset, long[] p_graph_pos_map)
	{
		dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
		dataset = p_dataset;
		graph_pos_map_list = p_graph_pos_map;
	}

	public void shutdown()
	{
		dbservice.shutdown();
	}
	
	public static int[][] Ini_Minhop(Query_Graph query_Graph)
	{
		int query_node_count = query_Graph.graph.size();
		int [][] minhop_index = new int[query_node_count][];
		
		for ( int i = 0; i < query_node_count; i++)
		{
			if ( query_Graph.spa_predicate[i] == null)
				minhop_index[i] = null;
			else
				minhop_index[i] = new int[query_node_count];
		}
		
		for ( int i = 0; i < query_node_count; i++)
		{
			if(query_Graph.spa_predicate[i] != null)
			{
				boolean[] visited = new boolean[query_node_count];
				visited[i] = true;
				minhop_index[i][i] = -1;
				
				Queue<Integer> queue = new LinkedList<Integer>();
				queue.add(i);
				int pre_level_count = 1;
				int cur_level_count = 0;
				int level_index = 1;
				
				while ( queue.isEmpty() == false )
				{
					for ( int j = 0; j < pre_level_count; j++)
					{
						int node = queue.poll();
						for ( int k = 0; k < query_Graph.graph.get(node).size(); k++)
						{
							int neighbor = query_Graph.graph.get(node).get(k);
							if(visited[neighbor] == false)
							{
								minhop_index[i][neighbor] = level_index;
								visited[neighbor] = true;
								cur_level_count += 1;
								queue.add(neighbor);
							}
						}
					}
					level_index ++;
					pre_level_count = cur_level_count;
					cur_level_count = 0;
				}
			}
		}
		
//		minhop_index[2][0] = -1;	minhop_index[2][3] = -1;
		return minhop_index;
	}

	/**
	 * only returns the deepest non-leaf rtree nodes
	 * @param root_node
	 * @param query_rectangle
	 * @return
	 */
	public LinkedList<Node> rangeQuery(Node root_node, MyRectangle query_rectangle)
	{
		try {
			LinkedList<Node> cur_list = new LinkedList<Node>();
			cur_list.add(root_node);

			int level_index = 0;
			while(cur_list.isEmpty() == false)
			{
				long start = System.currentTimeMillis();
				//				OwnMethods.Print(String.format("level %d", level_index));
				LinkedList<Node> next_list = new LinkedList<Node>();
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

//				OwnMethods.Print(String.format("level %d time: %d", level_index, System.currentTimeMillis() - start));

				if ( next_list.isEmpty())
					return overlap_MBR_list;

				cur_list = next_list;
				level_index++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * form the cypher query
	 * @param query_Graph
	 * @param limit	-1 is no limit
	 * @param Explain_Or_Profile
	 * @param spa_predicates	spatial predicates except for the min_pos spatial predicate 
	 * @param pos	query graph node id with the trigger spatial predicate
	 * @param id	corresponding spatial graph node id (neo4j pos id)
	 * @param NL_hopnum	shrunk query node <query_graph_id, hop_num> 
	 * @param node	the rtree node stores NL_list information
	 * @return
	 */
	public String formSubgraphQuery(Query_Graph query_Graph, int limit, Explain_Or_Profile explain_Or_Profile,
			HashMap<Integer, MyRectangle> spa_predicates, int pos, long id, 
			HashMap<Integer, Integer> NL_hopnum, Node node)
	{
		String query = "";
		switch (explain_Or_Profile) {
		case Profile:
			query += "profile match ";
			break;
		case Explain:
			query += "explain match ";
			break;			
		case Nothing:
			query += "match ";
			break;
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
					query += String.format(",(a%d)-[:%s]-(a%d)", i, graphLinkLabelName, neighbor);
			}
		}

		query += " \n where ";

		//spatial predicate
		for ( int key : spa_predicates.keySet())
		{
			MyRectangle qRect = spa_predicates.get(key);
			query += String.format(" %f <= a%d.%s <= %f ", qRect.min_x, key, lon_name, qRect.max_x);
			query += String.format("and %f <= a%d.%s <= %f and", qRect.min_y, key, lat_name, qRect.max_y);
		} 

		//id
//		query += String.format(" id(a%d) in [%d]", pos, id);
		query += String.format(" \nid(a%d) = %d\n", pos, id);
		
		//NL_id_list
		for ( int key : NL_hopnum.keySet())
		{
			String id_list_property_name = String.format("NL_%d_%d_list", NL_hopnum.get(key), query_Graph.label_list[key]);
			
			if ( node.hasProperty(id_list_property_name) == false)
			{
				query = "match (n) where false return n";
				return query;
			}
			
			int[] graph_id_list = (int[]) node.getProperty(id_list_property_name);
			ArrayList<Long> pos_id_list = new ArrayList<Long>(graph_id_list.length);
			for ( int i = 0; i < graph_id_list.length; i++)
				pos_id_list.add(graph_pos_map_list[graph_id_list[i]]);
			
			query += String.format(" and ( id(a%d) = %d", key, pos_id_list.get(0));
			for ( int i = 1; i < pos_id_list.size(); i++)
				query += String.format(" or id(a%d) = %d", key, pos_id_list.get(i));
			query += " )\n";
//			query += String.format(" and id(a%d) in %s", key, pos_id_list.toString());
		}
			
		//return
		query += " return id(a0)";
		for(int i = 1; i<query_Graph.graph.size(); i++)
			query += String.format(",id(a%d)", i);

		if(limit != -1)
			query += String.format(" limit %d", limit);

		return query;
	}
	
	/**
	 * for each spatial vertex run a cypher query using NL_list
	 * @param query_Graph
	 * @param limit
	 */
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

			int[][] min_hop = Ini_Minhop(query_Graph);
			
			//<spa_id, rectangle>
			HashMap<Integer, MyRectangle> spa_predicates = new HashMap<Integer, MyRectangle>();
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
			
			//query vertex to be shrunk <id, hop_num>
			//calculated from the min_pos
			HashMap<Integer, Integer> NL_hopnum = new HashMap<Integer, Integer>();
			for (int i = 0; i < query_Graph.graph.size(); i++)
				if ( min_hop[min_pos][i] <= MAX_HOPNUM && min_hop[min_pos][i] > 0)
					NL_hopnum.put(i, min_hop[min_pos][i]);

			long start_1 = System.currentTimeMillis();
			Node rootNode = OSM_Utility.getRTreeRoot(dbservice, dataset);
			LinkedList<Node> rangeQueryResult = this.rangeQuery(rootNode, min_queryRectangle);
			range_query_time = System.currentTimeMillis() - start_1;
			
			int located_in_count = 0;
			for ( Node rtree_node : rangeQueryResult)
			{
				start_1 = System.currentTimeMillis();
				Iterable<Relationship> rels = rtree_node.getRelationships(
						Direction.OUTGOING, RTreeRelationshipTypes.RTREE_REFERENCE);
				range_query_time += System.currentTimeMillis() - start_1;
				
				for ( Relationship relationship : rels)
				{
					start_1 = System.currentTimeMillis();
					Node geom = relationship.getEndNode();
					double[] bbox = (double[]) geom.getProperty("bbox");
					MyRectangle bbox_rect = new MyRectangle(bbox);
					if ( min_queryRectangle.intersect(bbox_rect) != null)
					{
						located_in_count++;
//						Node node = geom.getSingleRelationship(OSMRelation.GEOM, Direction.INCOMING).getStartNode();
//						long id = node.getId();
						long id = geom.getId();
						range_query_time += System.currentTimeMillis() - start_1;
						
						start_1 = System.currentTimeMillis();
						String query = formSubgraphQuery(query_Graph, limit, Explain_Or_Profile.Profile, 
								spa_predicates, min_pos,
								id, NL_hopnum, rtree_node);
						
						Result result = dbservice.execute(query);
						get_iterator_time += System.currentTimeMillis() - start_1;
						
						start_1 = System.currentTimeMillis();
						int cur_count = 0;
						while( result.hasNext())
						{
							cur_count++;
							result.next();
							//					Map<String, Object> row = result.next();
							//					String str = row.toString();
							//					OwnMethods.Print(row.toString());
						}
						iterate_time += System.currentTimeMillis() - start_1;
						
						if ( cur_count != 0)
						{
							ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
							ExecutionPlanDescription.ProfilerStatistics profile = planDescription.getProfilerStatistics();
							result_count += profile.getRows();
							page_hit_count += OwnMethods.GetTotalDBHits(planDescription);
//						OwnMethods.Print(planDescription);
							OwnMethods.Print(String.format("%d, %d", id, cur_count));
						}
					}
					else {
						range_query_time += System.currentTimeMillis() - start_1;
					}
				}
			}

			tx.success();
			tx.close();
			OwnMethods.Print(String.format("locate in count:%d", located_in_count));
//			OwnMethods.Print(String.format("result size: %d", result_count));
//			OwnMethods.Print(String.format("time: %d", System.currentTimeMillis() - start));
			//			return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		//		return null;
	}
	
	/**
	 * form the cypher query for MBR block
	 * @param query_Graph
	 * @param limit	-1 is no limit
	 * @param Explain_Or_Profile	-1 is Explain; 1 is Profile; the rest is nothing 
	 * @param spa_predicates	spatial predicates except the min_pos spatial predicate 
	 * @param pos	query graph node id with the trigger spatial predicate
	 * @param ids	corresponding spatial graph node ids that in this MBR (neo4j pos id)
	 * @param NL_hopnum	shrunk query node <query_graph_id, hop_num> 
	 * @param node	the rtree node stores NL_list information
	 * @return
	 */
	public String formSubgraphQuery_Block(Query_Graph query_Graph, int limit, Explain_Or_Profile explain_Or_Profile,
			HashMap<Integer, MyRectangle> spa_predicates, int pos, ArrayList<Long> ids, 
			HashMap<Integer, Integer> NL_hopnum, Node node)
	{
		String query = "";
		switch (explain_Or_Profile) {
		case Profile:
			query += "profile match ";
			break;
		case Explain:
			query += "explain match ";
			break;			
		case Nothing:
			query += "match ";
			break;
		}

		//label
		if ( pos == 0 || NL_hopnum.containsKey(0))
			query += "(a0)";
		else
			query += String.format("(a0:GRAPH_%d)", query_Graph.label_list[0]);
		for(int i = 1; i < query_Graph.graph.size(); i++)
		{
			if ( pos == i || NL_hopnum.containsKey(i))
				query += String.format(",(a%d)", i);
			else
				query += String.format(",(a%d:GRAPH_%d)",i, query_Graph.label_list[i]);
		}

		//edge
		for(int i = 0; i<query_Graph.graph.size(); i++)
		{
			for(int j = 0;j<query_Graph.graph.get(i).size();j++)
			{
				int neighbor = query_Graph.graph.get(i).get(j);
				if(neighbor > i)
					query += String.format(",(a%d)-[:%s]-(a%d)", i, graphLinkLabelName, neighbor);
			}
		}

		query += " where\n";

		//spatial predicate
		for ( int key : spa_predicates.keySet())
		{
			MyRectangle qRect = spa_predicates.get(key);
			query += String.format(" %f <= a%d.%s <= %f ", qRect.min_x, key, lon_name, qRect.max_x);
			query += String.format("and %f <= a%d.%s <= %f and", qRect.min_y, key, lat_name, qRect.max_y);
		} 
		
		query += "\n";

		//id
		query += String.format(" (id(a%d)=%d", pos, ids.get(0));
		if ( ids.size() > 1)
			for ( int i = 1; i < ids.size(); i++)
				query += String.format(" or id(a%d)=%d", pos, ids.get(i));
//		query += String.format(" id(a%d) in %s\n", pos, ids.toString());
		
		query += ")\n"; 
		OwnMethods.Print(String.format("spa_ids size: %d", ids.size()));
		
		//NL_id_list
//		for ( int key : NL_hopnum.keySet())
//		{
//			String id_list_size_property_name = String.format("NL_%d_%d_size", NL_hopnum.get(key), query_Graph.label_list[key]);
//			int id_list_size = (Integer) node.getProperty(id_list_size_property_name);
//			if( id_list_size > 0)	//whether to use the shrunk label
//				query = query.replaceFirst(String.format("a%d", key), String.format("a%d:GRAPH_%d", key, query_Graph.label_list[key]));
//			else {
//				String id_list_property_name = String.format("NL_%d_%d_list", NL_hopnum.get(key), query_Graph.label_list[key]);
//				
//				if ( node.hasProperty(id_list_property_name) == false)
//					return "match (n) where false return n";
//				
//				int[] graph_id_list = (int[]) node.getProperty(id_list_property_name);
//				ArrayList<Long> pos_id_list = new ArrayList<Long>(graph_id_list.length);
//				for ( int i = 0; i < graph_id_list.length; i++)
//					pos_id_list.add(graph_pos_map_list[graph_id_list[i]]);
//				
//				query += String.format(" and ( id(a%d) = %d", key, pos_id_list.get(0));
//				for ( int i = 1; i < pos_id_list.size(); i++)
//					query += String.format(" or id(a%d) = %d", key, pos_id_list.get(i));
//				query += " )\n";
////			query += String.format(" and id(a%d) in %s\n", key, pos_id_list.toString());
//				OwnMethods.Print(String.format("%s size is %d", id_list_property_name, pos_id_list.size()));
//			}
//		}
			
		//return
		query += " return id(a0)";
		for(int i = 1; i<query_Graph.graph.size(); i++)
			query += String.format(",id(a%d)", i);

		if(limit != -1)
			query += String.format(" limit %d", limit);

		return query;
	}
	

	/**
	 * for spatial vertices in the same MBR run a cypher query using NL_list
	 * @param query_Graph
	 * @param limit
	 */
	public void query_Block(Query_Graph query_Graph, int limit)
	{
		try {
			range_query_time = 0;
			get_iterator_time = 0;
			iterate_time = 0;
			result_count = 0;
			page_hit_count = 0;
			
			long start = System.currentTimeMillis();
			Transaction tx = dbservice.beginTx();

			int[][] min_hop = Ini_Minhop(query_Graph);
			
			//<spa_id, rectangle>
			HashMap<Integer, MyRectangle> spa_predicates = new HashMap<Integer, MyRectangle>();
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
			
			//query vertex to be shrunk <id, hop_num>
			//calculated from the min_pos
			HashMap<Integer, Integer> NL_hopnum = new HashMap<Integer, Integer>();
			for (int i = 0; i < query_Graph.graph.size(); i++)
				if ( min_hop[min_pos][i] <= MAX_HOPNUM && min_hop[min_pos][i] > 0)
					NL_hopnum.put(i, min_hop[min_pos][i]);

			long start_1 = System.currentTimeMillis();
			Node rootNode = OSM_Utility.getRTreeRoot(dbservice, dataset);
			LinkedList<Node> rangeQueryResult = this.rangeQuery(rootNode, min_queryRectangle);
			range_query_time = System.currentTimeMillis() - start_1;
			
			int located_in_count = 0;
			for ( Node rtree_node : rangeQueryResult)
			{
				start_1 = System.currentTimeMillis();
				Iterable<Relationship> rels = rtree_node.getRelationships(
						Direction.OUTGOING, RTreeRelationshipTypes.RTREE_REFERENCE);
				
				ArrayList<Long> ids = new ArrayList<Long>();
				for ( Relationship relationship : rels)
				{
					Node geom = relationship.getEndNode();
					double[] bbox = (double[]) geom.getProperty("bbox");
					MyRectangle bbox_rect = new MyRectangle(bbox);
					if ( min_queryRectangle.intersect(bbox_rect) != null)
					{
						located_in_count++;
//						Node node = geom.getSingleRelationship(OSMRelation.GEOM, Direction.INCOMING).getStartNode();
//						long id = node.getId();
						long id = geom.getId();
						ids.add(id);
					}
				}
				range_query_time += System.currentTimeMillis() - start_1;
				
				if (ids.size() > 0)
				{
					start_1 = System.currentTimeMillis();
					String query = formSubgraphQuery_Block(query_Graph, limit, Explain_Or_Profile.Profile, spa_predicates, min_pos,
							ids, NL_hopnum, rtree_node);
					OwnMethods.Print(query);

					Result result = dbservice.execute(query);
					get_iterator_time += System.currentTimeMillis() - start_1;

					start_1 = System.currentTimeMillis();
					int cur_count = 0;
					while( result.hasNext())
					{
						cur_count++;
						result.next();
//						Map<String, Object> row = result.next();
//						String str = row.toString();
//						OwnMethods.Print(row.toString());
					}
					iterate_time += System.currentTimeMillis() - start_1;

					if ( cur_count != 0)
					{
						ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
						ExecutionPlanDescription.ProfilerStatistics profile = planDescription.getProfilerStatistics();
						result_count += profile.getRows();
						page_hit_count += OwnMethods.GetTotalDBHits(planDescription);
						
//					OwnMethods.Print(planDescription);
					}
				}
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

	public static void formSubgraphQueryTest() throws Exception
	{
		try {
			HashMap<String, String> graph_pos_map = OwnMethods.ReadMap(graph_pos_map_path);
			long[] graph_pos_map_list_test = new long[graph_pos_map.size()];
			for ( String key_str : graph_pos_map.keySet())
			{
				int key = Integer.parseInt(key_str);
				int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
				graph_pos_map_list_test[key] = pos_id;
			}
			
			SpatialFirst_List spatialFirstlist = new SpatialFirst_List(db_path_test, dataset_test, graph_pos_map_list_test);
			query_Graph.spa_predicate[1] = queryRectangle;

			HashMap<Integer, MyRectangle> spa_predicates = new HashMap<Integer, MyRectangle>();
//			spa_predicates.put(3, queryRectangle);	//query id 3
			//query id 5 do nothing on spa_predicates
			
			int pos = 1;
			long id = 511901;
			
			HashMap<Integer, Integer> NL_hopnum = new HashMap<Integer, Integer>();
//			NL_hopnum.put(1, 1); NL_hopnum.put(2, 2);	//query id 3
			//query id 5
			NL_hopnum.put(0, 1);
//			NL_hopnum.put(2, 2);
			
			
			Transaction tx = spatialFirstlist.dbservice.beginTx();
			
			Node node = spatialFirstlist.dbservice.getNodeById(id).getSingleRelationship
					(OSMRelation.GEOM, Direction.OUTGOING).getEndNode()
					.getSingleRelationship(RTreeRelationshipTypes.RTREE_REFERENCE, Direction.INCOMING).getStartNode();
			
			String query = spatialFirstlist.formSubgraphQuery(query_Graph, -1, Explain_Or_Profile.Profile, 
					spa_predicates, pos, id, NL_hopnum, node);
			OwnMethods.Print(query);
			
			Result result = spatialFirstlist.dbservice.execute(query);
//			ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
//			OwnMethods.Print(planDescription);
			int count = 0;
			HashSet<Long> ida1_list = new HashSet<Long>();
			while(result.hasNext())
			{
				Map<String, Object> row = result.next();
				long ida1 = (Long) row.get("id(a0)");
				ida1_list.add(ida1);
				count++;
			}
			OwnMethods.Print(count);
			
			for ( long ida1 : ida1_list)
				OwnMethods.WriteFile(log_path, true, ida1 + "\n");
			
			tx.success();
			tx.close();
			spatialFirstlist.dbservice.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void subgraphMatchQueryTest()
	{
		HashMap<String, String> graph_pos_map = OwnMethods.ReadMap(graph_pos_map_path);
		long[] graph_pos_map_list_test = new long[graph_pos_map.size()];
		for ( String key_str : graph_pos_map.keySet())
		{
			int key = Integer.parseInt(key_str);
			int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
			graph_pos_map_list_test[key] = pos_id;
		}
		
		SpatialFirst_List spatialFirstlist = new SpatialFirst_List(db_path_test, dataset_test, graph_pos_map_list_test);
		query_Graph.spa_predicate[1] = queryRectangle;	//query id 5
		
		//query id 3
//		query_Graph.spa_predicate[0] = queryRectangle;
//		query_Graph.spa_predicate[3] = queryRectangle;

		spatialFirstlist.query(query_Graph, -1);
		OwnMethods.Print(String.format("result size: %d", spatialFirstlist.result_count));
		spatialFirstlist.shutdown();
	}
	
	public static void subgraphMatchQuery_Block_Test()
	{
		HashMap<String, String> graph_pos_map = OwnMethods.ReadMap(graph_pos_map_path);
		long[] graph_pos_map_list_test = new long[graph_pos_map.size()];
		for ( String key_str : graph_pos_map.keySet())
		{
			int key = Integer.parseInt(key_str);
			int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
			graph_pos_map_list_test[key] = pos_id;
		}
		
		SpatialFirst_List spatialFirstlist = new SpatialFirst_List(db_path_test, dataset_test, graph_pos_map_list_test);
		
		query_Graph.spa_predicate[1] = queryRectangle;	//query id 5
		//query id 3
//		query_Graph.spa_predicate[0] = queryRectangle;
//		query_Graph.spa_predicate[3] = queryRectangle;

		spatialFirstlist.query_Block(query_Graph, -1);
		OwnMethods.Print(String.format("result size: %d", spatialFirstlist.result_count));
		spatialFirstlist.shutdown();
	}
	
	/**
	 * Query function with KNN predicate.
	 * @param query_Graph
	 * @param K
	 */
	public ArrayList<Long> LAGAQ_KNN(Query_Graph query_Graph, int K)
	{
		visit_spatial_object_count = 0;
		try {
			ArrayList<Long> resultIDs = new ArrayList<Long>(); 
			MyPoint queryLoc = null;
			int querySpatialVertexID = 0;
			
			for ( int i = 0; i < query_Graph.Has_Spa_Predicate.length; i++)
			{
				if (query_Graph.Has_Spa_Predicate[i])
				{
					querySpatialVertexID = i;
					MyRectangle queryRect = query_Graph.spa_predicate[i];
					queryLoc = new MyPoint(queryRect.min_x, queryRect.min_y);
					break;
				}
			}
				
			Transaction tx = dbservice.beginTx();
			Node root_node =  OSM_Utility.getRTreeRoot(dbservice, dataset);
			
			PriorityQueue<Element> queue = new PriorityQueue<Element>(100, new KNNComparator());
			queue.add(new Element(root_node, 0));
			
			while ( resultIDs.size() < K && queue.isEmpty() == false)
			{
				Element element = queue.poll();
				Node node = element.node;
				//Tree non-leaf node
				if ( node.hasRelationship(Labels.RTreeRel.RTREE_CHILD, Direction.OUTGOING))
				{
					Iterable<Relationship> rels = node.getRelationships(Labels.RTreeRel.RTREE_CHILD, 
							Direction.OUTGOING);
					for ( Relationship relationship : rels)
					{
						Node child = relationship.getEndNode();
						double[] bbox = (double[]) child.getProperty("bbox");
						MyRectangle MBR = new MyRectangle(bbox[0], bbox[1], bbox[2], bbox[3]);
						queue.add(new Element(child, Utility.distance(queryLoc, MBR)));
					}
				}
				//tree leaf node
				else if ( node.hasRelationship(Labels.RTreeRel.RTREE_REFERENCE, Direction.OUTGOING))
				{
					Iterable<Relationship> rels = node.getRelationships(Labels.RTreeRel.RTREE_REFERENCE, 
							Direction.OUTGOING);
					for ( Relationship relationship : rels)
					{
						Node geom = relationship.getEndNode();
						Object object = geom.getProperty(lon_name);
						if ( object == null)
							throw new Exception(String.format("Node %d does not have %s property", 
									geom.getId(), lon_name));
						else
						{
							double lon = (Double)object;
							double lat = (Double) geom.getProperty(lat_name);
							queue.add(new Element(geom, Utility.distance(queryLoc, new MyPoint(lon, lat))));
						}
					}
				}
				//spatial object
				else if (node.hasLabel(Labels.GraphLabel.GRAPH_1))
				{
					visit_spatial_object_count++;
					long id = node.getId();
					OwnMethods.Print(id);
					String query = RisoTreeQueryPN.formQuery_KNN(query_Graph, -1, Explain_Or_Profile.Profile, 
							querySpatialVertexID, id);
					Result result = dbservice.execute(query);
					if ( result.hasNext())
					{
						resultIDs.add(id);
						OwnMethods.Print(String.format("%d, %f", id, element.distance));
					}
//					resultIDs.add(id);
					
				}
				else
					throw new Exception(String.format("Node %d does not affiliate to any type!", node.getId()));
			}
			
			return resultIDs;
			
		}
		catch(Exception e) {
			e.printStackTrace(); System.exit(-1);
		}
		return null;
	}

	//for test
	static String dataset_test;
	static String db_path_test;
	static String querygraph_path;
	static String queryrect_path;
	static String graph_pos_map_path;
	static String log_path;
	static int query_id = 0;
	static ArrayList<Query_Graph> queryGraphs;
	static Query_Graph query_Graph;
	static MyRectangle queryRectangle;
	
	static int nodeCount = 5;
	static int name_suffix = 1280;

	public static void initVariablesForTest()
	{
		Config config = new Config();
		dataset_test = config.getDatasetName();
		system systemName = config.getSystemName();
		String neo4jVersion = config.GetNeo4jVersion(); 
		switch (systemName) {
		case Ubuntu:
			db_path_test = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", neo4jVersion, dataset_test);
			querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph.txt";
			graph_pos_map_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset_test + "/node_map_RTree.txt";
			log_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset_test + "/test.log";
			break;
		case Windows:
//			db_path_test = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset_test, neo4jVersion, dataset_test);
//			querygraph_path = "D:\\Ubuntu_shared\\GeoMinHop\\query\\query_graph.txt";
//			graph_pos_map_path = "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset_test + "\\node_map_RTree.txt";
			String dataDirectory = "D:\\Ubuntu_shared\\GeoMinHop\\data";
			db_path_test = String.format("%s\\%s\\%s_%s\\data\\databases\\graph.db", dataDirectory, dataset_test, neo4jVersion, dataset_test);
			graph_pos_map_path = "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset_test + "\\node_map_RTree.txt";
			String querygraphDir = String.format("D:\\Google_Drive\\Projects\\risotree\\query\\query_graph\\%s", dataset_test);
			String spaPredicateDir = String.format("D:\\Google_Drive\\Projects\\risotree\\query\\spa_predicate\\%s", dataset_test);
			querygraph_path = String.format("%s\\%d.txt", querygraphDir, nodeCount);
			queryrect_path = String.format("%s\\queryrect_%d.txt", spaPredicateDir, name_suffix);
			log_path = "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset_test + "\\test.log";
		default:
			break;
		}
		queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
		query_Graph = queryGraphs.get(query_id);
//		queryRectangle = new MyRectangle(-80.115808, 26.187365, -80.115808, 26.187365);//1
		queryRectangle = new MyRectangle(-80.119514, 26.183659, -80.112102, 26.191071);//10
//		queryRectangle = new MyRectangle(-80.133549, 26.169624, -80.098067, 26.205106);//100
//		queryRectangle = new MyRectangle(-84.468680, 33.879658, -84.428434, 33.919904);//100
//		queryRectangle = new MyRectangle(-80.200353, 26.102820, -80.031263, 26.271910);//1000
		//queryRectangle = new MyRectangle(-98.025157, 29.953977, -97.641747, 30.337387);//10000
		//queryRectangle = new MyRectangle(-91.713778, 14.589395, -68.517838, 37.785335);//100000
	}
	
	public static void KNNTest()
	{
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
		Query_Graph query_Graph = queryGraphs.get(query_id);
		
		ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
		MyRectangle rectangle = queryrect.get(0);
		int j = 0;
		for (  ; j < query_Graph.graph.size(); j++)
			if(query_Graph.Has_Spa_Predicate[j])
				break;
		query_Graph.spa_predicate[j] = rectangle;
		
		HashMap<String, String> graph_pos_map = OwnMethods.ReadMap(graph_pos_map_path);
		long[] graph_pos_map_list= new long[graph_pos_map.size()];
		for ( String key_str : graph_pos_map.keySet())
		{
			int key = Integer.parseInt(key_str);
			int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
			graph_pos_map_list[key] = pos_id;
		}
		SpatialFirst_List spatialFirst_List = new SpatialFirst_List(db_path_test, dataset_test, graph_pos_map_list);
		int K = 10;
		try
		{
			ArrayList<Long> resultIDs = spatialFirst_List.LAGAQ_KNN(query_Graph, K);
			OwnMethods.Print(resultIDs);
			OwnMethods.Print(spatialFirst_List.visit_spatial_object_count);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			spatialFirst_List.dbservice.shutdown();
		}
		finally
		{
			spatialFirst_List.dbservice.shutdown();
		}
	}

	public static void main(String[] args) {
		initVariablesForTest();

//		rangeQueryTest();
//		formSubgraphQueryTest();
//		subgraphMatchQueryTest();
//		subgraphMatchQuery_Block_Test();
		KNNTest();
	}

}
