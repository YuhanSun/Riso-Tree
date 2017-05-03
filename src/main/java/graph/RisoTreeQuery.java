package graph;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.TreeSet;

import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import commons.Config;
import commons.MyRectangle;
import commons.OwnMethods;
import commons.Query_Graph;
import commons.Utility;
import osm.OSM_Utility;

public class RisoTreeQuery {
	
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
	
	public static TreeSet<Long> Query(Query_Graph query_Graph, Node root_node, int max_hopnum)
	{
		try {
			//<spa_id, rectangle>
			HashMap<Integer, MyRectangle> spa_predicates = new HashMap<Integer, MyRectangle>();
			
			//<spa_id, <neighbor_id, hop_num>>
			HashMap<Integer, HashMap<Integer, Integer>> min_hop = new HashMap<Integer, HashMap<Integer, Integer>>();
			//<spa_id, <neighbor_id, size_property_name>>
			HashMap<Integer, HashMap<Integer, String>> NL_size_propertyname = new HashMap<Integer, HashMap<Integer, String>>();
			//<spa_id, <neighbor_id, list_property_name>>
			HashMap<Integer, HashMap<Integer, String>> NL_list_propertyname = new HashMap<Integer, HashMap<Integer,String>>();	
			//<spa_id, <neighbor_id, NL_list>>
			HashMap<Integer, HashMap<Integer, HashSet<Integer>>> NL_list = new HashMap<Integer, HashMap<Integer, HashSet<Integer>>>();
			
			int[][] min_hop_array = Ini_Minhop(query_Graph);
			
			for (int i = 0; i < query_Graph.Has_Spa_Predicate.length; i++)
				if(query_Graph.Has_Spa_Predicate[i])
				{
					spa_predicates.put(i, query_Graph.spa_predicate[i]);
					min_hop.put(i, new HashMap<Integer, Integer>());
					NL_size_propertyname.put(i, new HashMap<Integer, String>());
					NL_list_propertyname.put(i, new HashMap<Integer, String>());
					for ( int j = 0; j < min_hop_array[i].length; j++)
					{
						if(min_hop_array[i][j] <= max_hopnum && min_hop_array[i][j] != -1)
						{
							min_hop.get(i).put(j, min_hop_array[i][j]);
							int label = query_Graph.label_list[j];
							NL_size_propertyname.get(i).put(j, String.format("NL_%d_%d_size", min_hop_array[i][j], label));
							NL_list_propertyname.get(i).put(j, String.format("NL_%d_%d_list", min_hop_array[i][j], label));
						}
					}
				}
			
			OwnMethods.Print(String.format("min_hop: %s", min_hop));
			OwnMethods.Print(String.format("NL_property: %s", NL_size_propertyname));
					
			LinkedList<Node> cur_list = new LinkedList<Node>();
			cur_list.add(root_node);
			LinkedList<Node> next_list = new LinkedList<Node>(); 
			
			int level_index = 0;
			while(cur_list.isEmpty() == false)
			{
				long start = System.currentTimeMillis();
				//<spa_id, card>
				HashMap<Integer, Double> spa_cards = new HashMap<Integer, Double>();
				for (int key : spa_predicates.keySet())
					spa_cards.put(key, 0.0);
				
				//<spa_id, <neighbor_id, card>>
				HashMap<Integer, HashMap<Integer, Double>> NL_cards = new HashMap<Integer, HashMap<Integer, Double>>();
				for (int spa_id : min_hop.keySet())
				{
					NL_cards.put(spa_id, new HashMap<Integer, Double>());
					HashMap<Integer, Integer> min_hop_vector = min_hop.get(spa_id);
					for ( int neighbor_pos : min_hop_vector.keySet())
						NL_cards.get(spa_id).put(neighbor_pos, 0.0);	
				}
				
				//<spa_id, overlap_nodes_list>
//				HashMap<Integer, LinkedList<Node>> overlap_MBR_list = new HashMap<>();
				LinkedList<Node>overlap_MBR_list = new LinkedList<Node>(); //just support one spatial predicate
				
				Iterator<Node> iterator = cur_list.iterator();
				while(iterator.hasNext())
				{
					Node node = iterator.next();
					if(node.hasProperty("bbox"))
					{
						double[] bbox = (double[]) node.getProperty("bbox");
						MyRectangle MBR = new MyRectangle(bbox[0], bbox[1], bbox[2], bbox[3]);
						OwnMethods.Print(MBR);
						double MBR_area = MBR.area();
						
						int spa_count = (int) node.getProperty("count");
						for ( int key : spa_predicates.keySet())
						{
							MyRectangle intersect = MBR.intersect(spa_predicates.get(key)); 
							if(intersect != null)
							{
//								overlap_MBR_list.get(key).add(node);
								overlap_MBR_list.add(node);
								
								double ratio;
								if(MBR_area == 0)
									ratio = 1;
								else
									ratio = intersect.area() / MBR_area;
								spa_cards.put(key, (spa_cards.get(key) + ratio * spa_count));
								
								HashMap<Integer, Double> NL_cards_vector = NL_cards.get(key);
								for ( int neighbor_id : NL_cards_vector.keySet())
								{
									int NL_label_size = (int) node.getProperty(NL_size_propertyname.get(key).get(neighbor_id));
									NL_cards_vector.put(neighbor_id, (NL_cards_vector.get(neighbor_id) + ratio * NL_label_size));
								}
								
								Iterable<Relationship> rels = node.getRelationships(RTreeRelationshipTypes.RTREE_CHILD, Direction.OUTGOING);
								for ( Relationship relationship : rels)
									next_list.add(relationship.getEndNode());
							}
						}
					}
					else
						throw new Exception(String.format("node %d does not has \"bbox\" property", node));
				}
				
				OwnMethods.Print(String.format("level %d", level_index));
				
				double min_spa_card = Double.MAX_VALUE, min_NL_card = Double.MAX_VALUE;
				int min_NL_spa_id = 0, min_NL_neighbor_id = 0;
				
				for (int key : spa_cards.keySet())
				{
					double spa_card = spa_cards.get(key);
					if(spa_card < min_spa_card)
						min_spa_card = spa_card;
					
					OwnMethods.Print(String.format("spa_card %d %f", key, spa_card));
					
					HashMap<Integer, Double> NL_cards_vector = NL_cards.get(key);
					for ( int neighbor_id : NL_cards_vector.keySet())
					{
						double NL_card = NL_cards_vector.get(neighbor_id);
						if(NL_card < min_NL_card)
						{
							min_NL_spa_id = key;	min_NL_neighbor_id = neighbor_id;
							min_NL_card = NL_card;
						}
						OwnMethods.Print(String.format("NL_size %d %d %s", key, neighbor_id, NL_cards_vector.get(neighbor_id).toString()));
					}
				}
				
				OwnMethods.Print(String.format("level %d min card : %f", level_index, Math.min(min_spa_card, min_NL_card)));
				
				if (overlap_MBR_list.isEmpty() == true)
				{
					OwnMethods.Print("No result satisfy the query.");
					return null;
				}
				
				//construct the NL_list with the highest selectivity
				long start1 = System.currentTimeMillis();
//				if ( min_NL_card < min_spa_card)
				{
					String property_name = NL_list_propertyname.get(min_NL_spa_id).get(min_NL_neighbor_id);
					
					HashSet<Integer> min_NL_list = new HashSet<Integer>();
//					NL_list.put(min_NL_spa_id, new HashMap<Integer, HashSet<Integer>>());
//					NL_list.get(min_NL_spa_id).put(min_NL_neighbor_id, new HashSet<Integer>());
					for ( Node node : overlap_MBR_list)
					{
						int[] NL_list_label = ( int[] ) node.getProperty(property_name);
						for ( int node_id : NL_list_label)
							min_NL_list.add(node_id);
					}
					OwnMethods.Print(String.format("min_NL_list size is %d", min_NL_list.size()));
					if ( min_NL_list.size() < min_spa_card)
					{
						OwnMethods.Print("NL_list is smaller");
						
					}
				}
				OwnMethods.Print(String.format("NL_serialize time: %d", System.currentTimeMillis() - start1));
				
				OwnMethods.Print(String.format("level %d time: %d", level_index, System.currentTimeMillis() - start));
				
				int located_in_count = 0;
				if( overlap_MBR_list.isEmpty() == false && next_list.isEmpty())
				{
					TreeSet<Long> ids = new TreeSet<Long>(); 
					start = System.currentTimeMillis();
					for ( Node node : overlap_MBR_list)
						for ( Relationship relationship : node.getRelationships(
								Direction.OUTGOING, RTreeRelationshipTypes.RTREE_REFERENCE))
						{
							Node geom = relationship.getEndNode();
							double[] bbox = (double[]) geom.getProperty("bbox");
							MyRectangle rectangle = new MyRectangle(bbox);
							for ( int key : spa_predicates.keySet())
								if ( rectangle.intersect(spa_predicates.get(key)) != null)
								{
									ids.add(geom.getId());
									located_in_count++;
								}
						}
					OwnMethods.Print(String.format("Located in nodes: %d", located_in_count));
					level_index++;
					OwnMethods.Print(String.format("level %d time: %d", level_index, System.currentTimeMillis() - start));
					return ids;
				}
				
				cur_list = next_list;
				next_list = new LinkedList<Node>();
				

				level_index++;
				
				
			}
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return null;
	}
	
	static String dataset = "Gowalla";
	static Config config = new Config();
	static String version = config.GetNeo4jVersion();
//	static String db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
//	static String querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph.txt";
	static String db_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset, version, dataset);
	static String querygraph_path = "D:\\Ubuntu_shared\\GeoMinHop\\query\\query_graph.txt";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int query_id = 5;
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
		Query_Graph query_Graph = queryGraphs.get(query_id);
		query_Graph.spa_predicate[1] = new MyRectangle(-84.468680, 33.879658, -84.428434, 33.919904);//100
//		query_Graph.spa_predicate[1] = new MyRectangle(9.523183, 46.839041, 9.541593, 46.857451);//100
//		query_Graph.spa_predicate[1] = new MyRectangle(-98.025157, 29.953977, -97.641747, 30.337387);/10,000
//		query_Graph.spa_predicate[1] = new MyRectangle(-91.713778, 14.589395, -68.517838, 37.785335);//100,000
//		query_Graph.spa_predicate[1] = new MyRectangle(-179.017757, -135.408325, 207.362849, 250.972281);//1,000,000
//		query_Graph.spa_predicate[1] = new MyRectangle(-91.713778, 14.589395, -68.517838, 37.785335);
//		query_Graph.spa_predicate[3] = new MyRectangle(-98.025157, 29.953977, -97.641747, 30.337387);
		
		try {
			GraphDatabaseService dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
			
			long start = System.currentTimeMillis();
			Transaction tx = dbservice.beginTx();
			Node root_node =  OSM_Utility.getRTreeRoot(dbservice, dataset);
			TreeSet<Long> result1 = Query(query_Graph, root_node, 2);
			tx.success();
			tx.close();
			OwnMethods.Print("Time:" + (System.currentTimeMillis() - start));
			dbservice.shutdown();
			
//			SpatialFirst spatialFirst = new SpatialFirst(db_path, dataset);
//			TreeSet<Long> result2 = spatialFirst.Query(query_Graph);
//			spatialFirst.shutdown();
//			
//			LinkedList<Long> counter1 = new LinkedList<Long>();
//			for ( long id : result1)
//				if(result2.contains(id) == false)
//					counter1.add(id);
//			
//			for ( long id : counter1)
//				OwnMethods.Print(id);
//			OwnMethods.Print(counter1.size());
			
//			LinkedList<Long> counter2 = new LinkedList<Long>();
//			for ( long id : result2)
//				if(result1.contains(id) == false)
//					counter2.add(id);
//			
//			for ( long id : counter2)
//				OwnMethods.Print(id);
//			OwnMethods.Print(counter2.size());
			
//			dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
//			tx = dbservice.beginTx();
//			for ( long id : counter1)
//			{
//				double[] bbox = (double[])dbservice.getNodeById(id).getProperty("bbox");
//				MyRectangle rectangle = new MyRectangle(bbox);
////				for ( double element : bbox)
////					OwnMethods.Print(element);
//				OwnMethods.Print(rectangle.toString());
////				break;
//			}
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
