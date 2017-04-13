package graph;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

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
	
	public static void Query(Query_Graph query_Graph, Node root_node, int max_hopnum)
	{
		try {
			//<spa_id, rectangle>
			HashMap<Integer, MyRectangle> spa_predicates = new HashMap<>();
			
			//<spa_id, <neighbor_id, hop_num>>
			HashMap<Integer, HashMap<Integer, Integer>> min_hop = new HashMap<>();
			//<spa_id, <neighbor_id, property_name>>
			HashMap<Integer, HashMap<Integer, String>> NL_size_propertyname = new HashMap<>();
			//<spa_id, <neighbor_id, NL_list>>
			HashMap<Integer, HashMap<Integer, HashSet<Integer>>> NL_list = new HashMap<>();
			
			int[][] min_hop_array = Ini_Minhop(query_Graph);
			
			for (int i = 0; i < query_Graph.Has_Spa_Predicate.length; i++)
				if(query_Graph.Has_Spa_Predicate[i])
				{
					spa_predicates.put(i, query_Graph.spa_predicate[i]);
					min_hop.put(i, new HashMap<>());
					NL_size_propertyname.put(i, new HashMap<>());
					for ( int j = 0; j < min_hop_array[i].length; j++)
					{
						if(min_hop_array[i][j] <= max_hopnum && min_hop_array[i][j] != -1)
						{
							min_hop.get(i).put(j, min_hop_array[i][j]);
							int label = query_Graph.label_list[j];
							NL_size_propertyname.get(i).put(j, String.format("NL_%d_%d_size", min_hop_array[i][j], label));
						}
					}
				}
			
			OwnMethods.Print(String.format("min_hop: %s", min_hop));
			OwnMethods.Print(String.format("NL_property: %s", NL_size_propertyname));
					
			LinkedList<Node> cur_list = new LinkedList<>();
			cur_list.add(root_node);
			LinkedList<Node> next_list = new LinkedList<>(); 
			
			int level_index = 0;
			while(cur_list.isEmpty() == false)
			{
				//<spa_id, card>
				HashMap<Integer, Double> spa_cards = new HashMap<>();
				for (int key : spa_predicates.keySet())
					spa_cards.put(key, 0.0);
				
				//<spa_id, <neighbor_id, card>>
				HashMap<Integer, HashMap<Integer, Double>> NL_cards = new HashMap<>();
				for (int spa_id : min_hop.keySet())
				{
					NL_cards.put(spa_id, new HashMap<>());
					HashMap<Integer, Integer> min_hop_vector = min_hop.get(spa_id);
					for ( int neighbor_pos : min_hop_vector.keySet())
						NL_cards.get(spa_id).put(neighbor_pos, 0.0);	
				}
				
				//<spa_id, overlap_nodes_list>
//				HashMap<Integer, LinkedList<Node>> overlap_MBR_list = new HashMap<>();
				LinkedList<Node>overlap_MBR_list = new LinkedList<>(); 
				
				Iterator<Node> iterator = cur_list.iterator();
				while(iterator.hasNext())
				{
					Node node = iterator.next();
					if(node.hasProperty("bbox"))
					{
						double[] bbox = (double[]) node.getProperty("bbox");
						MyRectangle MBR = new MyRectangle(bbox[0], bbox[1], bbox[2], bbox[3]);
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
				level_index++;
				
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
					return;
				}
				
				if ( min_NL_card < min_spa_card)
				{
//					String property_name = NL_
					for ( Node node : overlap_MBR_list)
					{
						
					}
				}
				
				cur_list = next_list;
				next_list = new LinkedList<>();
				
				
				
			}
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}
	
	static String dataset = "Gowalla";
	static Config config = new Config();
	static String version = config.GetNeo4jVersion();
	static String db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int query_id = 5;
		String querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph.txt";
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
		Query_Graph query_Graph = queryGraphs.get(query_id);
		query_Graph.spa_predicate[1] = new MyRectangle(-84.468680, 33.879658, -84.428434, 33.919904);
//		query_Graph.spa_predicate[3] = new MyRectangle(-98.025157, 29.953977, -97.641747, 30.337387);
		
		try {
			String db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
			GraphDatabaseService dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
			
			long start = System.currentTimeMillis();
			Transaction tx = dbservice.beginTx();
			Node root_node =  OSM_Utility.getRTreeRoot(dbservice, dataset);
			Query(query_Graph, root_node, 2);
			tx.success();
			tx.close();
			OwnMethods.Print("Time:" + (System.currentTimeMillis() - start));
			dbservice.shutdown();
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
