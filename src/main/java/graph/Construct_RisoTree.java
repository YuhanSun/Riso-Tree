package graph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;


import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import commons.Config;
import commons.Labels;
import commons.OwnMethods;
import commons.Config.system;
import commons.Labels.RTreeRel;
import osm.OSM_Utility;
import scala.collection.immutable.RedBlackTree.Tree;

/**
 * 
 * @author yuhansun
 *
 */
public class Construct_RisoTree {

	static Config config = new Config();
	static String dataset = config.getDatasetName();
	static String version = config.GetNeo4jVersion();
	static system systemName = config.getSystemName();
	static int MAX_HOPNUM = config.getMaxHopNum();
	static int nonspatial_label_count = config.getNonSpatialLabelCount();

	static String db_path;
//	static String vertex_map_path;
	static String graph_path;
	static String geo_id_map_path;
	static String label_list_path;
	static String graph_node_map_path;
	static String log_path;
	
	static ArrayList<Integer> labels;//all labels in the graph
	
	static void initParameters()
	{
		switch (systemName) {
		case Ubuntu:
			db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
//			vertex_map_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map.txt", dataset);
			graph_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
//			geo_id_map_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/geom_osmid_map.txt", dataset);
			label_list_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/label.txt", dataset);
//			graph_node_map_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map.txt", dataset);
			log_path = String.format("/mnt/hgfs/Experiment_Result/Riso-Tree/%s/set_label.log", dataset);
			break;
		case Windows:
			db_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset, version, dataset);
//			vertex_map_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\node_map.txt", dataset);
			graph_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\graph.txt", dataset);
//			geo_id_map_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/geom_osmid_map.txt", dataset);
			label_list_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\label.txt", dataset);
//			graph_node_map_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\node_map.txt", dataset);
			log_path = String.format("/mnt/hgfs/Experiment_Result/Riso-Tree/%s/set_label.log", dataset);
		default:
			break;
		}
		
		if (nonspatial_label_count == 1)
			labels = new ArrayList<Integer>(Arrays.asList(0, 1));
		else
		{
			labels = new ArrayList<Integer>(1 + nonspatial_label_count);
			labels.add(1);
			for ( int i = 0; i < nonspatial_label_count; i++)
				labels.add(i + 2);
		}
	}
	
	public static void main(String[] args) {
		initParameters();
		
		Construct();
//		ClearNL();
//		
//		ArrayList<Integer> label_list = OwnMethods.readIntegerArray(label_list_path);
//		String neighborListPath = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/"+dataset+"/2hop_neighbor_spa.txt";
//		Construct(neighborListPath, 2, labels, label_list);
		
		//for the second hop
//		LoadLeafNodeNList();
//		MergeLeafNL();
		
		generateNL_size();
//		NL_size_Check();
//		set_Spatial_Label();
		
		//set a lot of labels
//		set_NL_list_label(max_hop_num, labels);
//		set_NL_list_label_DeepestNonLeaf(max_hop_num, labels);
	}
	
	public static void set_NL_list_label_DeepestNonLeaf(int max_hop_num, ArrayList<Integer> labels)
	{
		HashMap<String, String> map = OwnMethods.ReadMap(graph_node_map_path); 
		HashMap<Integer, Integer> graph_node_map = new HashMap<Integer, Integer>();
		for ( String key : map.keySet())
		{
			String value = map.get(key);
			int graph_id = Integer.parseInt(key);
			int pos_id = Integer.parseInt(value);
			graph_node_map.put(graph_id, pos_id);
		}
		
		GraphDatabaseService dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
		try {
			Transaction tx = dbservice.beginTx();
			
			Set<Node> nodes = OSM_Utility.getRTreeNonleafDeepestLevelNodes(dbservice, dataset);
			Queue<Long> queue = new LinkedList<Long>(); 
			for ( Node node : nodes) 
				queue.add(node.getId());
			tx.success();
			tx.close();
			
			int index = 0, count = 0, commit_time = 0;
			tx = dbservice.beginTx();
			while ( queue.isEmpty() == false)
			{
				OwnMethods.Print("index:" + index);
				OwnMethods.Print("count:" + count);
				Long pos = queue.poll();
				Node node = dbservice.getNodeById(pos);
				if(count < 4250)
				{
					count++;
					continue;
				}
				
				int rtree_id = (Integer) node.getProperty("rtree_id");
				for ( int i = 1; i <= max_hop_num; i++)
				{
					for ( int label : labels)
					{
						String property_name = String.format("NL_%d_%d_list", i, label);
						if(node.hasProperty(property_name))
						{
							int[] NL_list_label = ( int[]) node.getProperty(property_name);
							String label_name = String.format("%d_NL_%d_%d_list", rtree_id, i, label);
//							OwnMethods.Print(label_name);
							Label sub_label = DynamicLabel.label(label_name);
							for ( int id : NL_list_label)
							{
								int pos_id = graph_node_map.get(id);
								Node graph_node = dbservice.getNodeById(pos_id);
								graph_node.addLabel(sub_label);
//								OwnMethods.Print(graph_node.getAllProperties());
//								break;
							}
						}
					}
				}
				if(index == 19)
				{
					long start = System.currentTimeMillis();
					tx.success();
					tx.close();
					OwnMethods.WriteFile(log_path, true, (System.currentTimeMillis() - start) + "\n");
					index = 0; count++;
					dbservice.shutdown();
					
					commit_time++;
//					if(commit_time == 2)
//						break;
					
					dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
					tx = dbservice.beginTx();
					continue;
				}
				index++; count++;
//				break;
			}
			if( index != 0)
			{
				tx.success();
				tx.close();
			}
			dbservice.shutdown();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void set_NL_list_label(int max_hop_num, ArrayList<Integer> labels)
	{
		HashMap<String, String> map = OwnMethods.ReadMap(graph_node_map_path); 
		HashMap<Integer, Integer> graph_node_map = new HashMap<Integer, Integer>();
		for ( String key : map.keySet())
		{
			String value = map.get(key);
			int graph_id = Integer.parseInt(key);
			int pos_id = Integer.parseInt(value);
			graph_node_map.put(graph_id, pos_id);
		}
		
		GraphDatabaseService dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
		try {
			Transaction tx = dbservice.beginTx();
			Node root = OSM_Utility.getRTreeRoot(dbservice, dataset);
			long rootID = root.getId();
			tx.success();
			tx.close();
			
			Queue<Long> queue = new LinkedList<Long>(); 
			queue.add(rootID);
			int index = 0, count = 0;
			tx = dbservice.beginTx();
			while ( queue.isEmpty() == false)
			{
				OwnMethods.Print("index:" + index);
				OwnMethods.Print("count:" + count);
				Long pos = queue.poll();
				Node node = dbservice.getNodeById(pos);
				Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD);
				for ( Relationship relationship : rels)
					queue.add(relationship.getEndNode().getId());
				if(count < 2890)
				{
					count++;
					continue;
				}
				
				int rtree_id = (Integer) node.getProperty("rtree_id");
				for ( int i = 1; i <= max_hop_num; i++)
				{
					for ( int label : labels)
					{
						String property_name = String.format("NL_%d_%d_list", i, label);
						if(node.hasProperty(property_name))
						{
							int[] NL_list_label = ( int[]) node.getProperty(property_name);
							String label_name = String.format("%d_NL_%d_%d_list", rtree_id, i, label);
//							OwnMethods.Print(label_name);
							Label sub_label = DynamicLabel.label(label_name);
							for ( int id : NL_list_label)
							{
								int pos_id = graph_node_map.get(id);
								Node graph_node = dbservice.getNodeById(pos_id);
								graph_node.addLabel(sub_label);
//								OwnMethods.Print(graph_node.getAllProperties());
//								break;
							}
						}
					}
				}
				if(index == 20)
				{
					tx.success();
					tx.close();
					index = 0; count++;
					dbservice.shutdown();
					
					dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
					tx = dbservice.beginTx();
					continue;
				}
				index++; count++;
//				break;
			}
			if( index != 0)
			{
				tx.success();
				tx.close();
				dbservice.shutdown();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
	public static void NL_size_Check()
	{
		String output_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/treenode_NL_stast.txt";
		GraphDatabaseService dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
		try {
			Transaction tx = dbservice.beginTx();
			Node root = OSM_Utility.getRTreeRoot(dbservice, dataset);
			Queue<Node> cur = new LinkedList<Node>();
			Queue<Node> next = new LinkedList<Node>(); 
			cur.add(root);
			
			int level_index = 0;
			while(cur.isEmpty() == false)
			{
				OwnMethods.WriteFile(output_path, true, String.format("level: %d\n", level_index));
				level_index++;
				Iterator<Node> iterator = cur.iterator();
				while(iterator.hasNext())	
				{
					Node node = iterator.next();
					for ( int i = 1; i <= MAX_HOPNUM; i++)
					{
						for ( int label : labels)
						{
							String NL_size_property_name = String.format("NL_%d_%d_size", i, label);
							int size = (Integer) node.getProperty(NL_size_property_name);
							String line = String.format("%d %s size:%d", node.getProperty("rtree_id"), NL_size_property_name, size);
							OwnMethods.Print(line);
							OwnMethods.WriteFile(output_path, true, line + "\n");
						}
					}
					Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD);
					for ( Relationship relationship : rels)
						next.add(relationship.getEndNode());	
				}
				cur = next;
				next = new LinkedList<Node>();
			}
			tx.success();
			tx.close();
			dbservice.shutdown();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void generateNL_size()
	{
		OwnMethods.Print("Calculate NL size\n");
		GraphDatabaseService dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
		try {
			Transaction tx = dbservice.beginTx();
			Node root = OSM_Utility.getRTreeRoot(dbservice, dataset);
			Queue<Node> cur = new LinkedList<Node>();
			cur.add(root);
			
			while(cur.isEmpty() == false)
			{
				Node node = cur.poll();
				for ( int i = 1; i <= MAX_HOPNUM; i++)
				{
					for ( int label : labels)
					{
						String NL_list_property_name = String.format("NL_%d_%d_list", i, label);
						String NL_size_property_name = String.format("NL_%d_%d_size", i, label);
						if(node.hasProperty(NL_list_property_name))
						{
							int[] NL_list_label = (int[]) node.getProperty(NL_list_property_name);
							int size = NL_list_label.length;
							node.setProperty(NL_size_property_name, size);
//							OwnMethods.Print(String.format("%s size:%d", NL_list_property_name, size));
						}
						else
							node.setProperty(NL_size_property_name, 0);
					}
				}
				Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD);
				for ( Relationship relationship : rels)
					cur.add(relationship.getEndNode());
			}
			
			tx.success();
			tx.close();
			dbservice.shutdown();
			
		} catch (Exception e) {
			e.printStackTrace();	System.exit(-1);
		}
	}
	
	/**
	 * for the second hop
	 */
	public static void MergeLeafNL()
	{
		GraphDatabaseService dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
		try {
			Transaction tx = dbservice.beginTx();
			Set<Node> cur_level_nodes = OSM_Utility.getRTreeNonleafDeepestLevelNodes(dbservice, dataset);
			
			HashSet<Node> up_level_nodes = new HashSet<Node>(); 
			
			while( cur_level_nodes.isEmpty() == false)
			{
				for ( Node node : cur_level_nodes)
				{
					HashMap<Integer, TreeSet<Integer>> NL_list = new HashMap<Integer, TreeSet<Integer>>();
					HashMap<Integer, String> property_name_map = new HashMap<Integer, String>();
					
					for ( int label : labels)
					{
						NL_list.put(label, new TreeSet<Integer>());
						property_name_map.put(label, String.format("NL_%d_%d_list", MAX_HOPNUM, label));
					}
					
					for (Relationship relationship : node.getRelationships(Direction.OUTGOING))
					{
						Node out_neighbor = relationship.getEndNode();
						for ( int label : labels)
						{
							int[] neighbor_NL_label = (int[]) out_neighbor.getProperty(property_name_map.get(label));
							TreeSet<Integer> NL_label = NL_list.get(label);
							for ( int element : neighbor_NL_label)
								NL_label.add(element);
						}
					}
					
					for ( int label : labels)
					{
						TreeSet<Integer> NL_label_set = NL_list.get(label);
						int[] NL_label = new int[NL_label_set.size()];
						int index = 0;
						for ( int element : NL_label_set)
						{
							NL_label[index] = element;
							index++;
						}
						node.setProperty(property_name_map.get(label), NL_label);
					}
					
					
					
					Relationship relationship = node.getSingleRelationship(RTreeRelationshipTypes.RTREE_CHILD, Direction.INCOMING);
					if(relationship != null)
						up_level_nodes.add(relationship.getStartNode());
				}
				cur_level_nodes = up_level_nodes;
				up_level_nodes = new HashSet<Node>();
			}
			
			tx.success();
			tx.close();
			
			dbservice.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * for the second hop
	 */
	public static void LoadLeafNodeNList()
	{
		String NL_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/"+dataset+"/2hop_neighbor_spa.txt";
		
		String line = "";
		try {
			Map<String, String> map_str = OwnMethods.ReadMap(geo_id_map_path );
			Map<Long, Long> map = new HashMap<Long, Long>();
			Set<Entry<String, String >> set = map_str.entrySet();
			for (Entry<String, String> entry : set)
			{
				Long key = Long.parseLong(entry.getKey());
				Long value = Long.parseLong(entry.getValue());
				map.put(value, key);
			}
			
			ArrayList<Integer> label_list = OwnMethods.readIntegerArray(label_list_path);
//			ArrayList<Integer> labels = new ArrayList<Integer>(Arrays.asList(0,1));
			
			BufferedReader reader = new BufferedReader(new FileReader(new File(NL_path)));
			line = reader.readLine();
			int node_count = Integer.parseInt(line);
			
			GraphDatabaseService dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
			
			int tx_index = 0;
			
			while ( ( line = reader.readLine()) != null)
			{
				OwnMethods.Print(tx_index);
				tx_index++;
				
				Transaction tx = dbservice.beginTx();
				
				String [] str_list = line.split(",");
				long graph_id = Long.parseLong(str_list[0]);
				long pos_id = map.get(graph_id);
				
				int line_count = Integer.parseInt(str_list[1]);
				
				HashMap<Integer, ArrayList<Integer>> NL_list = new HashMap<Integer, ArrayList<Integer>>();
				for ( int label : labels)
					NL_list.put(label, new ArrayList<Integer>(line_count));

				for ( int i = 0; i < line_count; i++)
				{
					int neighbor = Integer.parseInt(str_list[i+2]);
					NL_list.get(label_list.get(neighbor)).add(neighbor);
				}
				
				Node node = dbservice.getNodeById(pos_id);
//				OwnMethods.Print(osm_node.getAllProperties().toString());
				for ( int label : labels)
				{
					if(NL_list.get(label).size() == 0)
						continue;
					else
					{
						ArrayList<Integer> label_neighbors = NL_list.get(label);
						int[] NL_array = new int[label_neighbors.size()];
						int i = 0;
						Iterator<Integer> iterator = label_neighbors.iterator();
						while(iterator.hasNext())
						{
							NL_array[i] = iterator.next();
							i++;
						}
						String property_name = String.format("NL_%d_%d_list", MAX_HOPNUM, label);
						node.setProperty(property_name, NL_array);
					}
				}
				
				tx.success();
				tx.close();
				
			}
			reader.close();
			dbservice.shutdown();
			
		} catch (Exception e) {
			OwnMethods.Print(line);
			e.printStackTrace();
		}
		
		
	}

	/**
	 * load first hop neighbor list with distinguishing labels
	 * for all non-leaf nodes in the RTree
	 * the graph data will be used directly
	 */
	public static void Construct()
	{
		OwnMethods.Print("Construct the first hop NL list\n");
		try {
			ArrayList<ArrayList<Integer>> graph = OwnMethods.ReadGraph(graph_path);
			ArrayList<Integer> labelList = OwnMethods.readIntegerArray(label_list_path);
			HashMap<Integer, String> NLListPropertyName = new HashMap<Integer, String>();
			for ( int label : labels)
				NLListPropertyName.put(label, String.format("NL_1_%d_list", label));
			
			GraphDatabaseService dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
			Transaction tx = dbservice.beginTx();
			
			//get all the deepest non-leaf nodes
			Set<Node> cur_level_nodes = OSM_Utility.getRTreeNonleafDeepestLevelNodes(dbservice, dataset); 
			for ( Node node : cur_level_nodes)
			{
				TreeSet<Integer> NL = new TreeSet<Integer>();
				Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING, Labels.RTreeRel.RTREE_REFERENCE);
				for ( Relationship rel : rels)
				{
					Node child  = rel.getEndNode();
					int graph_id = (Integer) child.getProperty("id");
					for (int neighbor : graph.get(graph_id))
						NL.add(neighbor);
				}
				
				HashMap<Integer, ArrayList<Integer>> labelID_list = new HashMap<Integer, ArrayList<Integer>>();
				for ( int label : labels)
					labelID_list.put(label, new ArrayList<Integer>(NL.size()));
				
				Iterator<Integer> iterator = NL.iterator();
				while(iterator.hasNext())
				{
					int neighborID = iterator.next();
					int label = labelList.get(neighborID);
					labelID_list.get(label).add(neighborID);
				}
				
				for ( int label : labels)
					if ( labelID_list.get(label).size() > 0)
					{
						ArrayList<Integer> neighborList = labelID_list.get(label);
						int[] neighborArray = new int[neighborList.size()];
						for ( int i = 0; i < neighborList.size(); i++)
							neighborArray[i] = neighborList.get(i);
						node.setProperty(NLListPropertyName.get(label), neighborArray);
					}
			}
			
			//upper level
			Set<Node> next_level_nodes = new HashSet<Node>();
			while (cur_level_nodes.isEmpty() ==  false)
			{
				for (Node node : cur_level_nodes)
				{
					Relationship relationship = node.getSingleRelationship(RTreeRel.RTREE_CHILD, Direction.INCOMING);
					if ( relationship != null)
					{
						Node parent = relationship.getStartNode();
						next_level_nodes.add(parent);
					}
				}
				
				for ( Node node : next_level_nodes)
				{
					HashMap<Integer, TreeSet<Integer>> labelID_neighborSet = new HashMap<Integer, TreeSet<Integer>>();
					for ( int label : labels)
						labelID_neighborSet.put(label, new TreeSet<Integer>());
					
					Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING, Labels.RTreeRel.RTREE_CHILD);
					for ( Relationship rel : rels)
					{
						Node child  = rel.getEndNode();
						
						for ( int label : labels)
						{
							if ( child.hasProperty(NLListPropertyName.get(label)))
							{
								int[] list = (int[]) child.getProperty(NLListPropertyName.get(label));
								for ( int neighbor : list)
									labelID_neighborSet.get(label).add(neighbor);
							}
						}
					}
					
					for ( int label : labels)
					{
						TreeSet<Integer> neighborSet = labelID_neighborSet.get(label);
						if ( neighborSet.size() > 0)
						{
							int[] NL_array = new int[neighborSet.size()];
							Iterator<Integer> iterator = neighborSet.iterator();
							int i = 0;
							while(iterator.hasNext())
							{
								NL_array[i] = iterator.next();
								i++;
							}
							node.setProperty(NLListPropertyName.get(label), NL_array);
						}
					}
				}
				
				cur_level_nodes = next_level_nodes;
				next_level_nodes = new HashSet<Node>();
			}
			tx.success();
			tx.close();
			dbservice.shutdown();
			
		} catch (Exception e) {
			e.printStackTrace();	System.exit(-1);
		}
		
	}
	
	/**
	 * load specific hop neighbor list from file
	 * @param hop_num
	 * @param labels
	 * @param label_list
	 */
	public static void Construct(String neighborListPath, int hop_num, ArrayList<Integer> labels, 
			ArrayList<Integer> label_list)
	{
		int offset = 196591;
		try {
			ArrayList<ArrayList<Integer>> graph = OwnMethods.ReadGraph(neighborListPath);
			Map<String, String> map_str = OwnMethods.ReadMap(geo_id_map_path );
			Map<Long, Long> map = new HashMap<Long, Long>();
			Set<Entry<String, String >> set = map_str.entrySet();
			for (Entry<String, String> entry : set)
			{
				Long key = Long.parseLong(entry.getKey());
				Long value = Long.parseLong(entry.getValue());
				map.put(key, value);
			}
			
			GraphDatabaseService dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
			Transaction tx = dbservice.beginTx();
			
			Set<Node> cur_level_nodes = OSM_Utility.getRTreeNonleafDeepestLevelNodes(dbservice, dataset); 
			for ( Node node : cur_level_nodes)
			{
				HashMap<Integer, TreeSet<Integer>> NL = new HashMap<Integer, TreeSet<Integer>>();
				for (int i = 0; i < labels.size(); i++)
					NL.put(labels.get(i), new TreeSet<Integer>());
				
				Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING, Labels.RTreeRel.RTREE_REFERENCE);
				for ( Relationship rel : rels)
				{
					Node child  = rel.getEndNode();
					long pos_id = child.getId();
					long graph_id = map.get(pos_id) - offset;
					if(graph_id > Integer.MAX_VALUE)
						throw new Exception("graph id out of int range!");
					int id = (int) graph_id;
					for (int neighbor : graph.get(id))
					{
						int label = label_list.get(neighbor);
						NL.get(label).add(neighbor);
					}
				}
				
				
				for ( int label : labels)
				{
					TreeSet<Integer> hop_neighbors = NL.get(label);
					int[] NL_array = new int[hop_neighbors.size()];
					int i = 0;
					Iterator<Integer> iterator = hop_neighbors.iterator();
					while(iterator.hasNext())
					{
						NL_array[i] = iterator.next();
						i++;
					}
					String property_name = String.format("NL_%d_%d_list", hop_num, label);
					node.setProperty(property_name, NL_array);
				}
				
			}
			
			Set<Node> next_level_nodes = new HashSet<Node>();
			while (cur_level_nodes.isEmpty() ==  false)
			{
				for (Node node : cur_level_nodes)
				{
					Relationship relationship = node.getSingleRelationship(RTreeRel.RTREE_CHILD, Direction.INCOMING);
					if ( relationship != null)
					{
						Node parent = relationship.getStartNode();
						next_level_nodes.add(parent);
					}
				}
				
				for ( Node node : next_level_nodes)
				{
					HashMap<Integer, TreeSet<Integer>> NL = new HashMap<Integer, TreeSet<Integer>>();
					for (int i = 0; i < labels.size(); i++)
						NL.put(labels.get(i), new TreeSet<Integer>());
					Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING, Labels.RTreeRel.RTREE_CHILD);
					for ( Relationship rel : rels)
					{
						Node child  = rel.getEndNode();
						
						for (int label : labels)
						{
							String property_name = String.format("NL_%d_%d_list", hop_num, label);
							if(child.hasProperty(property_name))
							{
								int[] child_NL_list = (int[]) child.getProperty(property_name);
								for ( int neighbor : child_NL_list)
									NL.get(property_name).add(neighbor);
							}
						}
					}
					
					for ( int label : labels)
					{
						int[] NL_array = new int[NL.get(label).size()];
						int i = 0;
						Iterator<Integer> iterator = NL.get(label).iterator();
						while(iterator.hasNext())
						{
							NL_array[i] = iterator.next();
							i++;
						}
						String property_name = String.format("NL_%d_%d_list", hop_num, label);
						node.setProperty(property_name, NL_array);
					}
					
				}
				
				cur_level_nodes = next_level_nodes;
				next_level_nodes = new HashSet<Node>();
			}
			tx.success();
			tx.close();
			dbservice.shutdown();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public static void ReplaceNL()
	{
		String clear_property_name = "NL_1_list";
		String replace_property_name = "NL_1_0_list";
		try {
			GraphDatabaseService dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
			Transaction tx = dbservice.beginTx();
			
			Node rtree_root_node = OSM_Utility.getRTreeRoot(dbservice, dataset);
			
			Queue<Node> queue = new LinkedList<Node>();
			queue.add(rtree_root_node);
			while ( queue.isEmpty() == false)
			{
				Node node = queue.poll();
				if(node.hasProperty(clear_property_name))
				{
					int[] property = (int[]) node.removeProperty(clear_property_name);
					node.setProperty(replace_property_name, property);
				}
				Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD);
				for (Relationship rel : rels)
				{
					Node neighbor = rel.getEndNode();
					queue.add(neighbor);
				}
			}
			
			tx.success();
			tx.close();
			dbservice.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void ClearNL()
	{
		String clear_property_name = "NL_1_list";
		try {
			GraphDatabaseService dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
			Transaction tx = dbservice.beginTx();
			
			Node rtree_root_node = OSM_Utility.getRTreeRoot(dbservice, dataset);
			
			Queue<Node> queue = new LinkedList<Node>();
			queue.add(rtree_root_node);
			while ( queue.isEmpty() == false)
			{
				Node node = queue.poll();
				if(node.hasProperty(clear_property_name))
				{
					int[] property = (int[]) node.removeProperty(clear_property_name);
					OwnMethods.Print(property.toString());
				}
				Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING, RTreeRelationshipTypes.RTREE_CHILD);
				for (Relationship rel : rels)
				{
					Node neighbor = rel.getEndNode();
					queue.add(neighbor);
				}
			}
			
			tx.success();
			tx.close();
			dbservice.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
