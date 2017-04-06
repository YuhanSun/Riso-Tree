package graph;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import commons.Config;
import commons.Labels;
import commons.OwnMethods;
import commons.Labels.RTreeRel;
import osm.OSM_Utility;

public class Construct_RisoTree {

	static int max_hop = 1;
	static String dataset = "Gowalla";
	static Config config = new Config();
	static String version = config.GetNeo4jVersion();
	static String db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
	static String vertex_map_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map.txt", dataset);
	static String graph_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
	static String geo_id_map_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/geom_osmid_map.txt", dataset);
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Construct();
	}

	public static void Construct()
	{
		try {
			ArrayList<ArrayList<Integer>> graph = OwnMethods.ReadGraph(graph_path);
			Map<String, String> map_str = OwnMethods.ReadMap(geo_id_map_path );
			Map<Long, Long> map = new HashMap<>();
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
				TreeSet<Integer> NL = new TreeSet<>();
				Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING, Labels.RTreeRel.RTREE_REFERENCE);
				for ( Relationship rel : rels)
				{
					Node child  = rel.getEndNode();
					long pos_id = child.getId();
					long graph_id = map.get(pos_id);
					if(graph_id > Integer.MAX_VALUE)
						throw new Exception("graph id out of int range!");
					int id = (int) graph_id;
					for (int neighbor : graph.get(id))
						NL.add(neighbor);
				}
				int[] NL_array = new int[NL.size()];
				int i = 0;
				Iterator<Integer> iterator = NL.iterator();
				while(iterator.hasNext())
				{
					NL_array[i] = iterator.next();
					i++;
				}
				node.setProperty("NL_1_list", NL_array);
			}
			
			Set<Node> next_level_nodes = new HashSet<>();
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
					TreeSet<Integer> NL = new TreeSet<>();
					Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING, Labels.RTreeRel.RTREE_CHILD);
					for ( Relationship rel : rels)
					{
						Node child  = rel.getEndNode();
						int[] child_NL_list = (int[]) child.getProperty("NL_1_list");
						for ( int neighbor : child_NL_list)
							NL.add(neighbor);
					}
					int[] NL_array = new int[NL.size()];
					int i = 0;
					Iterator<Integer> iterator = NL.iterator();
					while(iterator.hasNext())
					{
						NL_array[i] = iterator.next();
						i++;
					}
					node.setProperty("NL_1_list", NL_array);
				}
				
				cur_level_nodes = next_level_nodes;
				next_level_nodes = new HashSet<Node>();
			}
			tx.success();
			tx.close();
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
	}
	
}
