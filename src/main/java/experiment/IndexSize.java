package experiment;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import commons.Config;
import commons.Labels;
import commons.Config.system;
import osm.OSM_Utility;
import commons.OwnMethods;

public class IndexSize {

	static Config config = new Config();
	static String dataset = config.getDatasetName();
	static String version = config.GetNeo4jVersion();
	static system systemName = config.getSystemName();
	static int MAX_HOPNUM = config.getMaxHopNum();
	static int nonspatial_label_count = config.getNonSpatialLabelCount();
	
	static String db_path;
	static String graphPath;
	static ArrayList<Integer> labels;//all labels in the graph
	
	public static void initializeParameters()
	{	
		switch (systemName) {
		case Ubuntu:
			db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
			graphPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
			break;
		case Windows:
			db_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset, version, dataset);
			graphPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\graph.txt", dataset);
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
		initializeParameters();
//		calculateIndexSize();
//		calculateValidIndexSize();
		graphSize();
	}

	public static void graphSize()
	{
		try {
			ArrayList<ArrayList<Integer>> graph = OwnMethods.ReadGraph(graphPath);
			OwnMethods.Print(OwnMethods.getGraphSize(graph));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void calculateIndexSize()
	{
		try {
			int size = 0;
			int visitedNodeCount = 0;
			OwnMethods.Print(db_path);
			GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
			Transaction tx = databaseService.beginTx();
			
			Set<Node> nodes = OSM_Utility.getRTreeNonleafDeepestLevelNodes(databaseService, dataset);
			for ( Node node: nodes)
			{
				visitedNodeCount++;
				for ( int i = 0; i < labels.size(); i++)
					{
						for ( int j = 1; j <= MAX_HOPNUM; j++)
						{
							String NL_size_propertyname = String.format("NL_%d_%d_size", j, labels.get(i));
							if ( node.hasProperty(NL_size_propertyname))
							{
								int NL_size = (Integer) node.getProperty(NL_size_propertyname);
								size += NL_size;
							}
							else
								OwnMethods.Print(String.format("Not has %s", NL_size_propertyname));
						}
					}
			}
			
//			Node root = OSM_Utility.getRTreeRoot(databaseService, dataset);
//			Queue<Node> queue = new LinkedList<Node>();
//			queue.add(root);
//			while ( queue.isEmpty() == false)
//			{
//				Node node = queue.poll();
//				visitedNodeCount++;
//				for ( int i = 2; i < labels.size(); i++)
//				{
//					for ( int j = 1; j <= max_hop_num; j++)
//					{
//						String NL_size_propertyname = String.format("NL_%d_%d_size", j, i);
//						if ( node.hasProperty(NL_size_propertyname))
//						{
//							int NL_size = (Integer) node.getProperty(NL_size_propertyname);
//							size += NL_size;
//						}
//					}
//				}
//				Iterable<Relationship> rels = node.getRelationships(Direction.OUTGOING, Labels.RTreeRel.RTREE_CHILD);
//				for ( Relationship relationship : rels)
//				{
//					Node next = relationship.getEndNode();
//					queue.add(next);
//				}
//			}
			
			tx.success();
			tx.close();
			databaseService.shutdown();
			
			OwnMethods.Print(String.format("visited node count: %d", visitedNodeCount));
			OwnMethods.Print(String.format("index size:%d", size * 4));
			
		} catch (Exception e) {
			e.printStackTrace();	System.exit(-1);
		}
	}
	
	public static void calculateValidIndexSize()
	{
		try {
			int size = 0;
			int visitedNodeCount = 0;
			OwnMethods.Print(db_path);
			GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
			Transaction tx = databaseService.beginTx();
			
			Set<Node> nodes = OSM_Utility.getRTreeNonleafDeepestLevelNodes(databaseService, dataset);
			for ( Node node: nodes)
			{
				visitedNodeCount++;
				
				int spa_count = (Integer) node.getProperty("count");
				for ( int i = 2; i < labels.size(); i++)
					{
						for ( int j = 1; j <= MAX_HOPNUM; j++)
						{
							String NL_size_propertyname = String.format("NL_%d_%d_size", j, i);
							if ( node.hasProperty(NL_size_propertyname))
							{
								int NL_size = (Integer) node.getProperty(NL_size_propertyname);
								if ( NL_size < spa_count)
									size += NL_size;
							}
						}
					}
			}
			
			tx.success();
			tx.close();
			databaseService.shutdown();
			
			OwnMethods.Print(String.format("visited node count: %d", visitedNodeCount));
			OwnMethods.Print(String.format("index size:%d", size));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
