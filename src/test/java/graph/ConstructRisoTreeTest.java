package graph;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.sound.sampled.Line;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Str;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import commons.Config;
import commons.OwnMethods;
import osm.OSM_Utility;
import commons.Config.system;
import commons.Labels;
import commons.Labels.OSMLabel;
import commons.Labels.RTreeRel;

public class ConstructRisoTreeTest {

	static Config config = new Config();
	static String dataset = config.getDatasetName();
	static String version = config.GetNeo4jVersion();
	static system systemName = config.getSystemName();

	static String db_path;
	static String containIDPath;
	static GraphDatabaseService databaseService;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		switch (systemName) {
		case Ubuntu:
			db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
			containIDPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/containID.txt", dataset);
			break;
		case Windows:
			String dataDirectory = "D:\\Ubuntu_shared\\GeoMinHop\\data";
			db_path = String.format("%s\\%s\\%s_%s\\data\\databases\\graph.db", dataDirectory, dataset, version, dataset);
			containIDPath= String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\containID.txt", dataset);
		default:
			break;
		}
		
		databaseService = new GraphDatabaseFactory().
				newEmbeddedDatabase(new File(db_path));
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		databaseService.shutdown();
	}
	
	@Test
	public void nonLeafContentTest()
	{
		Transaction tx = databaseService.beginTx();
		ResourceIterator<Node> nodes = databaseService.findNodes(Labels.GraphLabel.GRAPH_1);
		while ( nodes.hasNext())
		{
			Node node =  nodes.next();
			Node parent = node.getSingleRelationship
					(RTreeRel.RTREE_REFERENCE, Direction.INCOMING).
					getStartNode();
			OwnMethods.Print(parent.getAllProperties());
			break;
		}
		tx.success();
		tx.close();
	}

	@Test
	public void readContainIDMapTest()
	{
		HashMap<Long, ArrayList<Integer>> containIDMap = Construct_RisoTree.readContainIDMap(containIDPath);
		for (long key : containIDMap.keySet())
		{
			OwnMethods.Print(String.format("%d:%s", key, containIDMap.get(key).toString()));
			break;
		}
	}
	
	@Test
	public void constructPNTest()
	{
		long nodeID = 754959;
		GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
		Transaction tx = databaseService.beginTx();
		Node node = databaseService.getNodeById(nodeID);
		Map<String, Object> properties = node.getAllProperties();
		for ( String key : properties.keySet())
		{
			if ( key.equals("count")|| key.equals("bbox"))
				OwnMethods.Print(key);
			
//			if ( key.contains("PN"))
//			{
//				String line = String.format("%s:", key);
//				int[] property = (int[]) properties.get(key);
//				line += "[";
//				for ( int id : property)
//					line += String.format("%d, ", id);
//				line = line.substring(0, line.length() - 2);
//				line += "]";
//				OwnMethods.Print(line);
//			}
		}
		
		tx.success();
		tx.close();
		databaseService.shutdown();
	}

	@Test
	public void constructNLTest()
	{
		long nodeID = 754959;
		GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
		Transaction tx = databaseService.beginTx();
		
		Node node = databaseService.getNodeById(nodeID);
		Map<String, Object> properties = node.getAllProperties();
		
//		Set<Node> nodes = OSM_Utility.getRTreeNonleafDeepestLevelNodes(databaseService, dataset);
//		Node node = nodes.iterator().next();
//		Map<String, Object> properties = node.getAllProperties();
		
		int count = 0;
		for ( String key : properties.keySet())
		{
			if ( key.contains("list"))
			{
				count++;
				OwnMethods.Print(count);
				String line = String.format("%s:", key);
				int[] property = (int[]) properties.get(key);
				line += "[";
				for ( int id : property)
					line += String.format("%d, ", id);
				line = line.substring(0, line.length() - 2);
				line += "]";
				OwnMethods.Print(line);
			}
//			OwnMethods.Print(key);
		}
//		OwnMethods.Print(properties);
		
		
		tx.success();
		tx.close();
		databaseService.shutdown();
	}
}
