package graph;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import commons.Config;
import commons.MyRectangle;
import commons.OwnMethods;
import commons.Query_Graph;
import commons.Utility;
import commons.Config.system;

public class RisoTreeQueryPNTest {

	static Config config = new Config();
	static String dataset = config.getDatasetName();
	static String version = config.GetNeo4jVersion();
	static system systemName = config.getSystemName();
	static int MAX_HOPNUM = config.getMaxHopNum();

	static String db_path, graph_pos_map_path;
	static String querygraphDir, spaPredicateDir;
	
	//query input
	static Query_Graph query_Graph;
	static long[] graph_pos_map_list;
	
	static int nodeCount = 5, query_id = 1;
	
//	name_suffix = 7;
//	name_suffix = 75;
//	name_suffix = 7549;//0.1
	static int name_suffix = 1280;//Gowalla 0.001
	static String queryrect_path = null, querygraph_path = null;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		switch (systemName) {
		case Ubuntu:
			db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
			graph_pos_map_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/node_map_RTree.txt";
			querygraphDir = String.format("/mnt/hgfs/Google_Drive/Projects/risotree/query/query_graph/%s", dataset);
			spaPredicateDir = String.format("/mnt/hgfs/Google_Drive/Projects/risotree/query/spa_predicate/%s", dataset);
			querygraph_path = String.format("%s/%d.txt", querygraphDir, nodeCount);
			queryrect_path = String.format("%s/queryrect_%d.txt", spaPredicateDir, name_suffix);
			break;
		case Windows:
			String dataDirectory = "D:\\Ubuntu_shared\\GeoMinHop\\data";
			db_path = String.format("%s\\%s\\%s_%s\\data\\databases\\graph.db", dataDirectory, dataset, version, dataset);
			graph_pos_map_path = "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset + "\\node_map_RTree.txt";
			querygraphDir = String.format("D:\\Google_Drive\\Projects\\risotree\\query\\query_graph\\%s", dataset);
			spaPredicateDir = String.format("D:\\Google_Drive\\Projects\\risotree\\query\\spa_predicate\\%s", dataset);
			querygraph_path = String.format("%s\\%d.txt", querygraphDir, nodeCount);
			queryrect_path = String.format("%s\\queryrect_%d.txt", spaPredicateDir, name_suffix);
		default:
			break;
		}
		iniQueryInput();
	}
	
	public static void iniQueryInput()
	{
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
		query_Graph = queryGraphs.get(query_id);
		
		ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
		MyRectangle rectangle = queryrect.get(1);
		int j = 0;
		for (  ; j < query_Graph.graph.size(); j++)
			if(query_Graph.Has_Spa_Predicate[j])
				break;
		query_Graph.spa_predicate[j] = rectangle;
		
		HashMap<String, String> graph_pos_map = OwnMethods.ReadMap(graph_pos_map_path);
		graph_pos_map_list= new long[graph_pos_map.size()];
		for ( String key_str : graph_pos_map.keySet())
		{
			int key = Integer.parseInt(key_str);
			int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
			graph_pos_map_list[key] = pos_id;
		}
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}
	
	@Test
	public void recognizePathsTest()
	{
		RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(db_path, dataset, null, MAX_HOPNUM);
		HashMap<Integer, HashMap<Integer, HashSet<String>>> paths = risoTreeQueryPN.recognizePaths(query_Graph);
		for ( int key : paths.keySet())
			OwnMethods.Print(String.format("%d:%s", key, paths.get(key)));
		
		risoTreeQueryPN.dbservice.shutdown();
	}
	
	@Test
	public void queryTest() {
		RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(db_path, dataset, 
				graph_pos_map_list, MAX_HOPNUM);
		risoTreeQueryPN.Query(query_Graph, -1);
		OwnMethods.Print("Result count:" + risoTreeQueryPN.result_count);
		OwnMethods.Print("Page access:" + risoTreeQueryPN.page_hit_count);
		OwnMethods.Print("get iterator time:" + risoTreeQueryPN.get_iterator_time);
		OwnMethods.Print("iterate time:" + risoTreeQueryPN.iterate_time);
		
		risoTreeQueryPN.dbservice.shutdown();
	}
	
	@Test
	public void queryKNNtest()
	{
		RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(db_path, dataset, 
				graph_pos_map_list, MAX_HOPNUM);
		
		int K = 10;
		try
		{
			ArrayList<Long> resultIDs = risoTreeQueryPN.LAGAQ_KNN(query_Graph, K);
			OwnMethods.Print(resultIDs);
			OwnMethods.Print(risoTreeQueryPN.visit_spatial_object_count);
			OwnMethods.Print(risoTreeQueryPN.page_hit_count);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			risoTreeQueryPN.dbservice.shutdown();
		}
		finally
		{
			risoTreeQueryPN.dbservice.shutdown();
		}
	}
	
	@Test
	public void checkPathsTest()
	{
		GraphDatabaseService databaseService = new GraphDatabaseFactory()
				.newEmbeddedDatabase(new File(db_path));
		Transaction tx = databaseService.beginTx();
		Node node = databaseService.getNodeById(1294918);
		LinkedList<String> paths = new LinkedList<String>();
		paths.add("PN_27_1000");
		boolean result = RisoTreeQueryPN.checkPaths(node, paths);
		OwnMethods.Print(result);
	}

}
