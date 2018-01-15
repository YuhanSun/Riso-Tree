package graph;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

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
import commons.Config.Explain_Or_Profile;
import commons.Config.system;
import commons.Entity;

public class RisoTreeQueryPNTest {

	static Config config = new Config();
	static String dataset = config.getDatasetName();
	static String version = config.GetNeo4jVersion();
	static system systemName = config.getSystemName();
	static int MAX_HOPNUM = config.getMaxHopNum();

	static String db_path, entityPath, graph_pos_map_path;
	static String querygraphDir, spaPredicateDir;
	
	//query input
	static Query_Graph query_Graph;
	static long[] graph_pos_map_list;
	
	static int nodeCount = 5, query_id = 0;
	
//	name_suffix = 7;
//	name_suffix = 75;
//	name_suffix = 7549;//0.1
	static int name_suffix = 1280;//Gowalla 0.001
	static String queryrect_path = null, querygraph_path = null, queryrectCenterPath = null;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		switch (systemName) {
		case Ubuntu:
			db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
			entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
			graph_pos_map_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/node_map_RTree.txt";
			querygraphDir = String.format("/mnt/hgfs/Google_Drive/Projects/risotree/query/query_graph/%s", dataset);
			spaPredicateDir = String.format("/mnt/hgfs/Google_Drive/Projects/risotree/query/spa_predicate/%s", dataset);
			querygraph_path = String.format("%s/%d.txt", querygraphDir, nodeCount);
			queryrectCenterPath = String.format("%s/%s_centerids.txt", spaPredicateDir, dataset);
			queryrect_path = String.format("%s/queryrect_%d.txt", spaPredicateDir, name_suffix);
			break;
		case Windows:
			String dataDirectory = "D:\\Ubuntu_shared\\GeoMinHop\\data";
			db_path = String.format("%s\\%s\\%s_%s\\data\\databases\\graph.db", dataDirectory, dataset, version, dataset);
			entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
			graph_pos_map_path = "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset + "\\node_map_RTree.txt";
			querygraphDir = String.format("D:\\Google_Drive\\Projects\\risotree\\query\\query_graph\\%s", dataset);
			spaPredicateDir = String.format("D:\\Google_Drive\\Projects\\risotree\\query\\spa_predicate\\%s", dataset);
			querygraph_path = String.format("%s\\%d.txt", querygraphDir, nodeCount);
			queryrectCenterPath = String.format("%s\\%s_centerids.txt", spaPredicateDir, dataset);
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
		
//		ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
		
		ArrayList<Integer> centerIDs = OwnMethods.readIntegerArray(queryrectCenterPath);
		ArrayList<Entity> entities = OwnMethods.ReadEntity(entityPath);
		ArrayList<MyRectangle> queryrect = new ArrayList<MyRectangle>();
		for ( int id : centerIDs)
		{
			Entity entity = entities.get(id);
			queryrect.add(new MyRectangle(entity.lon, entity.lat, entity.lon, entity.lat));
		}
		
		MyRectangle rectangle = queryrect.get(0);
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
		
		int K = 5;
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
	
	@Test
	public void formQueryLAGAQ_JoinTest()
	{
		query_Graph = new Query_Graph(3);
		query_Graph.label_list[0] = 3;
		query_Graph.label_list[1] = 1;
		query_Graph.label_list[2] = 1;
		
		query_Graph.graph.get(0).add(1);
		query_Graph.graph.get(0).add(2);
		query_Graph.graph.get(1).add(0);
		query_Graph.graph.get(2).add(0);
		
		query_Graph.Has_Spa_Predicate[1] = true;
		query_Graph.Has_Spa_Predicate[2] = true;
		
		ArrayList<Integer> pos = new ArrayList<Integer>();
		pos.add(1);
		pos.add(2);
		
		Long[] idPair = new Long[2];
		idPair[0] = (long) 100;
		idPair[1] = (long) 200;
		
		String query = RisoTreeQueryPN.formQueryLAGAQ_Join(query_Graph, pos, idPair, 1, Explain_Or_Profile.Profile);
		OwnMethods.Print(query);
	}
	
//	@Test
//	public void spatialJoinRTreeTest() {
//		RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(db_path, dataset, graph_pos_map_list, MAX_HOPNUM);
//		try{
//			long start = System.currentTimeMillis();
//			List<Long[]> result = risoTreeQueryPN.spatialJoinRTree(0.01, pos, spaPathsMap);
//			OwnMethods.Print(System.currentTimeMillis() - start);
//		}
//		catch(Exception e)
//		{
//			e.printStackTrace();
//			System.exit(-1);
//		}
//	}

	@Test
	public void spatialJoinRTreeTest()
	{
		try {
			double distance = 0.01;
			OwnMethods.Print(distance);
//			FileWriter writer = new FileWriter("D:\\temp\\output2.txt");
			OwnMethods.ClearCache("syh19910205");
			RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(db_path, dataset, graph_pos_map_list, MAX_HOPNUM);
			long start = System.currentTimeMillis();
			List<Long[]> result = risoTreeQueryPN.spatialJoinRTree(distance, null, null);
			long time =  System.currentTimeMillis() - start;
			OwnMethods.Print(time);
			OwnMethods.Print(result.size());
			risoTreeQueryPN.dbservice.shutdown();
//			for (Long[] element : result)
//			{
//				writer.write(String.format("%d,%d\n", element[0], element[1]));
//			}
//			writer.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	@Test
	public void constructPNTest()
	{
		OwnMethods.convertQueryGraphForJoin(query_Graph);
		OwnMethods.Print(query_Graph.toString());
		RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(db_path, dataset, graph_pos_map_list, MAX_HOPNUM);
		GraphDatabaseService databaseService = risoTreeQueryPN.dbservice;
		Transaction tx = databaseService.beginTx();
		Node node = databaseService.getNodeById(758387);
//		OwnMethods.Print(node.getPropertyKeys());
		
		HashMap<Integer, HashMap<Integer, HashSet<String>>> spaPathsMap = risoTreeQueryPN.recognizePaths(query_Graph);
		HashMap<Integer, HashSet<String>> paths = spaPathsMap.get(2);
		ArrayList<Integer> overlapVertices = new ArrayList<>();
		overlapVertices.add(1);
		overlapVertices.add(4);
		HashMap<Integer, ArrayList<Integer>> res = risoTreeQueryPN.constructPN(node, paths, overlapVertices);
		OwnMethods.Print(res);
		tx.success();
		tx.close();
	}
	
	@Test
	public void isIntersectTest()
	{
		long start = System.currentTimeMillis();
		ArrayList<Integer> overlapVertices = new ArrayList<>(
				Arrays.asList(0, 2));
		
		HashMap<Integer, ArrayList<Integer>> pnListLeft = new HashMap<>();
		pnListLeft.put(0, new ArrayList<Integer>(Arrays.asList(0, 1, 2)));
		pnListLeft.put(2, new ArrayList<Integer>(Arrays.asList(0, 1, 2)));
		
		HashMap<Integer, ArrayList<Integer>> pnListRight = new HashMap<>();
		pnListRight.put(0, new ArrayList<Integer>(Arrays.asList(0, 1, 2)));
		pnListRight.put(2, new ArrayList<Integer>(Arrays.asList()));
		OwnMethods.Print(System.currentTimeMillis() - start);
		
		start = System.currentTimeMillis();
		OwnMethods.Print(RisoTreeQueryPN.isIntersect(overlapVertices, pnListLeft, pnListRight));
		OwnMethods.Print(System.currentTimeMillis() - start);
	}
	
	@Test
	public void LAGAQ_JoinTest()
	{
//		query_Graph = new Query_Graph(3);
//		query_Graph.label_list[0] = 3;
//		query_Graph.label_list[1] = 1;
//		query_Graph.label_list[2] = 1;
//		
//		query_Graph.graph.get(0).add(1);
//		query_Graph.graph.get(0).add(2);
//		query_Graph.graph.get(1).add(0);
//		query_Graph.graph.get(2).add(0);
//		
//		query_Graph.Has_Spa_Predicate[1] = true;
//		query_Graph.Has_Spa_Predicate[2] = true;
		
		OwnMethods.ClearCache("syh19910205");
		
		OwnMethods.convertQueryGraphForJoin(query_Graph);
		OwnMethods.Print(query_Graph.toString());
		
		RisoTreeQueryPN risoTreeQueryPN = new RisoTreeQueryPN(db_path, dataset, graph_pos_map_list, MAX_HOPNUM);
		long start = System.currentTimeMillis();
		List<Long[]> result = risoTreeQueryPN.LAGAQ_Join(query_Graph, 0.1);
		OwnMethods.Print(String.format("Total time: %d", System.currentTimeMillis() - start));
		
		OwnMethods.Print("Join time: " + risoTreeQueryPN.join_time);
		OwnMethods.Print("check paths time: " + risoTreeQueryPN.check_paths_time);
		OwnMethods.Print("has relation time: " + risoTreeQueryPN.has_relation_time);
		OwnMethods.Print("has relation addition time: " + risoTreeQueryPN.has_relation_time_addition);
		
		OwnMethods.Print("Get iterate time: " + risoTreeQueryPN.get_iterator_time); 
		OwnMethods.Print("Iterate time: " + risoTreeQueryPN.iterate_time);
		OwnMethods.Print("Join result count: " + risoTreeQueryPN.join_result_count);
		OwnMethods.Print(result);
		OwnMethods.Print(result.size());
		
		risoTreeQueryPN.dbservice.shutdown();
	}
}
