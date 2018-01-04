package graph;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import commons.Config;
import commons.Entity;
import commons.MyRectangle;
import commons.OwnMethods;
import commons.Query_Graph;
import commons.RTreeUtility;
import commons.Utility;
import commons.Config.Explain_Or_Profile;
import commons.Config.system;
import commons.Labels.OSMRelation;

public class SpatialFirst_ListTest {

	static Config config = new Config();
	static String dataset = config.getDatasetName();
	static String version = config.GetNeo4jVersion();
	static system systemName = config.getSystemName();
	static int MAX_HOPNUM = config.getMaxHopNum();

	static String db_path, entityPath, graph_pos_map_path;
	static String querygraphDir, spaPredicateDir;
	static String log_path;
	
	//query input
	static Query_Graph query_Graph;
	static long[] graph_pos_map_list;
	
	static int nodeCount = 3, query_id = 3;
	
	static int name_suffix = 1280;//Gowalla 0.001
	static String queryrect_path = null, querygraph_path = null, queryrectCenterPath = null;
	
	@Before
	public void setUp() throws Exception {
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

	@After
	public void tearDown() throws Exception {
	}
	
	public void iniQueryInput()
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
	
	public static void subgraphMatchQuery_Block_Test()
	{
		SpatialFirst_List spatialFirstlist = new SpatialFirst_List(db_path, dataset, graph_pos_map_list);
		
		spatialFirstlist.query_Block(query_Graph, -1);
		OwnMethods.Print(String.format("result size: %d", spatialFirstlist.result_count));
		spatialFirstlist.shutdown();
	}
	
	@Test
	public void subgraphMatchQueryTest()
	{
		SpatialFirst_List spatialFirstlist = new SpatialFirst_List(db_path, dataset, graph_pos_map_list);

		spatialFirstlist.query(query_Graph, -1);
		OwnMethods.Print(String.format("result size: %d", spatialFirstlist.result_count));
		spatialFirstlist.shutdown();
	}
	
	@Test
	public void rangeQueryTest()
	{
		try	{
			SpatialFirst_List spatialFirstlist = new SpatialFirst_List(db_path, dataset, graph_pos_map_list);
			Transaction tx = spatialFirstlist.dbservice.beginTx();
			Node rootNode = RTreeUtility.getRTreeRoot(spatialFirstlist.dbservice, dataset);
			MyRectangle query_rectangle = new MyRectangle(-98.025157, 29.953977, -97.641747, 30.337387);
			LinkedList<Node> result = spatialFirstlist.rangeQuery(rootNode, query_rectangle);
			OwnMethods.Print(String.format("Result size: %d", result.size()));
			tx.success();
			tx.close();
			spatialFirstlist.dbservice.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void formSubgraphQueryTest() throws Exception
	{
		try {
			SpatialFirst_List spatialFirstlist = new SpatialFirst_List(db_path, dataset, graph_pos_map_list);

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

	@Test
	public void LAGAQ_KNNtest() {
		SpatialFirst_List spatialFirst_List = new SpatialFirst_List(db_path, dataset, graph_pos_map_list);
		int K = 5;
		try
		{
			ArrayList<Long> resultIDs = spatialFirst_List.LAGAQ_KNN(query_Graph, K);
			OwnMethods.Print(resultIDs);
			OwnMethods.Print(spatialFirst_List.visit_spatial_object_count);
			OwnMethods.Print(spatialFirst_List.page_hit_count);
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
}
