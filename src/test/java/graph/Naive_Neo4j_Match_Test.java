package graph;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Result;

import commons.Config;
import commons.MyRectangle;
import commons.OwnMethods;
import commons.Config.system;
import commons.Query_Graph;
import commons.Utility;

public class Naive_Neo4j_Match_Test {

	static Config config = new Config();
	static String dataset = config.getDatasetName();
	static String version = config.GetNeo4jVersion();
	static system systemName = config.getSystemName();

	static String db_path;
	static String querygraphDir, spaPredicateDir;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		switch (systemName) {
		case Ubuntu:
			db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
			querygraphDir = String.format("/mnt/hgfs/Google_Drive/Projects/risotree/query/query_graph/%s", dataset);
			spaPredicateDir = String.format("/mnt/hgfs/Google_Drive/Projects/risotree/query/spa_predicate/%s", dataset);
			break;
		case Windows:
			String dataDirectory = "D:\\Ubuntu_shared\\GeoMinHop\\data";
			db_path = String.format("%s\\%s\\%s_%s\\data\\databases\\graph.db", dataDirectory, dataset, version, dataset);
			querygraphDir = String.format("D:\\Google_Drive\\Projects\\risotree\\query\\query_graph\\%s", dataset);
			spaPredicateDir = String.format("D:\\Google_Drive\\Projects\\risotree\\query\\spa_predicate\\%s", dataset);
		default:
			break;
		}
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void queryTest() {
		int nodeCount = 2, query_id = 0;
		int name_suffix = 75;
		
		String queryrect_path = null, querygraph_path = null;
		switch (systemName) {
		case Ubuntu:
			querygraph_path = String.format("%s/%d.txt", querygraphDir, nodeCount);
			queryrect_path = String.format("%s/queryrect_%d.txt", spaPredicateDir, name_suffix);
			break;
		case Windows:
			querygraph_path = String.format("%s\\%d.txt", querygraphDir, nodeCount);
			queryrect_path = String.format("%s\\queryrect_%d.txt", spaPredicateDir, name_suffix);
			break;
		}
		
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
		Query_Graph query_Graph = queryGraphs.get(query_id);
		
		ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
		MyRectangle rectangle = queryrect.get(0);
		int j = 0;
		for (  ; j < query_Graph.graph.size(); j++)
			if(query_Graph.Has_Spa_Predicate[j])
				break;
		query_Graph.spa_predicate[j] = rectangle;
		
		Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match(db_path);
		Result result = naive_Neo4j_Match.SubgraphMatch_Spa_API(query_Graph, -1);
		while (result.hasNext())
			OwnMethods.Print(result.next());
	}
	
	@Test
	public void graphOnlyQueryTest() {
		int nodeCount = 5, query_id = 0;
		
		String querygraph_path = "/mnt/hgfs/Google_Drive/Projects/risotree/query/query_graph/labelCount/Gowalla_25/5.txt";
		
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
		Query_Graph query_Graph = queryGraphs.get(query_id);
		
		int j = 0;
		for (  ; j < query_Graph.graph.size(); j++)
			if(query_Graph.Has_Spa_Predicate[j])
				break;
		query_Graph.spa_predicate[j] = null;
		query_Graph.Has_Spa_Predicate[j] = false;
		
		Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match(db_path);
		Result result = naive_Neo4j_Match.SubgraphMatch_Spa_API(query_Graph, 100);
		while (result.hasNext())
			OwnMethods.Print(result.next());
		OwnMethods.Print(result.getExecutionPlanDescription());
	}
	

}