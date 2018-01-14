package commons;

import java.io.File;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import commons.Config.system;

public class RTreeUtilityTest {
	
	static Config config = new Config();
	static String dataset = config.getDatasetName();
	static String version = config.GetNeo4jVersion();
	static system systemName = config.getSystemName();
	static int MAX_HOPNUM = config.getMaxHopNum();
	
	static String db_path, graph_pos_map_path;
	static String querygraphDir, spaPredicateDir;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		switch (systemName) {
		case Ubuntu:
			db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
			graph_pos_map_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/node_map_RTree.txt";
			querygraphDir = String.format("/mnt/hgfs/Google_Drive/Projects/risotree/query/query_graph/%s", dataset);
			spaPredicateDir = String.format("/mnt/hgfs/Google_Drive/Projects/risotree/query/spa_predicate/%s", dataset);
			break;
		case Windows:
			String dataDirectory = "D:\\Ubuntu_shared\\GeoMinHop\\data";
			db_path = String.format("%s\\%s\\%s_%s\\data\\databases\\graph.db", dataDirectory, dataset, version, dataset);
			graph_pos_map_path = "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset + "\\node_map_RTree.txt";
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
	public void getRootTest()
	{
		OwnMethods.Print(db_path);
		GraphDatabaseService databaseService = new GraphDatabaseFactory()
				.newEmbeddedDatabase(new File(db_path));
		Transaction tx = databaseService.beginTx();
		String layer_name = "Patents_100_random_80";
		OwnMethods.Print(layer_name);
		Node node = RTreeUtility.getRTreeRoot(databaseService, layer_name);
		OwnMethods.Print(node);
		
//		ResourceIterable<Label> labels = databaseService.getAllLabels();
//		for ( Label label : labels)
//		{
//			OwnMethods.Print(label);
//		}
		
		tx.success();tx.close();
		databaseService.shutdown();
	}
	
	@Test
	public void getHeightTest()
	{
		OwnMethods.Print(db_path);
		GraphDatabaseService databaseService = new GraphDatabaseFactory()
				.newEmbeddedDatabase(new File(db_path));
		Transaction tx = databaseService.beginTx();
		Node node = RTreeUtility.getRTreeRoot(databaseService, dataset);
		
//		Node node = databaseService.getNodeById(758295);
		
		OwnMethods.Print(RTreeUtility.getHeight(node));
		
		OwnMethods.Print(node.getAllProperties());
		
		tx.success();
		tx.close();
		databaseService.shutdown();
	}
}
