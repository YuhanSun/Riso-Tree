package commons;

import java.io.File;

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.ExecutionPlan;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class App {

	static String dataset = "Gowalla";
	static Config config = new Config();
	static String version = config.GetNeo4jVersion();
//	static String db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
	static String db_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset, version, dataset);
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
		try {
			Transaction tx = databaseService.beginTx();
//			Node node =  databaseService.getNodeById(3854497);
//			OwnMethods.Print(node.getAllProperties());
//			double [] bbox = (double []) node.getProperty("bbox");
//			for ( double element : bbox)
//				OwnMethods.Print(element);
			
//			String query = "explain match (a0:GRAPH_0)--(a1:GRAPH_1) where id(a0) = 100 or id(a0) = 99 return id(a0), id(a1)";
			String query = "explain match (a0:GRAPH_0)--(a1:GRAPH_1) where id(a0) in [100, 99] return id(a0), id(a1)";
			Result result = databaseService.execute(query);
			ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
			OwnMethods.Print(planDescription);
			
			tx.success();
			tx.close();
			databaseService.shutdown();
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
