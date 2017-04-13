package commons;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class App {

	static String dataset = "Gowalla";
	static Config config = new Config();
	static String version = config.GetNeo4jVersion();
	static String db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
		try {
			Transaction tx = databaseService.beginTx();
			Node node =  databaseService.getNodeById(3854497);
			OwnMethods.Print(node.getAllProperties());
			double [] bbox = (double []) node.getProperty("bbox");
			for ( double element : bbox)
				OwnMethods.Print(element);
			
			
			tx.success();
			tx.close();
			databaseService.shutdown();
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

}
