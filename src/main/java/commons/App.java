package commons;

import java.io.File;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.ExecutionPlan;
import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SimplePointLayer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.filter.SearchIntersectWindow;
import org.neo4j.gis.spatial.rtree.RTreeIndex;
import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.gis.spatial.rtree.SpatialIndexReader;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.api.cursor.TxAbstractNodeCursor;
import org.neo4j.register.Register.Int;

import com.vividsolutions.jts.geom.Envelope;

import commons.Config.system;
import commons.Labels.GraphLabel;
import commons.Labels.GraphRel;
import commons.Labels.OSMRelation;
import graph.RisoTreeQuery;
import graph.SpatialFirst;
import osm.OSM_Utility;

public class App {

	static Config config = new Config();
	static String dataset = config.getDatasetName();
	static String lon_name = config.GetLongitudePropertyName();
	static String lat_name = config.GetLatitudePropertyName();

	static String db_path;
	static String querygraph_path;
	static String graph_pos_map_path;
	static String log_path;
	static int query_id;
	static ArrayList<Query_Graph> queryGraphs;
	static Query_Graph query_Graph;
	static MyRectangle queryRectangle;

	public static void initVariablesForTest()
	{
		system systemName = config.getSystemName();
		String neo4jVersion = config.GetNeo4jVersion(); 
		switch (systemName) {
		case Ubuntu:
			db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", neo4jVersion, dataset);
			querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph.txt";
			graph_pos_map_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/node_map_RTree.txt";
			log_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/test.log";
			break;
		case Windows:
			db_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset, neo4jVersion, dataset);
			querygraph_path = "D:\\Ubuntu_shared\\GeoMinHop\\query\\query_graph.txt";
			graph_pos_map_path = "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset + "\\node_map_RTree.txt";
			log_path = "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset + "\\test.log";
		default:
			break;
		}
		query_id = 5;
//		queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
//		query_Graph = queryGraphs.get(query_id);
		queryRectangle = new MyRectangle(-80.115808, 26.187365, -80.115808, 26.187365);//1
//		queryRectangle = new MyRectangle(-80.119514, 26.183659, -80.112102, 26.191071);//10
//		queryRectangle = new MyRectangle(-80.133549, 26.169624, -80.098067, 26.205106);//100
//		queryRectangle = new MyRectangle(-84.468680, 33.879658, -84.428434, 33.919904);//100
//		queryRectangle = new MyRectangle(-80.200353, 26.102820, -80.031263, 26.271910);//1000
		//queryRectangle = new MyRectangle(-98.025157, 29.953977, -97.641747, 30.337387);//10000
		//queryRectangle = new MyRectangle(-91.713778, 14.589395, -68.517838, 37.785335);//100000
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
//		try {
//			Transaction tx = databaseService.beginTx();
////			Node node =  databaseService.getNodeById(3854497);
////			OwnMethods.Print(node.getAllProperties());
////			double [] bbox = (double []) node.getProperty("bbox");
////			for ( double element : bbox)
////				OwnMethods.Print(element);
//			
////			String query = "explain match (a0)--(a1:GRAPH_1) where id(a0) = 0 and id(a1) = 100 or id(a1) = 99 return id(a0), id(a1)";
////			String query = "explain match (a0:GRAPH_0)--(a1:GRAPH_1) where id(a1) = 0 and id(a0) in [100, 99] return id(a0), id(a1)";
//			String query = "profile match (a0),(a1),(a2),(a3:GRAPH_1),(a0)-[:GRAPH_LINK]-(a1),(a0)-[:GRAPH_LINK]-(a2),(a2)-[:GRAPH_LINK]-(a3) "
//					+ "where "
//					+ "( (a0) = 3862270 or id(a0) = 3862405 or id(a0) = 3862972 or id(a0) = 3863090 or id(a0) = 3864161 or id(a0) = 3864483 or id(a0) = 3866373 or id(a0) = 3866930 or id(a0) = 3867109 or id(a0) = 3870775 or id(a0) = 3870782 or id(a0) = 3872073 or id(a0) = 3876108 or id(a0) = 3877181 or id(a0) = 3877687 or id(a0) = 3882003 or id(a0) = 3883648 or id(a0) = 3884513 or id(a0) = 3888202 or id(a0) = 3888550 or id(a0) = 3894723 or id(a0) = 3895932 or id(a0) = 3896578 or id(a0) = 3898198 or id(a0) = 3898199 or id(a0) = 3898206 or id(a0) = 3898207 or id(a0) = 3900017 or id(a0) = 3911759 or id(a0) = 3911761 or id(a0) = 3915250 or id(a0) = 3915309 or id(a0) = 3936035 or id(a0) = 3957555 or id(a0) = 3961782 or id(a0) = 3974508 or id(a0) = 3979574 or id(a0) = 3979635 or id(a0) = 3979636 or id(a0) = 3986952 or id(a0) = 3994423 or id(a0) = 4004213 or id(a0) = 4017232 or id(a0) = 4019530 or id(a0) = 4021875 or id(a0) = 4027265 or id(a0) = 4030003 ) "
//					+ "and "
//					+ "(id(a1)=248969 or id(a1)=49532 or id(a1)=1413284 or id(a1)=3175283 or id(a1)=2889917 or id(a1)=1945319 or id(a1)=1693436 or id(a1)=1025087 or id(a1)=1286387 or id(a1)=803927 or id(a1)=2958857 or id(a1)=1130051 or id(a1)=1776356 or id(a1)=852950 or id(a1)=300623 or id(a1)=323288 or id(a1)=532562 or id(a1)=519407 or id(a1)=452600 or id(a1)=494474 or id(a1)=586472 or id(a1)=614126 or id(a1)=656378 or id(a1)=653168 or id(a1)=2879882 or id(a1)=2880902 or id(a1)=1577153 or id(a1)=2959376 or id(a1)=2882405 or id(a1)=1053287 or id(a1)=2890601 or id(a1)=1817861 or id(a1)=1903928 or id(a1)=1053281 or id(a1)=2797589 or id(a1)=1578443 or id(a1)=2890925 or id(a1)=1421222 or id(a1)=1279592 or id(a1)=1568633 or id(a1)=1106021 or id(a1)=1157006 or id(a1)=2483714 or id(a1)=2229806 or id(a1)=3091283 or id(a1)=2882330 or id(a1)=657512 or id(a1)=1175312 or id(a1)=2882456 or id(a1)=1101722 or id(a1)=2948867 or id(a1)=2428133 or id(a1)=2880275 or id(a1)=2880026 or id(a1)=2784233 or id(a1)=2880440 or id(a1)=2855147 or id(a1)=1344836 or id(a1)=701171 or id(a1)=947672 or id(a1)=1369277 or id(a1)=1190870 or id(a1)=2301707) "
//					+ "return id(a0),id(a1),id(a2),id(a3)";
//			
//			Result result = databaseService.execute(query);
//			while ( result.hasNext())
//				result.next();
//			ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
//			OwnMethods.Print(planDescription);
//			
//			tx.success();
//			tx.close();
//			databaseService.shutdown();
//			
//		} catch (Exception e) {
//			// TODO: handle exception
//			e.printStackTrace();
//		}
//		initVariablesForTest();
//		Naive();
//		test();
//		batchRTreeInsert();
//		generateLabelList();
		rangeQueryCompare();
	}
	
	/**
	 * compare the  range query time of my method and
	 * GeoPipe defined in SpatialFirst class
	 */
	public static void rangeQueryCompare()
	{
		String layerName = dataset;
//		String entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
		String queryRectanglePath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\spa_predicate\\%s\\queryrect_1.txt", dataset);
		
//		String entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
//		String dbPath = "/home/yuhansun/Documents/GeoGraphMatchData/neo4j-community-3.1.1/data/databases/graph.db";
//		String queryRectanglePath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_1.txt", dataset);
	    try {
	    	GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
			SpatialDatabaseService spatialDatabaseService = new SpatialDatabaseService(databaseService);
	        Layer layer = spatialDatabaseService.getLayer(layerName);
	        
	        ArrayList<MyRectangle> queryRectangles = OwnMethods.ReadQueryRectangle(queryRectanglePath);
	        long timeGeoPipe = 0, timeMyMethod = 0, countGeoPipe = 0, countMyMethod = 0;
	        
	        long start = System.currentTimeMillis();
	        Transaction tx = databaseService.beginTx();
	        for ( MyRectangle queryRectangle : queryRectangles)
	        {
	        	Envelope envelope = new Envelope(queryRectangle.min_x, queryRectangle.min_y, queryRectangle.max_x, queryRectangle.max_y);
	        	List<SpatialDatabaseRecord> results = OSM_Utility.RangeQuery(layer, envelope);
	        	for ( SpatialDatabaseRecord record : results)
	        	{
	        		record.getGeomNode().getId();
	        	}
	        	countGeoPipe += results.size();
	        }
	        tx.success();
	        tx.close();
	        timeGeoPipe += System.currentTimeMillis() - start;
	        
	        start = System.currentTimeMillis();
	        tx = databaseService.beginTx();
	        Node root = OSM_Utility.getRTreeRoot(databaseService, layerName);
	        for ( MyRectangle queryRectangle : queryRectangles)
	        {
	        	LinkedList<Node> results = SpatialFirst.rangeQuery(root, queryRectangle);
	        	for ( Node node : results)
	        		node.getId();
	        	countMyMethod += results.size();
	        }
	        tx.success();
	        tx.close();
	        timeMyMethod += System.currentTimeMillis() - start;
	        
	        databaseService.shutdown();
	        
	        OwnMethods.Print(String.format("GeoPile count:%d", countGeoPipe));
	        OwnMethods.Print(String.format("GeoPipe Time:%d", timeGeoPipe));
	        OwnMethods.Print(String.format("MyMethod count:%d", countMyMethod));
	        OwnMethods.Print(String.format("MyMethod Time:%d", timeMyMethod));
	    }
	    catch(Exception e)
	    {
	    	e.printStackTrace();	System.exit(-1);
	    }
	}
	
	
	public static void batchRTreeInsert()
	{
		try {
			String layerName = "test";
			String entityPath = "D:\\Ubuntu_shared\\GeoMinHop\\data\\Gowalla\\entity.txt";
			String dbPath = "D:\\Ubuntu_shared\\neo4j-community-3.1.1\\data\\databases\\graph.db";
			GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbPath));
			
			SpatialDatabaseService spatialDatabaseService = new SpatialDatabaseService(databaseService);
			
			Transaction tx = databaseService.beginTx();
//			SimplePointLayer simplePointLayer = spatialDatabaseService.createSimplePointLayer(layerName);
//			EditableLayer layer = spatialDatabaseService.getOrCreatePointLayer(layerName, "lon", "lat");
			
			org.neo4j.gis.spatial.Layer layer = spatialDatabaseService.getLayer(layerName);
			
			ArrayList<Entity> entities = OwnMethods.ReadEntity(entityPath); 
			ArrayList<Node> geomNodes = new ArrayList<Node>(entities.size());
			for ( Entity entity : entities)
			{
				if ( entity.IsSpatial)
				{
					Node node = databaseService.createNode(GraphLabel.GRAPH_1);
					node.setProperty(lon_name, entity.lon);
					node.setProperty(lat_name, entity.lat);
					geomNodes.add(node);
				}
			}
			
//			Node node = databaseService.createNode();
//			node.setProperty(lon_name, 22);	node.setProperty(lat_name, 33);
//			geomNodes.add(node);
//			
//			node = databaseService.createNode();
//			node.setProperty(lon_name, 23);	node.setProperty(lat_name, 34);
//			geomNodes.add(node);
			
			layer.addAll(geomNodes);
			
//			OwnMethods.Print(OSM_Utility.getRTreeRoot(databaseService, layerName));
			
//			RTreeIndex rtree = new RTreeIndex(
//                    databaseService,
//                    OSM_Utility.getRTreeRoot(databaseService, "test"),
//                    new SimplePointEncoder()
//            );
			
//			ResourceIterable<Node> nodes = databaseService.getAllNodes();
//			for ( Node node1 : nodes)
//			{
//				OwnMethods.Print(node1);
//				OwnMethods.Print(String.format("node label:%s", node1.getLabels().toString()));
//				OwnMethods.Print(String.format("node property:%s", node1.getAllProperties()));
//				
//				Iterable<Relationship> rels = node1.getRelationships(Direction.OUTGOING);
//				for ( Relationship relationship : rels)
//				{
//					OwnMethods.Print(String.format("%s, type:%s, property:%s", relationship, 
//							relationship.getType() ,relationship.getAllProperties()));
//				}
//				OwnMethods.Print("\n");
//			}
			
			tx.success();
			tx.close();
			spatialDatabaseService.getDatabase().shutdown();
			
		} catch (Exception e) {
			e.printStackTrace();	System.exit(-1);
		}
	}
	
	public static void generateLabelList()
	{
		String dataset = "foursquare";
		String entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
		String labelListPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\label.txt", dataset);
		
		OwnMethods.getLabelListFromEntity(entityPath, labelListPath);
	}
	
	public static void test()
	{
		
	}
	
	public static void Naive()
	{
		String query = "";
		query += String.format("profile match (a0:GRAPH_0),(a1:GRAPH_1),(a2:GRAPH_0),(a3:GRAPH_1),(a0)-[:GRAPH_LINK]-(a1),(a0)-[:GRAPH_LINK]-(a2),(a2)-[:GRAPH_LINK]-(a3) ")
				+ String.format(" where %f <= a%d.%s <= %f ", queryRectangle.min_x, 1, lon_name, queryRectangle.max_x)
				+ String.format("and %f <= a%d.%s <= %f", queryRectangle.min_y, 1, lat_name, queryRectangle.max_y);
		query += " return id(a0), id(a1), id(a2), id(a3)";
		OwnMethods.Print(query);
		GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
		try {
			Transaction tx = databaseService.beginTx();
			Result result = databaseService.execute(query);
			while ( result.hasNext())
				result.next();
			
			ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
			OwnMethods.Print(planDescription.getProfilerStatistics().getRows());
			tx.close();
			tx.success();
			databaseService.shutdown();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

}
