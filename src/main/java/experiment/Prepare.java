package experiment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

import org.neo4j.gis.spatial.rtree.RTreeRelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.strtree.GeometryItemDistance;
import com.vividsolutions.jts.index.strtree.STRtree;

import osm.OSM_Utility;
import commons.Config;
import commons.Entity;
import commons.Labels.OSMRelation;
import commons.Query_Graph;
import commons.Utility;
import commons.Config.system;
import commons.Labels.GraphLabel;
import commons.OwnMethods;

/**
 * convert {0,1} two labels graph to more selective graph with 10 or 100 labels
 * for Gowalla dataset,
 * new queries for the new graph are generated here
 * @author yuhansun
 *
 */
public class Prepare {

	static Config config = new Config();
	static String dataset = config.getDatasetName();
	static String version = config.GetNeo4jVersion();
	static system systemName = config.getSystemName();
	static int MAX_HOPNUM = config.getMaxHopNum();
	static int nonspatial_label_count = config.getNonSpatialLabelCount();

	static String db_path;
	static String vertex_map_path;
	static String graph_path;
	static String entityPath;
	static String geo_id_map_path;
	static String label_list_path;
	static String graph_node_map_path;
	static String rtree_map_path;
	static String log_path;
	
	static int nonspatial_vertex_count;
	static int spatialVertexCount;
	static ArrayList<Integer> labels;//all labels in the graph
	
	static void initParameters()
	{
		switch (systemName) {
		case Ubuntu:
			db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
			vertex_map_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map.txt", dataset);
			graph_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
			entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
			geo_id_map_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/geom_osmid_map.txt", dataset);
			label_list_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/label.txt", dataset);
			graph_node_map_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map.txt", dataset);
			rtree_map_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/rtree_map.txt", dataset);
			log_path = String.format("/mnt/hgfs/Experiment_Result/Riso-Tree/%s/set_label.log", dataset);
			break;
		case Windows:
			db_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset, version, dataset);
			vertex_map_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\node_map.txt", dataset);
			graph_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\graph.txt", dataset);
			entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
			geo_id_map_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\geom_osmid_map.txt", dataset);
			label_list_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\label.txt", dataset);
			graph_node_map_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\node_map.txt", dataset);
			rtree_map_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\rtree_map.txt", dataset);
			log_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\set_label.log", dataset);
		default:
			break;
		}
		
		ArrayList<Entity> entities = OwnMethods.ReadEntity(entityPath);
		spatialVertexCount = OwnMethods.GetSpatialEntityCount(entities);
		nonspatial_vertex_count = entities.size() - spatialVertexCount;
		
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
		initParameters();
		
		generateNonspatialLabel();
		nonspatialLabelTest();
		
//		String oldDataset = "Gowalla";
//		modifyLayerName(oldDataset);
//		modifyLayerNameTest();
		
//		setNewLabel();
//		newLabelTest();
		
//		generateRandomQueryGraph();
//		generateQueryRectangleCenterID();
		
		
		
//		generateNewNLList();
	}
	
	
	public static void newNLListTest()
	{
		try {
			GraphDatabaseService dbService= new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
			Transaction tx = dbService.beginTx();
			
			Node node = dbService.getNodeById(3852215);
			
			tx.success();
			tx.close();
			dbService.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * generate new NLList for 10 new nonspatial labels
	 * NL_x_0_list will be utilized to generate new list but not removed while
	 * NL_x_1_list will not be touched
	 */
	public static void generateNewNLList()
	{
		try {
			GraphDatabaseService dbService= new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
			Transaction tx = dbService.beginTx();
			
			ArrayList<Integer> labelList = OwnMethods.readIntegerArray(label_list_path);
			HashMap<String, String> map_str = OwnMethods.ReadMap(rtree_map_path);
			HashMap<Integer, Long> rtreeMap = new HashMap<Integer, Long>();
			for ( String key_str : map_str.keySet())
			{
				String value_str = map_str.get(key_str);
				rtreeMap.put(Integer.parseInt(key_str), Long.parseLong(value_str));
			}
//			Node root = OSM_Utility.getRTreeRoot(dbService, dataset);
//			OwnMethods.Print(root.getId());
//			OwnMethods.Print(rtreeMap.get(0));
			
			for ( int key : rtreeMap.keySet())
			{
				OwnMethods.Print(key);
				Node node = dbService.getNodeById(rtreeMap.get(key));
				for ( int hop = 1; hop <= MAX_HOPNUM; hop++)
				{
					String oriNLListPropertyName = String.format("NL_%d_0_list", hop);
					int[] oriNLList =  (int[]) node.getProperty(oriNLListPropertyName);
					
					HashMap<Integer, ArrayList<Integer>> newNLList = new HashMap<Integer, ArrayList<Integer>>();
					for ( int label : labels)
						newNLList.put(label, new ArrayList<Integer>());
					for ( int id : oriNLList)
					{
						int label = labelList.get(id);
						newNLList.get(label).add(id);
					}
					for ( int label : labels)
					{
						String NLListPropertyName = String.format("NL_%d_%d_list", hop, label);
						String NLListSizePropertyName = String.format("NL_%d_%d_size", hop, label);
						
						ArrayList<Integer> NLListLabel = newNLList.get(label);
						int size = NLListLabel.size();
						if ( size == 0)
						{
							node.setProperty(NLListSizePropertyName, 0);
							continue;
						}
						else
						{
							node.setProperty(NLListSizePropertyName, size);
							int[] NLListLabelArray = new int[size];
							for ( int i = 0; i < size; i++)
								NLListLabelArray[i] = NLListLabel.get(i);
							node.setProperty(NLListPropertyName, NLListLabelArray);
						}
					}
				}
			}
			
			tx.success();
			tx.close();
			dbService.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void modifyLayerNameTest()
	{
		try {
			GraphDatabaseService dbService= new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
			Transaction tx = dbService.beginTx();
			
			Node root = OSM_Utility.getRTreeRoot(dbService, dataset);
			OwnMethods.Print(root.getAllProperties());
			Node layer_node = root.getSingleRelationship(RTreeRelationshipTypes.RTREE_ROOT, Direction.INCOMING)
					.getStartNode();
			OwnMethods.Print(layer_node.getAllProperties());
			
			Node osmLayerNode = layer_node.getSingleRelationship(OSMRelation.LAYERS, Direction.INCOMING)
					.getStartNode();
			OwnMethods.Print(String.format("osmlayernode : %s", osmLayerNode.getAllProperties()));
			tx.success();
			tx.close();
			dbService.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Change the layername to new layername
	 * including layer property in RTree layer
	 * and name property in OSM data 
	 */
	public static void modifyLayerName(String oldDataset)
	{
		try {
			GraphDatabaseService dbService= new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
			Transaction tx = dbService.beginTx();
			
			Node root = OSM_Utility.getRTreeRoot(dbService, oldDataset);
			OwnMethods.Print(root.getAllProperties());
			Node layer_node = root.getSingleRelationship(RTreeRelationshipTypes.RTREE_ROOT, Direction.INCOMING)
					.getStartNode();
			OwnMethods.Print(layer_node.getAllProperties());
			layer_node.setProperty("layer", dataset);
			
			Node osmLayerNode = layer_node.getSingleRelationship(OSMRelation.LAYERS, Direction.INCOMING)
					.getStartNode();
			OwnMethods.Print(String.format("osmlayernode : %s", osmLayerNode.getAllProperties()));
			osmLayerNode.setProperty("name", dataset);
			tx.success();
			tx.close();
			dbService.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void generateQueryRectangleForSelectivity()
	{
		try {
			int experiment_count = 100;
			String entity_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
			ArrayList<Entity> entities = OwnMethods.ReadEntity((String)entity_path);
			int spa_count = OwnMethods.GetSpatialEntityCount(entities);
			STRtree stRtree = OwnMethods.ConstructSTRee(entities);

			String center_id_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/%s_centerids.txt", dataset, dataset);
			ArrayList<Integer> center_ids = OwnMethods.ReadCenterID(center_id_path);
			ArrayList<Integer> final_center_ids = OwnMethods.GetRandom_NoDuplicate(center_ids, experiment_count);

			double selectivity = 0.0001;
			while ( selectivity < 0.2)
			{
				int name_suffix = (int) (selectivity * spa_count);
				String output_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_%d.txt", dataset, name_suffix);
				String write_line = "";
				for (int id : final_center_ids)
				{
					double lon = entities.get(id).lon;
					double lat = entities.get(id).lat;
					GeometryFactory factory = new GeometryFactory();
					Point center = factory.createPoint(new Coordinate(lon, lat));
					Object[] result = stRtree.kNearestNeighbour(center.getEnvelopeInternal(),
							new GeometryFactory().toGeometry(center.getEnvelopeInternal()),
							new GeometryItemDistance(), name_suffix);
					double radius = 0.0;
					for (Object object : result)
					{
						Point point = (Point) object;
						double dist = center.distance(point);
						if(dist > radius)
							radius = dist;
					}
					double a = Math.sqrt(Math.PI) * radius;
					double minx = center.getX() - a / 2;
					double miny = center.getY() - a / 2;
					double maxx = center.getX() + a / 2;
					double maxy = center.getY() + a / 2;

					write_line = String.format("%f\t%f\t%f\t%f\n", minx, miny, maxx, maxy);
					OwnMethods.WriteFile(output_path, true, write_line);
				}
				selectivity *= 10;
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	public static void generateQueryRectangleCenterID()
	{
		String entity_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
		ArrayList<Entity> entities = OwnMethods.ReadEntity(entity_path);
		
		String center_id_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/%s_centerids.txt", dataset, dataset);
		OwnMethods.generateQueryRectangleCenterID(entities, center_id_path, 500);
	}
	
	/**
	 * Generate the query graph for the experiment
	 */
	public static void generateRandomQueryGraph()
	{
		for ( int node_count = 2; node_count < 4; node_count++)
		{
			int spa_pred_count = 1;
			String querygraph_path = "";
			switch (systemName) {
			case Ubuntu:
				querygraph_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/"
						+ "query/query_graph/%s/%d.txt", dataset, node_count);
				break;
			case Windows:
				querygraph_path = String.format("D:\\Ubuntu_shared\\GeoMinHop"
						+ "\\query\\query_graph\\%s\\%d.txt", dataset, node_count);
			}
			
			ArrayList<ArrayList<Integer>> datagraph = OwnMethods.ReadGraph(graph_path);
			ArrayList<Entity> entities = OwnMethods.ReadEntity(entityPath);
			ArrayList<Integer> labels = OwnMethods.readIntegerArray(label_list_path);
			
			OwnMethods.Print(datagraph.size());
			OwnMethods.Print(entities.size());
			OwnMethods.Print(labels.size());
			
			ArrayList<Query_Graph> query_Graphs = new ArrayList<Query_Graph>(10);
			while ( query_Graphs.size() != 10)
			{
				Query_Graph query_Graph = OwnMethods.GenerateRandomGraph(datagraph, labels, 
						entities, node_count, spa_pred_count);

				query_Graph.iniStatistic();

				if ( query_Graphs.size() == 0)
					query_Graphs.add(query_Graph);
				else
					for ( int j = 0; j < query_Graphs.size(); j++)
					{
						Query_Graph queryGraph = query_Graphs.get(j);
						if ( queryGraph.isIsomorphism(query_Graph) == true)
							break;
						if ( j == query_Graphs.size() - 1)
						{
							OwnMethods.Print(query_Graph.toString() + "\n");
							query_Graphs.add(query_Graph);
						}
					}
				OwnMethods.Print(query_Graphs.size());

			}
			Utility.WriteQueryGraph(querygraph_path, query_Graphs);
		}
	}
	
	public static void newLabelTest()
	{
		try {
			GraphDatabaseService dbService= new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
			Transaction tx = dbService.beginTx();
			
			for ( int i = 0; i < 10; i++)
			{
				int labelIndex = i + 2;
				Label label = DynamicLabel.label(String.format("GRAPH_%d", labelIndex));
				ResourceIterator<Node> nodes = dbService.findNodes(label);
				int count = 0;
				while ( nodes.hasNext())
				{
					count++;
					nodes.next();
				}
				OwnMethods.Print(count);
			}
			
			ResourceIterator<Node> nodes = dbService.findNodes(GraphLabel.GRAPH_0);
			while(nodes.hasNext())
			{
				Node node = nodes.next();
				int id = (Integer) node.getProperty("id");
				Iterable<Label> labels = node.getLabels();
				OwnMethods.Print(String.format("%d, %s", id, labels.toString()));
			}
			
			tx.success();
			tx.close();
			dbService.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void setNewLabel()
	{
		try {
			ArrayList<Integer> label_list = OwnMethods.readIntegerArray(label_list_path);
			GraphDatabaseService dbService= new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
			Transaction tx = dbService.beginTx();
			ResourceIterator<Node> nodes = dbService.findNodes(GraphLabel.GRAPH_0);
			int index = 0;
			while ( nodes.hasNext())
			{
				OwnMethods.Print(index);	index++;
				Node node = nodes.next();
//				node.removeLabel(GraphLabel.GRAPH_0);
				int id = (Integer) node.getProperty("id");
				int labelIndex = label_list.get(id);
				String label_name = String.format("GRAPH_%d", labelIndex);
				Label label = DynamicLabel.label(label_name);
				node.addLabel(label);
			}
			
			tx.success();
			tx.close();
			dbService.shutdown();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void nonspatialLabelTest()
	{
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(label_list_path)));
			ArrayList<Integer> statis = new ArrayList<Integer>(Collections.nCopies(nonspatial_label_count, 0));
			String line = "";
			for ( int i = 0; i < nonspatial_vertex_count; i++)
			{
				line = reader.readLine();
				int label = Integer.parseInt(line);
				int index = label - 2;
				statis.set(index, statis.get(index) + 1);
			}
			for ( int count : statis)
				OwnMethods.Print(count);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * generate new label.txt
	 * for entities that non-spatial vertices are all before spatial
	 */
	public static void generateNonspatialLabel()
	{
		try {
			FileWriter writer = new FileWriter(new File(label_list_path), true);
			Random random = new Random();
			for ( int i = 0; i < nonspatial_vertex_count; i++)
			{
				int label = random.nextInt(nonspatial_label_count);
				label += 2;
				writer.write(label + "\n");
			}
			
			for ( int i = 0; i < spatialVertexCount; i++)
				writer.write("1\n");
			
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

}
