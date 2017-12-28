package graph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import commons.Config;
import commons.Config.system;
import commons.Entity;
import commons.Labels.GraphLabel;
import commons.Labels.GraphRel;
import commons.Labels.RTreeRel;
import commons.OwnMethods;
import commons.RTreeUtility;
import osm.OSM_Utility;

/**
 * latest load class
 * @author ysun138
 *
 */
public class LoadDataNoOSM {
	static Config config = new Config();
	static system systemName;
	static String version, dataset, lon_name, lat_name;
	static int nonspatial_label_count;
	
	static String dbPath, entityPath, mapPath, graphPath, labelListPath, hmbrPath;
	
	static ArrayList<Entity> entities; 
	
	static void initParameters()
	{
		systemName = config.getSystemName();
		version = config.GetNeo4jVersion();
		dataset = config.getDatasetName();
		lon_name = config.GetLongitudePropertyName();
		lat_name = config.GetLatitudePropertyName();
		nonspatial_label_count = config.getNonSpatialLabelCount();
		switch (systemName) {
		case Ubuntu:
			dbPath = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
			entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
			labelListPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/label.txt", dataset);
//			static String map_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map.txt", dataset);
			/**
			 * use this because osm node are not seen as spatial graph
			 * but directly use RTree leaf node as the spatial vertices in the graph
			 */
			mapPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/node_map_RTree.txt", dataset);
			graphPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/graph.txt", dataset);
			hmbrPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/HMBR.txt", dataset);
			break;
		case Windows:
			dbPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", 
					dataset, version, dataset);
			entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
			labelListPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\label.txt", dataset);
			mapPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\node_map_RTree.txt", dataset);
			graphPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\graph.txt", dataset);
			hmbrPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\HMBR.txt", dataset);
		default:
			break;
		}
		
		entities = OwnMethods.ReadEntity(entityPath);
	}
	
	public LoadDataNoOSM()
	{
		initParameters();
	}
	
	public LoadDataNoOSM(Config pConfig)
	{
		this.config = pConfig;
		initParameters();
	}
	
	public static void main(String[] args) {
		
		try {
			initParameters();
			
			batchRTreeInsert();
			
			if ( nonspatial_label_count == 1)
				generateLabelList();
			else
			{
				generateNonspatialLabel();
				nonspatialLabelTest();
			}
			
			LoadNonSpatialEntity();
			
			GetSpatialNodeMap();
			
			LoadGraphEdges();
			
			CalculateCount();
			
			Construct_RisoTree.main(null);
//			
//			loadHMBR();
			
		} catch (Exception e) {
			e.printStackTrace();	System.exit(-1);
		}
		
	}
	
	public static void loadHMBR()
	{
		HashMap<String, String> mapStr = OwnMethods.ReadMap(mapPath);
		HashMap<Integer, Long> graphNeo4jIDMap = new HashMap<Integer, Long>();
		for (String keyStr : mapStr.keySet())
			graphNeo4jIDMap.put(Integer.parseInt(keyStr), Long.parseLong(mapStr.get(keyStr)));
		
		Config config = new Config();
		OwnMethods.loadHMBR(hmbrPath, dbPath, graphNeo4jIDMap, config.GetRectCornerName());
	}
	
	/**
	 * calculate count of spatial vertices 
	 * enclosed by the MBR for each non-leaf 
	 * R-Tree node.
	 * This is important in the query algorithm
	 */
	public static void CalculateCount()
	{
		OwnMethods.Print("Calculate spatial cardinality");
		try {
			GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbPath));
			Transaction tx = databaseService.beginTx();
			
			Iterable<Node> geometry_nodes = RTreeUtility.getAllGeometries(databaseService, dataset);
			Set<Node> set = new HashSet<Node>();
			for ( Node node : geometry_nodes)
			{
				Node parent = node.getSingleRelationship(RTreeRel.RTREE_REFERENCE, Direction.INCOMING).getStartNode();
				if(parent != null)
				{
					if(parent.hasProperty("count"))
						parent.setProperty("count", (Integer)parent.getProperty("count") + 1);
					else
						parent.setProperty("count", 1);
					set.add(parent);
				}
			}
			
			Set<Node> next_level_set = new HashSet<Node>();
			
			while (set.isEmpty() ==  false)
			{
				for (Node node : set)
				{
					Relationship relationship = node.getSingleRelationship(RTreeRel.RTREE_CHILD, Direction.INCOMING);
					if ( relationship != null)
					{
						Node parent = relationship.getStartNode();
						if(parent.hasProperty("count"))
							parent.setProperty("count", (Integer)parent.getProperty("count") + (Integer)node.getProperty("count"));
						else
							parent.setProperty("count", (Integer)node.getProperty("count"));
						next_level_set.add(parent);
					}
				}
				
				set = next_level_set;
				next_level_set = new HashSet<Node>();
			}
			
			tx.success();
			tx.close();
			databaseService.shutdown();
			
		} catch (Exception e) {
			e.printStackTrace();	System.exit(-1);
		}
	}
	
	public static void LoadGraphEdges()
	{
		OwnMethods.Print("Load graph edges\n");
		BatchInserter inserter = null;
		try {
			Map<String, String> id_map = OwnMethods.ReadMap(mapPath);
			Map<String, String> config = new HashMap<String, String>();
			config.put("dbms.pagecache.memory", "6g");
			inserter = BatchInserters.inserter(new File(dbPath).getAbsoluteFile(), config);
			
			ArrayList<ArrayList<Integer>> graph = OwnMethods.ReadGraph(graphPath);
			for (int i = 0; i < graph.size(); i++)
			{
				ArrayList<Integer> neighbors = graph.get(i);
				int start_neo4j_id = Integer.parseInt(id_map.get(String.valueOf(i)));
				for (int j = 0; j < neighbors.size(); j++)
				{
					int neighbor = neighbors.get(j);
					if ( i < neighbor )
					{
						int end_neo4j_id = Integer.parseInt(id_map.get(String.valueOf(neighbor)));
						inserter.createRelationship(start_neo4j_id, end_neo4j_id, GraphRel.GRAPH_LINK, null);
					}
				}
			}
			inserter.shutdown();
			
		} catch (Exception e) {
			e.printStackTrace();	System.exit(-1);
		}
	}
	
	/**
	 * Attach spatial node map to file node_map.txt
	 */
	public static void GetSpatialNodeMap()
	{
		OwnMethods.Print("Get spatial vertices map");
		try {
			Map<Object, Object> id_map = new TreeMap<Object, Object>();
			
			GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbPath));
			Transaction tx = databaseService.beginTx();
			ResourceIterator<Node> spatial_nodes = databaseService.findNodes(GraphLabel.GRAPH_1);
			
			while ( spatial_nodes.hasNext())
			{
				Node node = spatial_nodes.next();
				long neo4jId = node.getId();
				int graphId = (Integer) node.getProperty("id");
				id_map.put(graphId, neo4jId);
			}
			
			tx.success();	tx.close();
			databaseService.shutdown();
			
			OwnMethods.Print("Write spatial node map to " + mapPath + "\n");
			OwnMethods.WriteMap(mapPath, true, id_map);
			
		} catch (Exception e) {
			e.printStackTrace();	System.exit(-1);
		}
	}
	
	/**
	 * Load non-spatial graph vertices and write the map to file node_map.txt
	 */
	static void LoadNonSpatialEntity() 
	{
		try {
			OwnMethods.Print(String.format("LoadNonSpatialEntity\n from %s\n%s\n to %s", 
					entityPath, labelListPath, dbPath));
			
			ArrayList<Entity> entities = OwnMethods.ReadEntity(entityPath);
			ArrayList<Integer> labelList = OwnMethods.readIntegerArray(labelListPath);
			
			Map<Object, Object> id_map = new TreeMap<Object, Object>();

			Map<String, String> config = new HashMap<String, String>();
			config.put("dbms.pagecache.memory", "6g");
			BatchInserter inserter = BatchInserters.inserter(new File(dbPath).getAbsoluteFile(), config);

			for (int i = 0; i < entities.size(); i++)
			{
				Entity entity = entities.get(i);
				if(entity.IsSpatial == false)
				{
					Map<String, Object> properties = new HashMap<String, Object>();
					properties.put("id", entity.id);
					int labelID = labelList.get(i);
					Label label = DynamicLabel.label(String.format("GRAPH_%d", labelID));
					Long pos_id = inserter.createNode(properties, label);
					id_map.put(entity.id, pos_id);
				}
			}
			inserter.shutdown();
			OwnMethods.Print("Write non-spatial node map to "+mapPath+"\n");
			OwnMethods.WriteMap(mapPath, true, id_map);
		} catch (Exception e) {
			e.printStackTrace();	System.exit(-1);
		}
	}
	
	public static void nonspatialLabelTest()
	{
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(labelListPath)));
			ArrayList<Integer> statis = new ArrayList<Integer>(Collections.nCopies(nonspatial_label_count, 0));
			String line = "";
			while ( ( line = reader.readLine()) != null)
			{
				int label = Integer.parseInt(line);
				//if the entity is a spatial one
				if (label == 1)
					continue;
				int index = label - 2;
				statis.set(index, statis.get(index) + 1);
			}
			reader.close();
			for ( int count : statis)
				OwnMethods.Print(count);
		} catch (Exception e) {
			e.printStackTrace();	System.exit(-1);
		}
	}
	
	/**
	 * generate new label.txt
	 * for entities that non-spatial vertices
	 */
	public static void generateNonspatialLabel()
	{
		try {
			FileWriter writer = new FileWriter(new File(labelListPath), true);
			Random random = new Random();
			
			for ( Entity entity : entities)
			{
				if ( entity.IsSpatial)
					writer.write("1\n");
				else
				{
					int label = random.nextInt(nonspatial_label_count);
					label += 2;
					writer.write(label + "\n");
				}
			}
			
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Used if there is only one non-spatial label.
	 * Label will generated based on Entity.
	 * Spatial will be 1 and non-spatial will 0.
	 */
	public static void generateLabelList()
	{
		OwnMethods.Print("Generate the label list based on entity file\n");
		OwnMethods.getLabelListFromEntity(entityPath, labelListPath);
	}

	public static void batchRTreeInsert()
	{
		OwnMethods.Print("Batch insert RTree");
		try {
			String layerName = dataset;
			GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbPath));
			OwnMethods.Print("dataset:" + dataset + "\ndatabase:" + dbPath + "\n");
			
			SpatialDatabaseService spatialDatabaseService = new SpatialDatabaseService(databaseService);
			
			Transaction tx = databaseService.beginTx();
//			SimplePointLayer simplePointLayer = spatialDatabaseService.createSimplePointLayer(layerName);
			EditableLayer layer = spatialDatabaseService.getOrCreatePointLayer(layerName, lon_name, lat_name);
//			org.neo4j.gis.spatial.Layer layer = spatialDatabaseService.getLayer(layerName);
			
			ArrayList<Entity> entities = OwnMethods.ReadEntity(entityPath); 
			ArrayList<Node> geomNodes = new ArrayList<Node>(entities.size());
			for ( Entity entity : entities)
			{
				if ( entity.IsSpatial)
				{
					Node node = databaseService.createNode(GraphLabel.GRAPH_1);
					node.setProperty(lon_name, entity.lon);
					node.setProperty(lat_name, entity.lat);
					node.setProperty("id", entity.id);
					geomNodes.add(node);
				}
			}
			
			layer.addAll(geomNodes);
			
			tx.success();
			tx.close();
			spatialDatabaseService.getDatabase().shutdown();
			
		} catch (Exception e) {
			e.printStackTrace();	System.exit(-1);
		}
	}
	
	public long batchRTreeInsertTime()
	{
		OwnMethods.Print("Get Batch insert RTree time");
		String layerName = dataset;
		GraphDatabaseService databaseService = new GraphDatabaseFactory().newEmbeddedDatabase(new File(dbPath));
		OwnMethods.Print("dataset:" + dataset + "\ndatabase:" + dbPath + "\n");

		SpatialDatabaseService spatialDatabaseService = new SpatialDatabaseService(databaseService);

		Transaction tx = databaseService.beginTx();
		//			SimplePointLayer simplePointLayer = spatialDatabaseService.createSimplePointLayer(layerName);
		EditableLayer layer = spatialDatabaseService.getOrCreatePointLayer(layerName, lon_name, lat_name);
		//			org.neo4j.gis.spatial.Layer layer = spatialDatabaseService.getLayer(layerName);

		ArrayList<Entity> entities = OwnMethods.ReadEntity(entityPath); 
		ArrayList<Node> geomNodes = new ArrayList<Node>(entities.size());
		int spaCount = 0;
		long start =  System.currentTimeMillis();
		for ( Entity entity : entities)
		{
			if ( entity.IsSpatial)
			{
				Node node = databaseService.createNode(GraphLabel.GRAPH_1);
				node.setProperty(lon_name, entity.lon);
				node.setProperty(lat_name, entity.lat);
				node.setProperty("id", entity.id);
				geomNodes.add(node);
				spaCount++;
			}
		}
		long time = System.currentTimeMillis() - start;
		OwnMethods.Print("create node time:" + time);
		OwnMethods.Print("number of spatial objects:"+spaCount);

		start = System.currentTimeMillis();
		layer.addAll(geomNodes);
		long constructionTime = System.currentTimeMillis() - start;
		OwnMethods.Print("construct RTree time:" + constructionTime);

		start = System.currentTimeMillis();
		tx.success();
		tx.close();
		time = System.currentTimeMillis() - start;
		OwnMethods.Print("load into db time:" + time);
		spatialDatabaseService.getDatabase().shutdown();
		return constructionTime;
	}
}
