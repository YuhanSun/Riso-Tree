package experiment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import commons.Config;
import commons.Entity;
import commons.OwnMethods;
import commons.Config.Datasets;
import commons.Config.system;

/**
 * This is used for analyze experiment results 
 * in the RisoTree Paper.
 * @author ysun138
 *
 */
public class Analyze {
	static Config config = new Config();
	static system systemName;
	static String version, dataset, lon_name, lat_name;
	static int nonspatial_label_count;
	
	static String dbPath, entityPath, mapPath, graphPath, labelListPath, hmbrPath;
	static ArrayList<String> dataset_a = new ArrayList<String>(Arrays.asList(
			Config.Datasets.Gowalla_100.name(), 
			Config.Datasets.foursquare_100.name(),
			Config.Datasets.Patents_100_random_80.name(), 
			Config.Datasets.go_uniprot_100_random_80.name()));
	
//	static ArrayList<Entity> entities; 
	
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
		
//		entities = OwnMethods.ReadEntity(entityPath);
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		OwnMethods.Print(config.getDatasetName());
		config.setDatasetName(Datasets.wikidata_100.toString());
		initParameters();
		OwnMethods.Print(graphPath);
		getAverageDegree();
		
//		get2HopNeighborCount();
		
	}
	
	public static void getAverageDegree()
	{
		ArrayList<ArrayList<Integer>> graph = OwnMethods.ReadGraph(graphPath);
		int edgeCount = 0;
		for ( ArrayList<Integer> neighbors : graph)
		{
			edgeCount += neighbors.size();
		}
		OwnMethods.Print("node count: " + graph.size());
		OwnMethods.Print("edge count: " + edgeCount);
		OwnMethods.Print("average edge count: " + (double) edgeCount / graph.size() );
	}
	
	public static void get1HopNeighborCount() {
		
	}
	
	public static void get2HopNeighborCount() {
		for ( String dataset : dataset_a)
		{
			config.setDatasetName(dataset);
			initParameters();
			ArrayList<ArrayList<Integer>> graph = OwnMethods.ReadGraph(graphPath);
			int count = 0;
			int i = 0;
			for (ArrayList<Integer> list : graph)
			{
				OwnMethods.Print(i);
				HashSet<Integer> hop2Neighbors = new HashSet<>();
				for ( int neighborId : list)
				{
					for ( int id : graph.get(neighborId))
					hop2Neighbors.add(id);
				}
				count+=hop2Neighbors.size();
				i++;
			}
			OwnMethods.Print("count: " + count);
		}
		
	}

}
