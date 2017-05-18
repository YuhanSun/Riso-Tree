package graph;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import commons.Config;
import commons.Labels;
import commons.OwnMethods;
import commons.Labels.GraphLabel;
import commons.Labels.OSMRelation;
import commons.Config.system;
import osm.OSM_Utility;

/**
 * the class switch spatial vertices in the graph to leaf nodes in R-tree
 * in order to avoid one level of graph traversal from R-tree leaf node to osm nodes
 * So the GRAPH_1 label will be transferred among nodes and
 * GRAPH_LINK relationships will be transferred as well
 * the attribute id, lon and lat will be transfered as well  
 * @author yuhansun
 *
 */
public class Graph_Switch {

	static Config config = new Config();
	static String dataset = config.getDatasetName();
	static String version = config.GetNeo4jVersion();
	static system systemName = config.getSystemName();
	static String lon_name = config.GetLongitudePropertyName();
	static String lat_name = config.GetLatitudePropertyName();
	static String graphLinkLabelName = Labels.GraphRel.GRAPH_LINK.name();
	
	static String db_path;
	
	static int max_hop_num = 2;
	
	static void initParameters()
	{
		switch (systemName) {
		case Ubuntu:
			db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
			break;
		case Windows:
			db_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset, version, dataset);
		default:
			break;
		}
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		initParameters();
		transferGraph();
	}
	
	public static void transferGraph()
	{
		GraphDatabaseService dbservice = new GraphDatabaseFactory().newEmbeddedDatabase(new File(db_path));
		try {
			Transaction tx = dbservice.beginTx();
			Iterable<Node> leafNodes = OSM_Utility.getAllGeometries(dbservice, dataset);
			for ( Node leafNode : leafNodes)
			{
				Node osmNode = leafNode.getSingleRelationship(OSMRelation.GEOM, Direction.INCOMING)
						.getStartNode();
				
				
				//transfer location property
				if( osmNode.hasProperty(lon_name) == false)
					throw new Exception(String.format("%s does not has attribute %s", osmNode, lon_name));
				leafNode.setProperty(lon_name, osmNode.getProperty(lon_name));
				leafNode.setProperty(lat_name, osmNode.getProperty(lat_name));
				
				//transfer node_osm_id property
				if ( osmNode.hasProperty("node_osm_id") == false)
					throw new Exception(String.format("%s does not has attribute node_osm_id", osmNode));
				leafNode.setProperty("node_osm_id", value);
				
				//transfer label
				Iterable<Label> labels = osmNode.getLabels();
				boolean has = false;
				for ( Label label : labels)
					if ( label.name().equals(GraphLabel.GRAPH_1.name()))
					{
						has = true;
						break;
					}
				if ( has == false)
					throw new Exception(String.format("%s does not has label %s", osmNode, GraphLabel.GRAPH_1));
				else
					leafNode.addLabel(GraphLabel.GRAPH_1);
				
			}
			tx.success();
			tx.close();
			dbservice.shutdown();
		}
		catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
