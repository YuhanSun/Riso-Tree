package experiment;

import java.util.ArrayList;
import java.util.HashMap;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;

import commons.*;
import commons.Config.system;
import graph.*;

public class Experiment {
	
	static Config config = new Config();
	static String dataset = config.getDatasetName();
	static String version = config.GetNeo4jVersion();
	static system systemName = config.getSystemName();
	
	static String db_path;
	static String querygraphDir;
	static String graph_pos_map_path;
	
	static boolean TEST_FORMAT;
	
	public static void initializeParameters()
	{	
		TEST_FORMAT = false;
		switch (systemName) {
		case Ubuntu:
			db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
			querygraphDir = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph/%s/", dataset);
			graph_pos_map_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/" + dataset + "/node_map.txt";
			break;
		case Windows:
			db_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset, version, dataset);
			querygraphDir = String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\query_graph\\%s\\", dataset);
			graph_pos_map_path = "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset + "\\node_map.txt";
			break;
		}
	}
	
	public static void main(String[] args) {
		initializeParameters();
		
//		int queryIndex = 2;
//		SpatialFirst(queryIndex);
//		SpatialFirstList(queryIndex);
		
//		SpatialFirst(2, queryIndex);
//		SpatialFirstList_Block(2, queryIndex);
//		
//		SpatialFirst(3, queryIndex);
//		SpatialFirstList_Block(3, queryIndex);
		
//		SpatialFirst(4, queryIndex);
//		SpatialFirstList_Block(4, queryIndex);
		
//		dataset = "Gowalla_100";
		initializeParameters();
		
		for ( int queryIndex = 0; queryIndex < 3; queryIndex++)
		{
			if ( queryIndex == 0)
				for ( int nodeCount = 3; nodeCount <= 4; nodeCount++)
				{
					SpatialFirst(nodeCount, queryIndex);
					SpatialFirstList_Block(nodeCount, queryIndex);
	//				Neo4j_Naive(nodeCount, queryIndex);
				}
			else
				for ( int nodeCount = 2; nodeCount <= 4; nodeCount++)
				{
					SpatialFirst(nodeCount, queryIndex);
					SpatialFirstList_Block(nodeCount, queryIndex);
	//				Neo4j_Naive(nodeCount, queryIndex);
				}
		}
	}
	
	public static void Neo4j_Naive(int nodeCount, int query_id)
	{
		long start;
		long time;
		int limit = -1;
		int expe_count = 5;

		String querygraph_path = String.format("%s%d.txt", querygraphDir, nodeCount);
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
		Query_Graph query_Graph = queryGraphs.get(query_id);

		String result_detail_path = null, result_avg_path = null;
		switch (systemName) {
		case Ubuntu:
			result_detail_path = String.format("/mnt/hgfs/Experiment_Result/Riso-Tree/%s/neo4j_%d_%d_API.txt", dataset, nodeCount, query_id);
			result_avg_path = String.format("/mnt/hgfs/Experiment_Result/Riso-Tree/%s/neo4j_%d_%d_API_avg.txt", dataset, nodeCount, query_id);
			break;
		case Windows:
			result_detail_path = String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s\\neo4j_%d_%d_API.txt", dataset, nodeCount, query_id);
			result_avg_path = String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s\\neo4j_%d_%d_API_avg.txt", dataset, nodeCount, query_id);
			break;
		}

		ArrayList<Long> time_get_iterator = new ArrayList<Long>();
		ArrayList<Long> time_iterate = new ArrayList<Long>();
		ArrayList<Long> total_time = new ArrayList<Long>();
		ArrayList<Long> count = new ArrayList<Long>();
		ArrayList<Long> access = new ArrayList<Long>();

		String write_line = String.format("%s\t%d\n", dataset, limit);
		if(!TEST_FORMAT)
		{
			OwnMethods.WriteFile(result_detail_path, true, write_line);
			OwnMethods.WriteFile(result_avg_path, true, write_line);
		}

		String head_line = "count\tget_iterator_time\titerate_time\ttotal_time\taccess_pages\n";
		if(!TEST_FORMAT)
			OwnMethods.WriteFile(result_avg_path, true, "selectivity\t" + head_line);

		int name_suffix = 1;
		int times = 10;
		while ( name_suffix <= 2000)
		{
			double selectivity = name_suffix / 1000000.0;
			
			String queryrect_path = null;
			switch (systemName) {
			case Ubuntu:
				queryrect_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_%d.txt", dataset, name_suffix);
				break;
			case Windows:
				queryrect_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\spa_predicate\\%s\\queryrect_%d.txt", dataset, name_suffix);
				break;
			}

			write_line = selectivity + "\n" + head_line;
			if(!TEST_FORMAT)
				OwnMethods.WriteFile(result_detail_path, true, write_line);

			ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
			Naive_Neo4j_Match naive_Neo4j_Match = new Naive_Neo4j_Match(db_path);
			for ( int i = 0; i < expe_count; i++)
			{
				MyRectangle rectangle = queryrect.get(i);
				query_Graph.spa_predicate = new MyRectangle[query_Graph.graph.size()];

				int j = 0;
				for (  ; j < query_Graph.graph.size(); j++)
					if(query_Graph.Has_Spa_Predicate[j])
						break;
				query_Graph.spa_predicate[j] = rectangle;

				if(!TEST_FORMAT)
				{
					OwnMethods.Print(String.format("%d : %s", i, rectangle.toString()));

					start = System.currentTimeMillis();
					Result result = naive_Neo4j_Match.SubgraphMatch_Spa_API(query_Graph, limit);
					time = System.currentTimeMillis() - start;
					time_get_iterator.add(time);

					start = System.currentTimeMillis();
					while(result.hasNext())
						result.next();
					time = System.currentTimeMillis() - start;
					time_iterate.add(time);

					int index = time_get_iterator.size() - 1;
					total_time.add(time_get_iterator.get(index) + time_iterate.get(index));

					ExecutionPlanDescription planDescription = result.getExecutionPlanDescription();
					count.add(planDescription.getProfilerStatistics().getRows());
					access.add(OwnMethods.GetTotalDBHits(planDescription));

					write_line = String.format("%d\t%d\t", count.get(i), time_get_iterator.get(i));
					write_line += String.format("%d\t%d\t", time_iterate.get(i), total_time.get(i));
					write_line += String.format("%d\n", access.get(i));
					if(!TEST_FORMAT)
						OwnMethods.WriteFile(result_detail_path, true, write_line);
				}
			}
			naive_Neo4j_Match.neo4j_API.ShutDown();

			write_line = String.valueOf(selectivity) + "\t";
			write_line += String.format("%d\t%d\t", Utility.Average(count), Utility.Average(time_get_iterator));
			write_line += String.format("%d\t%d\t", Utility.Average(time_iterate), Utility.Average(total_time));
			write_line += String.format("%d\n", Utility.Average(access));
			if(!TEST_FORMAT)
				OwnMethods.WriteFile(result_avg_path, true, write_line);

			long larger_time = Utility.Average(total_time);
			if (larger_time * expe_count > 150 * 1000)
				expe_count = (int) (expe_count * 0.5 / (larger_time * expe_count / 150.0 / 1000.0));
			if(expe_count < 1)
				expe_count = 1;

			count.clear();	time_get_iterator.clear();
			time_iterate.clear();	total_time.clear();
			access.clear();

			name_suffix *= times;
		}

	}
	
	public static void SpatialFirstList_Block(int nodeCount, int query_id)
	{
		long start;
		long time;
		int limit = -1;
		int expe_count = 5;
		
		String querygraph_path = String.format("%s%d.txt", querygraphDir, nodeCount);
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
		Query_Graph query_Graph = queryGraphs.get(query_id);
		
		String result_detail_path = null, result_avg_path = null;
		switch (systemName) {
		case Ubuntu:
			result_detail_path = String.format("/mnt/hgfs/Experiment_Result/Riso-Tree/%s/spa_first_list_Block_%d_%d_API.txt", dataset, nodeCount, query_id);
			result_avg_path = String.format("/mnt/hgfs/Experiment_Result/Riso-Tree/%s/spa_first_list_Block_%d_%d_API_avg.txt", dataset, nodeCount, query_id);
			break;
		case Windows:
			result_detail_path = String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s\\spa_first_list_Block_%d_%d_API.txt", dataset, nodeCount, query_id);
			result_avg_path = String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s\\spa_first_list_Block_%d_%d_API_avg.txt", dataset, nodeCount, query_id);
			break;
		}
		
		String write_line = String.format("%s\t%d\n", dataset, limit);
		if(!TEST_FORMAT)
		{
			OwnMethods.WriteFile(result_detail_path, true, write_line);
			OwnMethods.WriteFile(result_avg_path, true, write_line);
		}
		
		String head_line = "count\trange_time\tget_iterator_time\titerate_time\ttotal_time\taccess_pages\n";
		if(!TEST_FORMAT)
			OwnMethods.WriteFile(result_avg_path, true, "selectivity\t" + head_line);
		
		int name_suffix = 1;
		int times = 10;
		while ( name_suffix <= 2000)
		{
			double selectivity = name_suffix / 1000000.0;
			
			String queryrect_path = null;
			switch (systemName) {
			case Ubuntu:
				queryrect_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_%d.txt", dataset, name_suffix);
				break;
			case Windows:
				queryrect_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\spa_predicate\\%s\\queryrect_%d.txt", dataset, name_suffix);
				break;
			}
			
			write_line = selectivity + "\n" + head_line;
			if(!TEST_FORMAT)
				OwnMethods.WriteFile(result_detail_path, true, write_line);
			
			ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
			HashMap<String, String> graph_pos_map = OwnMethods.ReadMap(graph_pos_map_path);
			long[] graph_pos_map_list= new long[graph_pos_map.size()];
			for ( String key_str : graph_pos_map.keySet())
			{
				int key = Integer.parseInt(key_str);
				int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
				graph_pos_map_list[key] = pos_id;
			}
			SpatialFirst_List spa_First_list = new SpatialFirst_List(db_path, dataset, graph_pos_map_list);
			
			ArrayList<Long> range_query_time = new ArrayList<Long>();
			ArrayList<Long> time_get_iterator = new ArrayList<Long>();
			ArrayList<Long> time_iterate = new ArrayList<Long>();
			ArrayList<Long> total_time = new ArrayList<Long>();
			ArrayList<Long> count = new ArrayList<Long>();
			ArrayList<Long> access = new ArrayList<Long>();
			
			for ( int i = 0; i < expe_count; i++)
			{
				MyRectangle rectangle = queryrect.get(i);
				query_Graph.spa_predicate = new MyRectangle[query_Graph.graph.size()];
				
				int j = 0;
				for (  ; j < query_Graph.graph.size(); j++)
					if(query_Graph.Has_Spa_Predicate[j])
						break;
				query_Graph.spa_predicate[j] = rectangle;
				
				if(!TEST_FORMAT)
				{
					OwnMethods.Print(String.format("%d : %s", i, rectangle.toString()));
					
					start = System.currentTimeMillis();
					spa_First_list.query_Block(query_Graph, -1);
					time = System.currentTimeMillis() - start;
					
					time_get_iterator.add(spa_First_list.get_iterator_time);
					time_iterate.add(spa_First_list.iterate_time);
					total_time.add(time);
					count.add(spa_First_list.result_count);
					access.add(spa_First_list.page_hit_count);
					range_query_time.add(spa_First_list.range_query_time);
					
					write_line = String.format("%d\t%d\t", count.get(i), range_query_time.get(i));
					write_line += String.format("%d\t", time_get_iterator.get(i));
					write_line += String.format("%d\t%d\t", time_iterate.get(i), total_time.get(i));
					write_line += String.format("%d\n", access.get(i));
					if(!TEST_FORMAT)
						OwnMethods.WriteFile(result_detail_path, true, write_line);
				}
			}
			spa_First_list.shutdown();
			
			write_line = String.valueOf(selectivity) + "\t";
			write_line += String.format("%d\t%d\t", Utility.Average(count), Utility.Average(range_query_time));
			write_line += String.format("%d\t", Utility.Average(time_get_iterator));
			write_line += String.format("%d\t%d\t", Utility.Average(time_iterate), Utility.Average(total_time));
			write_line += String.format("%d\n", Utility.Average(access));
			if(!TEST_FORMAT)
				OwnMethods.WriteFile(result_avg_path, true, write_line);
			
			long larger_time = Utility.Average(total_time);
			if (larger_time * expe_count > 450 * 1000)
				expe_count = (int) (expe_count * 0.5 / (larger_time * expe_count / 450.0 / 1000.0));
			if(expe_count < 1)
				expe_count = 1;
			
			name_suffix *= times;
		}
		OwnMethods.WriteFile(result_detail_path, true, "\n");
		OwnMethods.WriteFile(result_avg_path, true, "\n");
	}
	
	public static void SpatialFirstList(int query_id)
	{
		long start;
		long time;
		int limit = -1;
		int expe_count = 5;
		int nodeCount = 4;
		
		String querygraph_path = String.format("%s%d.txt", querygraphDir, nodeCount);
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
		Query_Graph query_Graph = queryGraphs.get(query_id);
		
		String result_detail_path = null, result_avg_path = null;
		switch (systemName) {
		case Ubuntu:
			result_detail_path = String.format("/mnt/hgfs/Experiment_Result/Riso-Tree/%s/spa_first_list_%d_API.txt", dataset, query_id);
			result_avg_path = String.format("/mnt/hgfs/Experiment_Result/Riso-Tree/%s/spa_first_list_%d_API_avg.txt", dataset, query_id);
			break;
		case Windows:
			result_detail_path = String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s\\spa_first_list_%d_API.txt", dataset, query_id);
			result_avg_path = String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s\\spa_first_list_%d_API_avg.txt", dataset, query_id);
			break;
		}
		
		String write_line = String.format("%s\t%d\n", dataset, limit);
		if(!TEST_FORMAT)
		{
			OwnMethods.WriteFile(result_detail_path, true, write_line);
			OwnMethods.WriteFile(result_avg_path, true, write_line);
		}
		
		String head_line = "count\trange_time\tget_iterator_time\titerate_time\ttotal_time\taccess_pages\n";
		if(!TEST_FORMAT)
			OwnMethods.WriteFile(result_avg_path, true, "selectivity\t" + head_line);
		
		int name_suffix = 1000;
		int times = 10;
		while ( name_suffix <= 2000)
		{
			double selectivity = name_suffix / 1000000.0;
			
			String queryrect_path = null;
			switch (systemName) {
			case Ubuntu:
				queryrect_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_%d.txt", dataset, name_suffix);
				break;
			case Windows:
				queryrect_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\spa_predicate\\%s\\queryrect_%d.txt", dataset, name_suffix);
				break;
			}
			
			write_line = selectivity + "\n" + head_line;
			if(!TEST_FORMAT)
				OwnMethods.WriteFile(result_detail_path, true, write_line);
			
			ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
			HashMap<String, String> graph_pos_map = OwnMethods.ReadMap(graph_pos_map_path);
			long[] graph_pos_map_list= new long[graph_pos_map.size()];
			for ( String key_str : graph_pos_map.keySet())
			{
				int key = Integer.parseInt(key_str);
				int pos_id = Integer.parseInt(graph_pos_map.get(key_str));
				graph_pos_map_list[key] = pos_id;
			}
			SpatialFirst_List spa_First_list = new SpatialFirst_List(db_path, dataset, graph_pos_map_list);
			
			ArrayList<Long> range_query_time = new ArrayList<Long>();
			ArrayList<Long> time_get_iterator = new ArrayList<Long>();
			ArrayList<Long> time_iterate = new ArrayList<Long>();
			ArrayList<Long> total_time = new ArrayList<Long>();
			ArrayList<Long> count = new ArrayList<Long>();
			ArrayList<Long> access = new ArrayList<Long>();
			
			for ( int i = 0; i < expe_count; i++)
			{
				MyRectangle rectangle = queryrect.get(i);
				query_Graph.spa_predicate = new MyRectangle[query_Graph.graph.size()];
				
				int j = 0;
				for (  ; j < query_Graph.graph.size(); j++)
					if(query_Graph.Has_Spa_Predicate[j])
						break;
				query_Graph.spa_predicate[j] = rectangle;
				
				if(!TEST_FORMAT)
				{
					OwnMethods.Print(String.format("%d : %s", i, rectangle.toString()));
					
					start = System.currentTimeMillis();
					spa_First_list.query(query_Graph, -1);
					time = System.currentTimeMillis() - start;
					
					time_get_iterator.add(spa_First_list.get_iterator_time);
					time_iterate.add(spa_First_list.iterate_time);
					total_time.add(time);
					count.add(spa_First_list.result_count);
					access.add(spa_First_list.page_hit_count);
					range_query_time.add(spa_First_list.range_query_time);
					
					write_line = String.format("%d\t%d\t", count.get(i), range_query_time.get(i));
					write_line += String.format("%d\t", time_get_iterator.get(i));
					write_line += String.format("%d\t%d\t", time_iterate.get(i), total_time.get(i));
					write_line += String.format("%d\n", access.get(i));
					if(!TEST_FORMAT)
						OwnMethods.WriteFile(result_detail_path, true, write_line);
				}
			}
			spa_First_list.shutdown();
			
			write_line = String.valueOf(selectivity) + "\t";
			write_line += String.format("%d\t%d\t", Utility.Average(count), Utility.Average(range_query_time));
			write_line += String.format("%d\t", Utility.Average(time_get_iterator));
			write_line += String.format("%d\t%d\t", Utility.Average(time_iterate), Utility.Average(total_time));
			write_line += String.format("%d\n", Utility.Average(access));
			if(!TEST_FORMAT)
				OwnMethods.WriteFile(result_avg_path, true, write_line);
			
			long larger_time = Utility.Average(total_time);
			if (larger_time * expe_count > 450 * 1000)
				expe_count = (int) (expe_count * 0.5 / (larger_time * expe_count / 450.0 / 1000.0));
			if(expe_count < 1)
				expe_count = 1;
			
			name_suffix *= times;
		}
		OwnMethods.WriteFile(result_detail_path, true, "\n");
		OwnMethods.WriteFile(result_avg_path, true, "\n");
	}
	
	public static void SpatialFirst(int nodeCount, int query_id)
	{
		long start;
		long time;
		int limit = -1;
		int expe_count = 5;

		String querygraph_path = String.format("%s%d.txt", querygraphDir, nodeCount);
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
		Query_Graph query_Graph = queryGraphs.get(query_id);

		String result_detail_path = null, result_avg_path = null;
		switch(systemName)
		{
		case Ubuntu:
			result_detail_path = String.format("/mnt/hgfs/Experiment_Result/Riso-Tree/%s/spa_first_%d_%d_API.txt", dataset, nodeCount, query_id);
			result_avg_path = String.format("/mnt/hgfs/Experiment_Result/Riso-Tree/%s/spa_first_%d_%d_API_avg.txt", dataset, nodeCount, query_id);
			break;
		case Windows:
			result_detail_path = String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s\\spa_first_%d_%d_API.txt", dataset, nodeCount, query_id);
			result_avg_path = String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s\\spa_first_%d_%d_API_avg.txt", dataset, nodeCount, query_id);
			break;
		}

		String write_line = String.format("%s\t%d\n", dataset, limit);
		if(!TEST_FORMAT)
		{
			OwnMethods.WriteFile(result_detail_path, true, write_line);
			OwnMethods.WriteFile(result_avg_path, true, write_line);
		}

		String head_line = "count\trange_time\tget_iterator_time\titerate_time\ttotal_time\taccess_pages\n";
		if(!TEST_FORMAT)
			OwnMethods.WriteFile(result_avg_path, true, "selectivity\t" + head_line);

		int name_suffix = 1;
		int times = 10;
		while ( name_suffix <= 2000)
		{
			double selectivity = name_suffix / 1000000.0;
			String queryrect_path = null;
			switch (systemName) {
			case Ubuntu:
				queryrect_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_%d.txt", dataset, name_suffix);
				break;
			case Windows:
				queryrect_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\spa_predicate\\%s\\queryrect_%d.txt", dataset, name_suffix);
				break;
			}

			write_line = selectivity + "\n" + head_line;
			if(!TEST_FORMAT)
				OwnMethods.WriteFile(result_detail_path, true, write_line);

			ArrayList<MyRectangle> queryrect = OwnMethods.ReadQueryRectangle(queryrect_path);
			SpatialFirst spa_First = new SpatialFirst(db_path, dataset);

			ArrayList<Long> range_query_time = new ArrayList<Long>();
			ArrayList<Long> time_get_iterator = new ArrayList<Long>();
			ArrayList<Long> time_iterate = new ArrayList<Long>();
			ArrayList<Long> total_time = new ArrayList<Long>();
			ArrayList<Long> count = new ArrayList<Long>();
			ArrayList<Long> access = new ArrayList<Long>();

			for ( int i = 0; i < expe_count; i++)
			{
				MyRectangle rectangle = queryrect.get(i);
				query_Graph.spa_predicate = new MyRectangle[query_Graph.graph.size()];

				int j = 0;
				for (  ; j < query_Graph.graph.size(); j++)
					if(query_Graph.Has_Spa_Predicate[j])
						break;
				query_Graph.spa_predicate[j] = rectangle;

				if(!TEST_FORMAT)
				{
					OwnMethods.Print(String.format("%d : %s", i, rectangle.toString()));

					start = System.currentTimeMillis();
					spa_First.query(query_Graph, -1);
					time = System.currentTimeMillis() - start;

					time_get_iterator.add(spa_First.get_iterator_time);
					time_iterate.add(spa_First.iterate_time);
					total_time.add(time);
					count.add(spa_First.result_count);
					access.add(spa_First.page_hit_count);
					range_query_time.add(spa_First.range_query_time);

					write_line = String.format("%d\t%d\t", count.get(i), range_query_time.get(i));
					write_line += String.format("%d\t", time_get_iterator.get(i));
					write_line += String.format("%d\t%d\t", time_iterate.get(i), total_time.get(i));
					write_line += String.format("%d\n", access.get(i));
					if(!TEST_FORMAT)
						OwnMethods.WriteFile(result_detail_path, true, write_line);
				}
			}
			spa_First.shutdown();

			write_line = String.valueOf(selectivity) + "\t";
			write_line += String.format("%d\t%d\t", Utility.Average(count), Utility.Average(range_query_time));
			write_line += String.format("%d\t", Utility.Average(time_get_iterator));
			write_line += String.format("%d\t%d\t", Utility.Average(time_iterate), Utility.Average(total_time));
			write_line += String.format("%d\n", Utility.Average(access));
			if(!TEST_FORMAT)
				OwnMethods.WriteFile(result_avg_path, true, write_line);

			long larger_time = Utility.Average(total_time);
			if (larger_time * expe_count > 450 * 1000)
				expe_count = (int) (expe_count * 0.5 / (larger_time * expe_count / 450.0 / 1000.0));
			if(expe_count < 1)
				expe_count = 1;

			name_suffix *= times;
		}
		OwnMethods.WriteFile(result_detail_path, true, "\n");
		OwnMethods.WriteFile(result_avg_path, true, "\n");
	}

}
