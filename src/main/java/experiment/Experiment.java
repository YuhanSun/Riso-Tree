package experiment;

import java.util.ArrayList;
import java.util.HashMap;

import commons.*;
import graph.*;

public class Experiment {
	
	static String dataset = "Gowalla";
	static Config config = new Config();
	static String version = config.GetNeo4jVersion();
//	static String db_path = String.format("/home/yuhansun/Documents/GeoGraphMatchData/%s_%s/data/databases/graph.db", version, dataset);
//	static String querygraph_path = "/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/query_graph.txt";
	static String db_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\%s_%s\\data\\databases\\graph.db", dataset, version, dataset);
	static String querygraph_path = "D:\\Ubuntu_shared\\GeoMinHop\\query\\query_graph.txt";
	static String graph_pos_map_path = "D:\\Ubuntu_shared\\GeoMinHop\\data\\" + dataset + "\\node_map.txt";

	static boolean TEST_FORMAT = false;
	
	public static void main(String[] args) {
		SpatialFirstList(5);
//		SpatialFirst(5);
	}
	
	public static void SpatialFirstList(int query_id)
	{
		long start;
		long time;
		int limit = -1;
		int expe_count = 15;
		
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
		Query_Graph query_Graph = queryGraphs.get(query_id);
		
//		String result_detail_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/experiment_result/%s/spa_first_%d_API.txt", dataset, query_id);
//		String result_avg_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/experiment_result/%s/spa_first_%d_API_avg.txt", dataset, query_id);
		String result_detail_path = String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s\\spa_first_list_%d_API.txt", dataset, query_id);
		String result_avg_path = String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s\\spa_first_list_%d_API_avg.txt", dataset, query_id);
		
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
		while ( name_suffix <= 200)
		{
			double selectivity = name_suffix / 1000000.0;
//			String queryrect_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_%d.txt", dataset, name_suffix);
			String queryrect_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\spa_predicate\\%s\\queryrect_%d.txt", dataset, name_suffix);
			
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
	}
	
	public static void SpatialFirst(int query_id)
	{
		long start;
		long time;
		int limit = -1;
		int expe_count = 15;
		
		ArrayList<Query_Graph> queryGraphs = Utility.ReadQueryGraph_Spa(querygraph_path, query_id + 1);
		Query_Graph query_Graph = queryGraphs.get(query_id);
		
//		String result_detail_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/experiment_result/%s/spa_first_%d_API.txt", dataset, query_id);
//		String result_avg_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/experiment_result/%s/spa_first_%d_API_avg.txt", dataset, query_id);
		String result_detail_path = String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s\\spa_first_%d_API.txt", dataset, query_id);
		String result_avg_path = String.format("D:\\Google_Drive\\Experiment_Result\\Riso-Tree\\%s\\spa_first_%d_API_avg.txt", dataset, query_id);
		
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
		while ( name_suffix <= 200)
		{
			double selectivity = name_suffix / 1000000.0;
//			String queryrect_path = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/query/spa_predicate/%s/queryrect_%d.txt", dataset, name_suffix);
			String queryrect_path = String.format("D:\\Ubuntu_shared\\GeoMinHop\\query\\spa_predicate\\%s\\queryrect_%d.txt", dataset, name_suffix);
			
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
	}

}
