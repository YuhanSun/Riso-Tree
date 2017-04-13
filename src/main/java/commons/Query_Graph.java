package commons;

import java.awt.geom.GeneralPath;
import java.util.ArrayList;

public class Query_Graph {
	public int[] label_list;
	public ArrayList<ArrayList<Integer>> graph;
	public MyRectangle[] spa_predicate;
	public boolean[] Has_Spa_Predicate;
	
	public Query_Graph(int node_count) {
		label_list = new int[node_count];
		graph = new ArrayList<ArrayList<Integer>>(node_count);
		for ( int i = 0; i < node_count; i++)
			graph.add(new ArrayList<Integer>());
		spa_predicate = new MyRectangle[node_count];
		Has_Spa_Predicate = new boolean[node_count];
	}
	
	@Override
	public String toString()
	{
		String string = "";
		for ( int i = 0; i < label_list.length; i++)
		{
			ArrayList<Integer> neighbors = graph.get(i);
			string += String.format("%d,%d", i, neighbors.size());
			for (int neighbor : neighbors)
				string += String.format(",%d", neighbor);
			string += "," + String.valueOf(Has_Spa_Predicate[i]);
			string += String.format(",%d\n", label_list[i]);
		}
		
		return string;
	}
}
