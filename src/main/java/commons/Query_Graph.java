package commons;

import java.awt.geom.GeneralPath;
import java.util.ArrayList;
import java.util.HashMap;

public class Query_Graph {
	public int[] label_list;
	public ArrayList<ArrayList<Integer>> graph;
	public MyRectangle[] spa_predicate;
	public boolean[] Has_Spa_Predicate;
	
	//statistic
	public HashMap<Integer, Integer> labelDistribution;
	public int highestSelectivityLabel;
	
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
	
	public void iniStatistic()
	{
		labelDistribution = new HashMap<Integer, Integer>();
		for ( int label : label_list)
			if( labelDistribution.containsKey(label) == false)
				labelDistribution.put(label, 0);
		
		for ( int label : label_list)
			labelDistribution.put(label, labelDistribution.get(label) + 1);
		
		int highestSelectivity = Integer.MAX_VALUE;
		for ( int key : labelDistribution.keySet())
			if ( labelDistribution.get(key) < highestSelectivity )
			{
				highestSelectivity = labelDistribution.get(key);
				highestSelectivityLabel = key;
			}
	}
	
	/**
	 * decide whether a given query graph is the isomorphism
	 * spatial predicate is not considered in this case
	 * @param query_Graph
	 * @return
	 */
	public boolean isIsomorphism(Query_Graph query_Graph)
	{
		if ( label_list.length != query_Graph.label_list.length)
			return false;
		
		HashMap<Integer, Integer> p_labelDistribution = query_Graph.labelDistribution;
		
		if ( labelDistribution.size() != query_Graph.labelDistribution.size())
			return false;
		
		for ( int key : labelDistribution.keySet())
		{
			if ( p_labelDistribution.containsKey(key) == false )
				return false;
			if ( p_labelDistribution.get(key) != labelDistribution.get(key) )
				return false;
		}
		
		return true;
		
	}
}
