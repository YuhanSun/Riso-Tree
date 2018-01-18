package graph;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import commons.OwnMethods;
import commons.Query_Graph;
import commons.*;
import commons.Config.Explain_Or_Profile;

/**
 * this class is graphfirst approach,
 * spatial predicate is checked during
 * graph search. (spatial predicate is
 * integrated into cypher query)
 * @author yuhansun
 *
 */
public class Naive_Neo4j_Match {

	public String lon_name;
	public String lat_name;

	//neo4j graphdb service
	public Neo4j_API neo4j_API;

	String minx_name;
	String miny_name;
	String maxx_name;
	String maxy_name;

	public int query_node_count;

	public int[] neo4j_time;
	public int[] hmbr_check_time;
	public int[] spa_check_time;
	public long start;

	public Naive_Neo4j_Match(String db_path)
	{
		neo4j_API = new Neo4j_API(db_path);
		Config config = new Config();
		lon_name = config.GetLongitudePropertyName();
		lat_name = config.GetLatitudePropertyName();

		String[] rect_corner_name = config.GetRectCornerName();
		minx_name = rect_corner_name[0];
		miny_name = rect_corner_name[1];
		maxx_name = rect_corner_name[2];
		maxy_name = rect_corner_name[3];
	}

    /**
     * subgraph isomorphism with spatial predicate.
     * It uses java api, ensure that instance is constructed
     * with db_path.
     * @param query_Graph
     * @param limit
     * @return
     */
    public Result SubgraphMatch_Spa_API(Query_Graph query_Graph, int limit)//use neo4j query
	{
		String query = FormCypherQuery(query_Graph, limit, true);
		OwnMethods.Print(query);
		
		Result result = neo4j_API.graphDb.execute(query);
		return result;
	}
    
//    public Result Explain_SubgraphMatch_Spa_API(Query_Graph query_Graph, int limit)//use neo4j query
//	{
//		String query = FormCypherQuery(query_Graph, limit, false);
//		OwnMethods.Print(query);
//		
//		Result result = neo4j_API.graphDb.execute(query);
//		return result;
//	}
    
    /**
     * for the cypher query for profile or explain with given query graph
     * @param query_Graph
     * @param limit
     * @param Profile_Or_Explain	set to true if profile, otherwise false
     * @return
     */
	public String FormCypherQuery(Query_Graph query_Graph, int limit, boolean Profile_Or_Explain)
	{
		String query = "";
		if(Profile_Or_Explain)
			query += "profile match ";
		else
			query += "explain match ";
		
		//label
		query += String.format("(a0:GRAPH_%d)", query_Graph.label_list[0]);
		for(int i = 1; i < query_Graph.graph.size(); i++)
		{
			query += String.format(",(a%d:GRAPH_%d)",i, query_Graph.label_list[i]);
		}
		
		//edge
		for(int i = 0; i<query_Graph.graph.size(); i++)
		{
			for(int j = 0;j<query_Graph.graph.get(i).size();j++)
			{
				int neighbor = query_Graph.graph.get(i).get(j);
				if(neighbor > i)
					query += String.format(",(a%d)--(a%d)", i, neighbor);
			}
		}
		
		//spatial predicate
		int i = 0;
		for(; i < query_Graph.label_list.length; i++)
			if(query_Graph.spa_predicate[i] != null)
			{
				MyRectangle qRect = query_Graph.spa_predicate[i];
				query += String.format(" where %f <= a%d.%s <= %f ", qRect.min_x, i, lon_name, qRect.max_x);
				query += String.format("and %f <= a%d.%s <= %f", qRect.min_y, i, lat_name, qRect.max_y);
				i++;
				break; 
			}
		for(; i < query_Graph.label_list.length; i++)
			if(query_Graph.spa_predicate[i] != null)
			{
				MyRectangle qRect = query_Graph.spa_predicate[i];
				query += String.format(" and %f <= a%d.%s <= %f ", qRect.min_x, i, lon_name, qRect.max_x);
				query += String.format("and %f <= a%d.%s <= %f", qRect.min_y, i, lat_name, qRect.max_y);
			}
		
		//return
		query += " return id(a0)";
		for(i = 1; i<query_Graph.graph.size(); i++)
			query += String.format(",id(a%d)", i);
		
		if(limit != -1)
			query += String.format(" limit %d", limit);
		
		return query;
	}

	public String formQueryJoin(Query_Graph query_Graph, double distance,
			Explain_Or_Profile explain_Or_Profile)
	{
		String query = "";
		switch (explain_Or_Profile) {
		case Profile:
			query += "profile match ";
			break;
		case Explain:
			query += "explain match ";
			break;			
		case Nothing:
			query += "match ";
			break;
		}
		
		//label
		query += String.format("(a0:GRAPH_%d)", query_Graph.label_list[0]);
		for(int i = 1; i < query_Graph.graph.size(); i++)
		{
			query += String.format(",(a%d:GRAPH_%d)",i, query_Graph.label_list[i]);
		}
		
		//edge
		for(int i = 0; i<query_Graph.graph.size(); i++)
		{
			for(int j = 0;j<query_Graph.graph.get(i).size();j++)
			{
				int neighbor = query_Graph.graph.get(i).get(j);
				if(neighbor > i)
					query += String.format(",(a%d)--(a%d)", i, neighbor);
			}
		}
		
		query += " where ";
		//spatial predicate
		ArrayList<Integer> pos = new ArrayList<>();
		for (int i = 0; i < query_Graph.Has_Spa_Predicate.length; i++)
			if ( query_Graph.Has_Spa_Predicate[i])
				pos.add(i);
		query += String.format("(a%1$d.%3$s - a%2$d.%3$s)*(a%1$d.%3$s - a%2$d.%3$s) + "
				+ "(a%1$d.%4$s - a%2$d.%4$s)*(a%1$d.%4$s - a%2$d.%4$s) <= %5$f", 
				pos.get(0), pos.get(1), lon_name, lat_name, distance*distance);
		
		//return
		query += String.format(" return id(a%d), id(a%d)", pos.get(0), pos.get(1));
		
		return query;
	}
	
	public List<Long[]> LAGAQ_Join(Query_Graph query_Graph, double distance) 
	{
		List<Long[]> res = new LinkedList<>();
		String query = formQueryJoin(query_Graph, distance, Explain_Or_Profile.Profile);
		OwnMethods.Print(query);
		Transaction tx = neo4j_API.graphDb.beginTx();
		Result result = neo4j_API.graphDb.execute(query);
		int count = 0;
		while(result.hasNext())
		{
			OwnMethods.Print(result.next());
			count++;
		}
		OwnMethods.Print(count);
		return res;
	}
}
