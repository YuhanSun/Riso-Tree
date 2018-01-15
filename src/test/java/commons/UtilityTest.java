package commons;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Str;

import com.vividsolutions.jts.index.strtree.STRtree;

import commons.Config.system;

public class UtilityTest {
	
	static Config config = new Config();
	static String dataset = config.getDatasetName();
	static system systemName = config.getSystemName();
	static String entityPath;

	@Before
	public void setUp() throws Exception {
		switch (systemName) {
		case Ubuntu:
			entityPath = String.format("/mnt/hgfs/Ubuntu_shared/GeoMinHop/data/%s/entity.txt", dataset);
			break;
		case Windows:
			String dataDirectory = "D:\\Ubuntu_shared\\GeoMinHop\\data";
			entityPath = String.format("D:\\Ubuntu_shared\\GeoMinHop\\data\\%s\\entity.txt", dataset);
		default:
			break;
		}
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void distanceConvertTest()
	{
		float lat1 = 10, lat = 12, lon1 = 0, lon2 = 2;
		OwnMethods.Print("Distance:" + Utility.distFrom(lat1, lon1, lat, lon2));
	}
	
	@Test
	public void distanceTest()
	{
//		double x = 0, y = 0;
		double x = 1.5, y = 1.5;
		MyRectangle rectangle = new MyRectangle(1, 1, 2, 2);
		OwnMethods.Print(Utility.distance(x, y, rectangle));
	}
	
	@Test
	public void distanceRRTest()
	{
		MyRectangle rectangle1 = new MyRectangle(1, 1, 2, 2);
		MyRectangle rectangle2 = new MyRectangle(0, 3, 0, 3);
		OwnMethods.Print(Utility.distance(rectangle1, rectangle2));
	}
	
	@Test
	public void groupSumTest()
	{
		ArrayList<Integer> input = new ArrayList<Integer>(Arrays.asList(0,1,2));
		ArrayList<Integer> result = Utility.groupSum(0, 2, input, 3);
		OwnMethods.Print(result);
	}
	
	@Test
	public void distanceQueryTest()
	{
		OwnMethods.Print(entityPath);
		ArrayList<Entity> entities = OwnMethods.ReadEntity(entityPath);
		double distance = 10;
		
		STRtree stRtree = OwnMethods.ConstructSTRee(entities);
		long start = System.currentTimeMillis();
		MyPoint point = new MyPoint(500, 500);	
		LinkedList<Entity> result = Utility.distanceQuery(stRtree, point, distance);
		long time = System.currentTimeMillis() - start;
		OwnMethods.Print(String.format("time using index: %d", time));
		OwnMethods.Print("result size: " + result.size());
		
		start = System.currentTimeMillis();
		int count = 0;
		for ( Entity entity: entities)
			if (Utility.distance(entity.lon, entity.lat, point.x, point.y) <= distance)
				count++;
		time = System.currentTimeMillis() - start;
		OwnMethods.Print("time no index: " + time);
		OwnMethods.Print("result size " + count);
	}
	
	@Test
	public void sortedListIntersectTest()
	{
		ArrayList<Integer> l1 = new ArrayList<>(Arrays.asList(1, 2, 4));
		int[] l2 = {3};
		OwnMethods.Print(Utility.sortedListIntersect(l1, l2));
		
		l1 = new ArrayList<>();
		l1.add(1);
		int [] l3 = {1, 2, 3, 4};
		OwnMethods.Print(Utility.sortedListIntersect(l1, l3));
		
	}
	
	@Test
	public void isSortedIntersectTest()
	{
		ArrayList<Integer> l1 = new ArrayList<>(Arrays.asList(1, 2, 4));
		ArrayList<Integer> l2 = new ArrayList<>(Arrays.asList(3));
		OwnMethods.Print(Utility.isSortedIntersect(l1, l2));
		
		l1 = new ArrayList<>(Arrays.asList(1, 2, 4));
		l2 = new ArrayList<>(Arrays.asList(2));
		OwnMethods.Print(Utility.isSortedIntersect(l1, l2));
	}
}
