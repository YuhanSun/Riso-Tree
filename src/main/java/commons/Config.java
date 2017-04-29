package commons;

public class Config 
{ 
	public static enum system {
		Ubuntu, Windows
	}
	
	public static enum Explain_Or_Profile {
		Explain, Profile, Nothing
	}
	
	public Config() 
	{

	}
	private String SERVER_ROOT_URI = "http://localhost:7474/db/data";

	private String longitude_property_name = "lon";
	private String latitude_property_name = "lat";
	
	private String neo4j_version = "neo4j-community-3.1.1";
	private system operatingSystem = system.Windows;

	private String Rect_minx_name = "minx";
	private String Rect_miny_name = "miny";
	private String Rect_maxx_name = "maxx";
	private String Rect_maxy_name = "maxy";
	
	private int MAX_HOPNUM = 2;

	public String GetServerRoot() {
		return SERVER_ROOT_URI;
	}

	public String GetLongitudePropertyName() {
		return longitude_property_name;
	}

	public String GetLatitudePropertyName() {
		return latitude_property_name;
	}

	public String [] GetRectCornerName() 
	{
		String [] rect_corner_name = new String[4];
		rect_corner_name[0] = this.Rect_minx_name;
		rect_corner_name[1] = this.Rect_miny_name;
		rect_corner_name[2] = this.Rect_maxx_name;
		rect_corner_name[3] = this.Rect_maxy_name;
		return rect_corner_name;
	}

	public String GetNeo4jVersion()
	{
		return neo4j_version;
	}
	
	public int getMaxHopNum()
	{
		return MAX_HOPNUM;
	}
	
	public system getSystemName()
	{
		return operatingSystem;
	}
}