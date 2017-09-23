package commons;

public class Config 
{ 
	public static enum system {
		Ubuntu, Windows
	}
	
	public static enum Explain_Or_Profile {
		Explain, Profile, Nothing
	}
	
	public static enum Datasets {
		Patents_100_random_80, Patents_100_random_60,
		Patents_100_random_40, Patents_100_random_20,
		
		Patents_10_random_20,Patents_10_random_80,
		Patents_1_random_80,Patents_1_random_20,
		go_uniprot_100_random_80,
		foursquare, foursquare_10, foursquare_100,
		Gowalla, Gowalla_10, Gowalla_100, Gowalla_25, Gowalla_50, 
		Yelp, Yelp_10, Yelp_100,
	}
	
	public Config() 
	{

	}
	
	private String SERVER_ROOT_URI = "http://localhost:7474/db/data";

	private String longitude_property_name = "lon";
	private String latitude_property_name = "lat";
	private String password = "syh19910205";
	
	//attention here, these settings change a lot
	private String neo4j_version = "neo4j-community-3.1.1";
	private system operatingSystem = system.Windows;
	private String dataset = Datasets.Gowalla_25.name();
	
	private int MAX_HOPNUM = 0;
	private int MAX_HMBR_HOPNUM = 3;
	private int nonspatial_label_count = 25;

	private String Rect_minx_name = "minx";
	private String Rect_miny_name = "miny";
	private String Rect_maxx_name = "maxx";
	private String Rect_maxy_name = "maxy";
	
	
	public void setDatasetName(String pName)
	{
		this.dataset = pName;
	}
	
	public void setMAXHOPNUM(int pMAXHOPNUM)
	{
		this.MAX_HOPNUM = pMAXHOPNUM;
	}

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
	
	public int getMaxHMBRHopNum()
	{
		return MAX_HMBR_HOPNUM;
	}
	
	public system getSystemName()
	{
		return operatingSystem;
	}
	
	public String getDatasetName()
	{
		return dataset;
	}
	
	public String getPassword()
	{
		return password;
	}
	
	public int getNonSpatialLabelCount()
	{
		return nonspatial_label_count;
	}
}