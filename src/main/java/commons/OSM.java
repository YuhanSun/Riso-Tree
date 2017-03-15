package commons;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

public class OSM {

	public static enum OSMLabel implements Label {
		ReferenceNode,
    }
	
	public static enum OSMRelation implements RelationshipType {
	    FIRST_NODE, LAST_NODE, OTHER, NEXT, OSM, WAYS, RELATIONS, MEMBERS, MEMBER, TAGS, GEOM, BBOX, NODE, CHANGESET, USER, USERS, OSM_USER;
	}
	
}
