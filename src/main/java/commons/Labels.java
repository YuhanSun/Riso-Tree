package commons;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

public class Labels {

  public static enum OSMLabel implements Label {
    ReferenceNode, OSM_NODE
  }

  public static enum OSMRelation implements RelationshipType {
    FIRST_NODE, LAST_NODE, OTHER, NEXT, OSM, WAYS, RELATIONS, MEMBERS, MEMBER, TAGS, GEOM, BBOX, NODE, CHANGESET, USER, USERS, OSM_USER, LAYERS
  }

  public static enum GraphLabel implements Label {
    GRAPH_0, GRAPH_1
  }

  public static enum GraphRel implements RelationshipType {
    GRAPH_LINK
  }

  public static enum RTreeRel implements RelationshipType {

    LAYER, RTREE_METADATA, RTREE_ROOT, RTREE_CHILD, RTREE_REFERENCE
  }

}
