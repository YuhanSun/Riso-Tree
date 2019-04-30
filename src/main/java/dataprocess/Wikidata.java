package dataprocess;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import com.google.common.base.CharMatcher;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import commons.Config;
import commons.Entity;
import commons.GraphUtil;
import commons.Neo4jGraphUtility;
import commons.ReadWriteUtil;
import commons.Util;

public class Wikidata {

  /**
   * entity with Q-start
   */
  private final static Pattern entityPattern =
      Pattern.compile("<http://www.wikidata.org/entity/Q(\\d+)>");

  /**
   * property treated as entity in the subject.
   */
  private final static Pattern propertyEntityPattern =
      Pattern.compile("<http://www.wikidata.org/entity/P(\\d+)>");

  /**
   * property treated as predicate
   */
  private final static Pattern propertyPredicatePattern =
      Pattern.compile("<http://www.wikidata.org/prop/direct/P(\\d+)>");

  // private final static Pattern patternContainLanguageMark =
  // Pattern.compile("\\\"(.*?)\\\"@\\S+(-*)(\\S*)");

  private final static String labelStr = "rdf-schema#label";
  private final static String labelPropertyName = "name";
  private final static String descriptionStr = "<http://schema.org/description>";
  private final static String descriptionPropertyName = "description";

  private final static String enStr = "@en";
  private final static String propertyStr = "<http://www.wikidata.org/prop/direct/P";
  private final static String entityStr = "<http://www.wikidata.org/entity/Q";

  private final static String instanceOfStr = "<http://www.wikidata.org/prop/direct/P31>";
  private final static String coordinateStr = "<http://www.wikidata.org/prop/direct/P625>";

  private static final Logger LOGGER = Logger.getLogger(Wikidata.class.getName());
  private static Level loggingLevel = Level.INFO;

  private final static int nodeCountLimit = 50000000;
  private final static int logInterval = 1000000;
  private final static int nodeCount = 47116657;
  private final static int attributeBulkInsertSize = 1000000;

  // for test
  String dir = "";
  String fullfilePath, wikiLabelPath, wikiAttributePath, wikiEdgePath, wikiDescriptionPath;

  // static String dir = "/hdd/code/yuhansun/data/wikidata";
  // static String fullfilePath = dir + "/wikidata-20180308-truthy-BETA.nt";
  String logPath;
  String locationPath;
  String entityMapPath;
  String graphPath, singleGraphPath;
  String graphPropertyEdgePath;
  String entityPath;

  String propertiesJsonFile = dir + "/properties_from_query.json";
  String propertyMapPath = dir + "/property_map.txt";
  String edgePath = dir + "/graph_edges.txt";
  String entityPropertiesPath = dir + "/entity_properties.txt";
  String entityStringLabelMapPath = dir + "/entity_string_label.txt";

  // for loading
  String propertyEdgePath = dir + "/edges_properties.txt";

  String graphLabelPath;
  String dbPath;

  private static Config config = new Config();
  private static String lon_name = config.GetLongitudePropertyName();
  private static String lat_name = config.GetLatitudePropertyName();

  public Wikidata(String homeDir) {
    this(homeDir, "wikidata-20180308-truthy-BETA.nt");
  }

  public Wikidata(String homeDir, String sourceFileName) {
    this(homeDir, sourceFileName, homeDir + "/neo4j-community-3.4.12/data/databases/graph.db");
  }

  public Wikidata(String homeDir, String sourceFileName, String dbPath) {
    this.dir = homeDir;
    fullfilePath = dir + "/" + sourceFileName;
    wikiLabelPath = dir + "/wiki_label.txt";
    wikiAttributePath = dir + "/wiki_attribute.txt";
    wikiEdgePath = dir + "/wiki_edge.txt";
    wikiDescriptionPath = dir + "/wiki_description.txt";

    logPath = dir + "/extract.log";
    locationPath = dir + "/locations.txt";
    entityMapPath = dir + "/entity_map.txt";
    graphPath = dir + "/graph.txt";
    singleGraphPath = dir + "/graph_single.txt";
    graphPropertyEdgePath = dir + "/graph_property_edge.txt";
    entityPath = dir + "/entity.txt";

    propertiesJsonFile = dir + "/properties_from_query.json";
    propertyMapPath = dir + "/property_map.txt";
    edgePath = dir + "/graph_edges.txt";
    entityPropertiesPath = dir + "/entity_properties.txt";
    entityStringLabelMapPath = dir + "/entity_string_label.txt";

    // for loading
    propertyEdgePath = dir + "/edges_properties.txt";
    graphLabelPath = dir + "/graph_label.txt";
    this.dbPath = dbPath;
  }

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    String dir = "D:/Project_Data/wikidata-20180308-truthy-BETA.nt";
    String sourceFilename = "slice_100000.nt";
    String dbPath =
        "D:\\Neo4jData\\neo4jDatabases\\database-ae5a632c-076d-42a6-ac8d-61f8f72af7f9\\installation-3.4.12\\data\\databases\\graph.db";
    Wikidata wikidata = new Wikidata(dir, sourceFilename, dbPath);

    // wikidata.loadAttributesDbService();
    // wikidata.loadAttributes();
    // wikidata.loadEdges();

    // wikidata.cutDescription();
    // wikidata.checkWikiLabelData();

    // extractEntityMap();
    // wikidata.extractEntityToEntityRelationEdgeFormat();
    wikidata.extractEntityToEntityRelation();
    // checkGraphVerticesCount();
    // generateEntityFile();

    // checkLocation();
    // findEntitiesNotOnEarth();
    // removeLocationOutOfEarth();
    // removeLocationOutOfBound();
    // getEdgeCount();

    // extractPropertyID();

    // getLabelCount();
    // extractLabels();
    // extractProperties();
    // wikidata.extractStringLabels();

    // extractPropertyLabelMap();

    // convert data for GraphFrame
    // GraphUtil.convertGraphToEdgeFormat(graphPath, edgePath);
    // GraphUtil.extractSpatialEntities(entityPath, dir + "/entity_spatial.txt");

    // edgeCountCheck();

    // wikidata.loadAllEntities();
    // wikidata.cutPropertyAndEdge();
    // wikidata.extractStringLabels();
  }

  public void checkWikiLabelData() throws Exception {
    BufferedReader reader = new BufferedReader(new FileReader(wikiLabelPath));
    List<Integer> idList = readGraphIdToQIdMap(entityMapPath);
    String line = null;
    TreeSet<Integer> idSet = new TreeSet<>();
    while ((line = reader.readLine()) != null) {
      int id = getId(decodeRow(line)[0]);
      idSet.add(id);
    }
    Util.close(reader);

    // check whether QId has a string label.
    for (int QId : idList) {
      if (!idSet.contains(QId)) {
        LOGGER.info("" + QId);
      }
    }

    LOGGER.info("" + idSet.size());
  }

  public void cutDescription() throws Exception {
    LOGGER.info("read from " + fullfilePath);
    BufferedReader reader = new BufferedReader(new FileReader(fullfilePath));

    LOGGER.info("write description to " + wikiDescriptionPath);
    FileWriter writer = new FileWriter(wikiDescriptionPath);

    String line = null;
    long count = 0;
    while ((line = reader.readLine()) != null) {
      count++;
      if (count % 1000000 == 0) {
        LOGGER.info("" + count);
      }

      if (!line.contains("@en")) {
        continue;
      }

      String[] strings = decodeRow(line);
      String predicate = strings[1];

      if (predicate.equals(descriptionStr)) {
        writer.write(line + "\n");
      }
    }
    reader.close();
    writer.close();
  }

  /**
   * 
   * @throws Exception
   */
  public void cutPropertyAndEdge() throws Exception {
    LOGGER.info("read from " + fullfilePath);
    BufferedReader reader = new BufferedReader(new FileReader(fullfilePath));

    LOGGER.info("write attribute to " + wikiAttributePath);
    FileWriter attrbuteWriter = new FileWriter(wikiAttributePath);

    LOGGER.info("write egde to " + wikiEdgePath);
    FileWriter edgeWriter = new FileWriter(wikiEdgePath);
    String line = null;
    long count = 0;
    while ((line = reader.readLine()) != null) {
      count++;
      if (count % 1000000 == 0) {
        LOGGER.info("" + count);
      }

      String[] strings = decodeRow(line);
      String predicate = strings[1];
      if (!isPropertyPredicate(predicate)) {
        continue;
      }

      String object = strings[2];
      if (isQEntity(object)) {
        edgeWriter.write(line + "\n");
      } else {
        attrbuteWriter.write(line + "\n");
      }
    }
    reader.close();
    attrbuteWriter.close();
    edgeWriter.close();
  }

  /**
   * Cut only the wikidata that contains label "@en" information.
   *
   * @throws Exception
   */
  public void cutLabelFile() throws Exception {
    BufferedReader reader = new BufferedReader(new FileReader(fullfilePath));
    FileWriter writer = new FileWriter(wikiLabelPath);
    String line = null;
    int count = 0;
    while ((line = reader.readLine()) != null) {
      count++;
      if (count % 1000000 == 0) {
        LOGGER.info("" + count);
      }
      if (!line.contains("@en")) {
        continue;
      }
      if (StringUtils.containsIgnoreCase(line, "label")) {
        writer.write(line + "\n");
      }
    }
    reader.close();
    writer.close();
  }


  /**
   * Recover name using batchinserter.
   *
   * @throws Exception
   */
  public void recoverName() throws Exception {
    String[] entityStringMap = readLabelMap(entityStringLabelMapPath);
    LOGGER.info("Batch insert names into: " + dbPath);
    Map<String, String> config = new HashMap<String, String>();
    config.put("dbms.pagecache.memory", "80g");
    BatchInserter inserter = null;
    try {
      inserter = BatchInserters.inserter(new File(dbPath).getAbsoluteFile(), config);
      int graphId = 0;
      for (String strLabel : entityStringMap) {
        if (graphId % logInterval == 0) {
          LOGGER.info("" + graphId);
        }
        if (entityStringMap[graphId] != null) {
          inserter.setNodeProperty(graphId, labelPropertyName, strLabel);
        }
        graphId++;
      }
    } catch (Exception e) {
      e.printStackTrace();
      Util.close(inserter);
    }
    Util.close(inserter);
  }

  // /**
  // * Recover using GraphDatabase. It is very slow..
  // *
  // * @throws Exception
  // */
  // public void recoverName() throws Exception {
  // GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(dbPath);
  // String[] entityStringMap = readLabelMap(entityStringLabelMapPath);
  // LOGGER.info("GraphDb Insert names into: " + dbPath);
  // Transaction tx = service.beginTx();
  // try {
  // int graphId = 0;
  // for (String strLabel : entityStringMap) {
  // if (graphId % logInterval == 0) {
  // LOGGER.info("" + graphId);
  // LOGGER.info(String.format("%s", strLabel));
  // }
  // if (strLabel != null) {
  // Node node = service.getNodeById(graphId);
  // node.setProperty(labelPropertyName, strLabel);
  // }
  // graphId++;
  // }
  // } catch (Exception e) {
  // e.printStackTrace();
  // tx.failure();
  // tx.close();
  // }
  // tx.success();
  // tx.close();
  // service.shutdown();
  // }

  public void recoverSpatialProperty() {
    ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
    LOGGER.info("Batch insert spatial properties into: " + dbPath);
    Map<String, String> config = new HashMap<String, String>();
    config.put("dbms.pagecache.memory", "80g");
    BatchInserter inserter = null;
    try {
      inserter = BatchInserters.inserter(new File(dbPath).getAbsoluteFile(), config);
      for (Entity entity : entities) {
        int graphId = entity.id;
        if (graphId % logInterval == 0) {
          LOGGER.info("" + graphId);
        }
        if (entity.IsSpatial) {
          inserter.setNodeProperty(graphId, lon_name, entity.lon);
          inserter.setNodeProperty(graphId, lat_name, entity.lat);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      Util.close(inserter);
    }

    Util.close(inserter);
  }

  public void loadAttributesDbService() throws Exception {
    ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
    String[] entityStringMap = readLabelMap(entityStringLabelMapPath);

    int[] idMap = readQIdToGraphIdMap(entityMapPath);
    BufferedReader reader = new BufferedReader(new FileReader(entityPropertiesPath));
    GraphDatabaseService service = Neo4jGraphUtility.getDatabaseService(dbPath);
    Transaction tx = service.beginTx();
    String line = null;
    int lineId = 0;
    JsonParser jsonParser = new JsonParser();
    try {
      while ((line = reader.readLine()) != null) {
        lineId++;
        if (lineId % logInterval == 0) {
          LOGGER.info("" + lineId);
        }
        if (lineId % attributeBulkInsertSize == 0) {
          LOGGER.info("close tranction...");
          tx.success();
          tx.close();
          LOGGER.info("transaction closed.");
          Util.close(service);
          service = Neo4jGraphUtility.getDatabaseService(dbPath);
          tx = service.beginTx();
        }
        JsonElement jsonElement = jsonParser.parse(line);
        JsonObject object = jsonElement.getAsJsonObject();
        int QId = object.get("id").getAsInt();
        int graphId = idMap[QId];
        Node node = service.getNodeById(graphId);
        for (String key : object.keySet()) {
          node.setProperty(key, object.get(key).getAsString());
        }

        // spatial attributes
        Entity entity = entities.get(graphId);
        if (entity.IsSpatial) {
          node.setProperty(lon_name, entity.lon);
          node.setProperty(lat_name, entity.lat);
        }

        // use wikidata label as name of a node
        String name = entityStringMap[graphId];
        if (name != null) {
          node.setProperty(labelPropertyName, name);
        }
      }

    } catch (Exception e) {
      Util.close(reader);
      Util.close(service);
      e.printStackTrace();
    }

    if (tx != null) {
      tx.success();
      tx.close();
    }

    Util.close(reader);
    Util.close(service);
  }

  /**
   * Lead to override of the spatial properties loaded in the node loading phase.
   *
   * @throws Exception
   */
  public void loadAttributes() throws Exception {
    ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
    String[] entityStringMap = readLabelMap(entityStringLabelMapPath);

    int[] idMap = readQIdToGraphIdMap(entityMapPath);
    BufferedReader reader = new BufferedReader(new FileReader(entityPropertiesPath));
    LOGGER.info("Batch insert properties into: " + dbPath);
    Map<String, String> config = new HashMap<String, String>();
    config.put("dbms.pagecache.memory", "80g");
    BatchInserter inserter = null;
    String line = null;
    int lineId = 0;
    JsonParser jsonParser = new JsonParser();
    try {
      inserter = BatchInserters.inserter(new File(dbPath).getAbsoluteFile(), config);
      while ((line = reader.readLine()) != null) {
        lineId++;
        if (lineId % logInterval == 0) {
          LOGGER.info("" + lineId);
        }
        // if (lineId % attributeBulkInsertSize == 0) {
        // Util.close(inserter);
        // inserter = BatchInserters.inserter(new File(dbPath).getAbsoluteFile(), config);
        // }
        JsonElement jsonElement = jsonParser.parse(line);
        JsonObject object = jsonElement.getAsJsonObject();
        int QId = object.get("id").getAsInt();
        int graphId = idMap[QId];
        Map<String, Object> addProperties = new HashMap<>();
        for (String key : object.keySet()) {
          addProperties.put(key, object.get(key).getAsString());
        }

        // spatial attributes
        Entity entity = entities.get(graphId);
        if (entity.IsSpatial) {
          addProperties.put(lon_name, entity.lon);
          addProperties.put(lat_name, entity.lat);
        }

        // use wikidata label as name of a node
        String name = entityStringMap[graphId];
        if (name != null) {
          addProperties.put(labelPropertyName, name);
        }

        inserter.setNodeProperties(graphId, addProperties);
      }

    } catch (Exception e) {
      e.printStackTrace();
      Util.close(reader);
      Util.close(inserter);
    }

    Util.close(reader);
    Util.close(inserter);
  }

  public void loadEdges() throws Exception {
    // read the relationshipsTypes
    Map<String, RelationshipType> map = createStringToRelationshipTypeMap(propertyMapPath);

    LOGGER.info("read edges from " + graphPropertyEdgePath);
    BufferedReader reader = new BufferedReader(new FileReader(graphPropertyEdgePath));
    LOGGER.info("Batch insert edges into: " + dbPath);
    Map<String, String> config = new HashMap<String, String>();
    config.put("dbms.pagecache.memory", "80g");
    BatchInserter inserter = null;
    String line = null;
    int lineId = 0;
    try {
      inserter = BatchInserters.inserter(new File(dbPath).getAbsoluteFile(), config);
      while ((line = reader.readLine()) != null) {
        lineId++;
        if (lineId % logInterval == 0) {
          LOGGER.info("" + lineId);
        }
        String[] strings = line.split(",");
        int startId = Integer.parseInt(strings[0]);
        int endId = Integer.parseInt(strings[2]);
        String label = strings[1];
        RelationshipType type = map.get(label);
        if (type == null) {
          LOGGER.info(label);
          inserter.createRelationship(startId, endId, RelationshipType.withName(label), null);
        } else {
          inserter.createRelationship(startId, endId, type, null);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      Util.close(reader);
      Util.close(inserter);
    }

    Util.close(reader);
    Util.close(inserter);
  }

  private static Map<String, RelationshipType> createStringToRelationshipTypeMap(String filepath) {
    LOGGER.info("create the string to relationType map from " + filepath);
    Map<String, RelationshipType> map = new HashMap<>();
    Map<Integer, String> strMap = readPropertyMap(filepath);
    for (String name : strMap.values()) {
      map.put(name, RelationshipType.withName(name));
    }
    return map;
  }

  public void loadAllEntities() throws Exception {
    ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
    ArrayList<ArrayList<Integer>> labels = GraphUtil.ReadGraph(graphLabelPath);
    String[] labelStringMap = readLabelMap(entityStringLabelMapPath);
    loadAllEntity(entities, labelStringMap, labels, dbPath);
  }

  /**
   * Load all entities and generate the id map.
   *
   * @param entities
   * @param labelList
   * @param dbPath
   * @param mapPath
   * @throws Exception
   */
  public static void loadAllEntity(List<Entity> entities, String[] labelStringMap,
      ArrayList<ArrayList<Integer>> labelList, String dbPath) throws Exception {
    LOGGER.info("Batch insert nodes into: " + dbPath);
    Map<String, String> config = new HashMap<String, String>();
    config.put("dbms.pagecache.memory", "80g");
    BatchInserter inserter = null;
    try {
      inserter = BatchInserters.inserter(new File(dbPath).getAbsoluteFile(), config);
      for (int i = 0; i < entities.size(); i++) {
        Entity entity = entities.get(i);
        Map<String, Object> properties = new HashMap<String, Object>();
        List<Label> labels = new ArrayList<>();
        for (int labelId : labelList.get(i)) {
          String labelString = labelStringMap[labelId];
          if (labelString == null) {
            continue;
          }
          Label label = Label.label(labelString);
          labels.add(label);
        }
        if (entity.IsSpatial) {
          properties.put(lon_name, entity.lon);
          properties.put(lat_name, entity.lat);
        }
        inserter.createNode(i, properties, labels.toArray(new Label[labels.size()]));
      }
    } catch (Exception e) {
      e.printStackTrace();
      Util.close(inserter);
    }
    Util.close(inserter);
  }

  /**
   * Check the number of edges in graph.txt and edges.txt.
   *
   * @throws Exception
   */
  public void edgeCountCheck() throws Exception {
    ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graphPath);
    LOGGER.log(loggingLevel, "Edge count in graph: {0}", GraphUtil.getEdgeCount(graph));
    LOGGER.log(loggingLevel, "Edge count in edges file: {0}",
        Files.lines(Paths.get(edgePath)).count());
  }

  public void checkPropertyEntityID() {
    HashMap<Long, Integer> idMap = readMap(entityMapPath);
    ArrayList<Integer> propertySet = ReadWriteUtil.readIntegerArray(dir + "\\propertyID.txt");
    int count = 0;
    for (int id : propertySet) {
      if (idMap.containsKey((long) id)) {
        Util.println(String.format("%d,%d", id, idMap.get((long) id)));
        count++;
      }
    }
    Util.println("count: " + count);
  }

  /**
   * Extract all node properties from the wiki_attribute.txt file. The file only contains lines like
   * '''QEntity predicate object'''. No label or description lines are there. Each row in the output
   * is a JsonObject representing a single node.
   *
   * @throws Exception
   */
  public void extractProperties() throws Exception {
    Map<Integer, String> propertyMap = readPropertyMap(propertyMapPath);
    BufferedReader reader = new BufferedReader(new FileReader(wikiAttributePath));
    FileWriter writer = new FileWriter(entityPropertiesPath);
    String line = null;
    long entityQId = -1;
    int lineIndex = 0;
    JsonObject properties = null;
    while ((line = reader.readLine()) != null) {
      lineIndex++;
      if (lineIndex % logInterval == 0) {
        LOGGER.info("" + lineIndex);
      }

      String[] spo = decodeRow(line);
      if (!isQEntity(spo[0])) {
        continue;
      }
      long curEntityId = getId(spo[0]);
      if (curEntityId != entityQId) {
        // entityId = -1, initialize the properties for the first entity.
        if (properties == null) {
          properties = new JsonObject();
        } else {
          // output the properties as json format for this entity.
          properties.addProperty("id", entityQId);
          writer.write(properties.toString() + "\n");
          properties = new JsonObject();
        }
        entityQId = curEntityId;
      }

      String predicate = spo[1];
      String object = spo[2];

      object = StringUtils.remove(object, "\"");

      // extract the label and description in language English.
      if (object.endsWith(enStr)) {
        if (predicate.contains(labelStr)) {
          properties.addProperty(labelPropertyName, object);
        } else if (predicate.equals(descriptionStr)) {
          properties.addProperty(descriptionPropertyName, object);
        }
      } else if (isQEntity(object)) {
        // skip the entity-to-entity edges.
        continue;
      } else if (isPropertyPredicate(predicate)) {
        // only extract the rows with existing property predicate.
        int propertyId = getId(predicate);
        // LOGGER.log(loggingLevel, propertyId + "");
        String propertyName = propertyMap.get(propertyId);
        // the propertyId does not exist in latest properties ids.
        if (propertyName == null) {
          continue;
        }
        if (properties.has(propertyName)) {
          properties.remove(propertyName);
        }
        properties.addProperty(propertyName, object);
      }
    }

    // handle the last entity.
    if (properties.size() > 0) {
      properties.addProperty("id", entityQId);
      writer.write(properties.toString() + "\n");
    }

    reader.close();
    writer.close();
  }

  /**
   * Extract all the string labels for all Q entities. <graphId, Stringlabel>.
   * 
   * @throws Exception
   */
  public void extractStringLabels() throws Exception {
    int[] map = readQIdToGraphIdMap(entityMapPath);

    LOGGER.info("read from " + wikiLabelPath);
    BufferedReader reader = new BufferedReader(new FileReader(wikiLabelPath));
    FileWriter writer = new FileWriter(entityStringLabelMapPath);
    String line = null;
    int count = 0;
    int QEntityId = -1;
    int labelLevel = 0, descptionLevel = 0;
    String labelString = null, descriptionString = null;
    try {
      while ((line = reader.readLine()) != null) {
        if (count % logInterval == 0) {
          LOGGER.info("" + count);
        }
        count++;

        String[] spo = decodeRow(line);
        if (!isQEntityReg(spo[0])) {
          continue;
        }

        int curQEntityId = getId(spo[0]);
        String predicate = spo[1];
        String object = spo[2];

        if (QEntityId != curQEntityId) {
          if (QEntityId == -1) {
            QEntityId = curQEntityId;
          } else {
            int graphId = map[(int) QEntityId];
            // Util.println(labelString);
            int start = labelString.indexOf("\"");
            int end = labelString.lastIndexOf("\"");
            labelString = labelString.substring(start + 1, end);
            writer.write(String.format("%d,%s\n", graphId, labelString));
            QEntityId = curQEntityId;
            labelString = null;
            labelLevel = 0;
          }
        }

        if (!object.contains(enStr)) {
          continue;
        }

        int curLanguageLevel = getLanguageLevel(object);
        if (curLanguageLevel > labelLevel) {
          labelString = object;
          labelLevel = curLanguageLevel;
        }

        // extract the label and description in language English.
        // if (object.endsWith(enStr) && predicate.contains(labelStr)) {
        //
        // }
      }
    } catch (Exception e) {
      e.printStackTrace();
      Util.println(line);
      Util.println(labelString);
      reader.close();
      writer.close();
    }
    Util.close(reader);
    Util.close(writer);
  }

  public static int getLanguageLevel(String object) {
    if (object.endsWith(enStr)) {
      return 2;
    } else {
      return 1;
    }
  }

  /**
   * Extract the map <id, label> from properties.json. Since the json file is not up-to-date, I
   * change the code extractProperty() to get the correct result.
   *
   * @throws Exception
   */
  public void extractPropertyLabelMap() throws Exception {
    FileWriter writer = new FileWriter(propertyMapPath);
    JSONParser parser = new JSONParser();
    LOGGER.log(loggingLevel, "read from " + propertiesJsonFile);
    JSONArray jsonArray = (JSONArray) parser.parse(new FileReader(propertiesJsonFile));
    for (Object object : jsonArray) {
      JSONObject propertyMap = (JSONObject) object;
      LOGGER.log(loggingLevel, propertyMap.toString());
      String property = propertyMap.get("property").toString();
      String label = propertyMap.get("propertyLabel").toString();
      int propertyId =
          Integer.parseInt(property.replaceAll("http://www.wikidata.org/entity/P", ""));
      writer.write(String.format("%d,%s\n", propertyId, label));
    }
    writer.close();
  }

  /**
   * Extract all property id.
   */
  public void extractPropertyID() {
    BufferedReader reader = null;
    String line = "";
    HashSet<Long> idSet = new HashSet<>();
    int lineIndex = 0;
    try {
      reader = new BufferedReader(new FileReader(new File(fullfilePath)));
      while ((line = reader.readLine()) != null) {
        lineIndex++;
        String[] strings = line.split(" ");
        String subject = strings[0];
        long id = getPropertySubjectIdReg(subject);
        if (id != -1)
          idSet.add(id);

        String object = strings[2];
        id = getPropertySubjectIdReg(object);
        if (id != -1)
          idSet.add(id);

        if (lineIndex % logInterval == 0)
          Util.println(lineIndex);
      }
      reader.close();
      ArrayList<String> output = new ArrayList<>(idSet.size());
      for (long id : idSet)
        output.add(String.valueOf(id));

      ReadWriteUtil.WriteArray(dir + "\\propertyID.txt", output);
    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
    }
  }

  /**
   * Extract labels of format <graphID, list of labels>.
   */
  public void extractEntityLabels() {
    BufferedReader reader = null;
    String line = "";
    HashMap<Long, Integer> idMap = readMap(entityMapPath);
    TreeSet<Integer> hasLabelVertices = new TreeSet<>();
    HashMap<Integer, TreeSet<Integer>> labels = new HashMap<>();
    int count = 0;
    try {
      reader = new BufferedReader(new FileReader(new File(fullfilePath)));
      while ((line = reader.readLine()) != null) {
        String[] strList = decodeRow(line);
        String predicate = strList[1];
        if (predicate.equals(instanceOfStr)) {
          count++;
          String subject = strList[0];
          String object = strList[2];

          if (!isQEntityReg(subject) || !isQEntityReg(object))
            continue;

          int graphID = idMap.get(getQEntityIdReg(subject));
          hasLabelVertices.add(graphID);

          // labelId is the mapped id of the entity.
          int labelID = idMap.get(getQEntityIdReg(object));
          if (!labels.containsKey(graphID))
            labels.put(graphID, new TreeSet<>());
          labels.get(graphID).add(labelID);

          if (count % logInterval == 0)
            Util.println(count);
        }
      }

      // String filePath = dir + "\\hasLabelVertices.txt";
      // ArrayList<String> outputArray = new ArrayList<>(hasLabelVertices.size());
      // for (int id : hasLabelVertices)
      // outputArray.add(String.valueOf(id));
      // ReadWriteUtil.WriteArray(filePath, outputArray);

      Util.println(labels);
      FileWriter writer = new FileWriter(graphLabelPath);
      FileWriter logwriter = new FileWriter(logPath, true);
      writer.write(idMap.size() + "\n");
      for (int key = 0; key < idMap.size(); key++) {
        TreeSet<Integer> keyLabels = labels.get(key);
        if (keyLabels == null) {
          logwriter.write(String.format("%d does not have label\n", key));
          writer.write(String.format("%d,0\n", key));
          continue;
        }
        writer.write(String.format("%d,%d", key, keyLabels.size()));
        for (int id : keyLabels)
          writer.write(String.format(",%d", id));
        writer.write("\n");
      }
      writer.close();
      logwriter.close();

    } catch (Exception e) {
      Util.println(line);
      e.printStackTrace();
    }
  }

  /**
   * Extract labels of format <labelID, set of graphIds>.
   */
  public void extractLabelGraphIds() {
    BufferedReader reader = null;
    String line = "";
    HashMap<Long, Integer> idMap = readMap(entityMapPath);
    TreeSet<Integer> hasLabelVertices = new TreeSet<>();
    HashMap<Integer, TreeSet<Integer>> labels = new HashMap<>();
    int count = 0;
    try {
      reader = new BufferedReader(new FileReader(new File(fullfilePath)));
      while ((line = reader.readLine()) != null) {
        String[] strList = line.split(" ");
        String predicate = strList[1];
        if (predicate.equals(instanceOfStr)) {
          count++;
          String subject = strList[0];
          String object = strList[2];

          if (!isQEntityReg(subject) || !isQEntityReg(object))
            continue;

          int graphID = idMap.get(getQEntityIdReg(subject));
          hasLabelVertices.add(graphID);

          int labelID = idMap.get(getQEntityIdReg(object));
          if (!labels.containsKey(labelID))
            labels.put(labelID, new TreeSet<>());
          labels.get(labelID).add(graphID);

          if (count % logInterval == 0)
            Util.println(count);
        }
      }

      String filePath = dir + "\\hasLabelVertices.txt";
      ArrayList<String> outputArray = new ArrayList<>(hasLabelVertices.size());
      for (int id : hasLabelVertices)
        outputArray.add(String.valueOf(id));
      ReadWriteUtil.WriteArray(filePath, outputArray);

      filePath = dir + "\\labels.txt";
      FileWriter writer = new FileWriter(new File(filePath));
      writer.write(labels.size() + "\n");
      for (int key : labels.keySet()) {
        TreeSet<Integer> verticesID = labels.get(key);
        writer.write(String.format("%d,%d", key, verticesID.size()));
        for (int id : verticesID)
          writer.write(String.format(",%d", id));
        writer.write("\n");
      }
      writer.close();

    } catch (Exception e) {
      Util.println(line);
      e.printStackTrace();
    }
  }

  public void getEdgeCount() {
    ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graphPath);
    int edgeCount = 0;
    for (ArrayList<Integer> neighbors : graph)
      edgeCount += neighbors.size();
    Util.println(edgeCount);
  }

  public void removeLocationOutOfBound() {
    ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
    int count = 0;
    for (Entity entity : entities) {
      if (entity.IsSpatial) {
        if (entity.lon < -180 || entity.lon > 180 || entity.lat < -90 || entity.lat > 90) {
          count++;
          entity.IsSpatial = false;
          entity.lon = 0;
          entity.lat = 0;
        }
      }
    }
    Util.println(count);
    GraphUtil.writeEntityToFile(entities, entityPath);
  }

  public void removeLocationOutOfEarth() {
    BufferedReader reader = null;
    HashMap<Long, Integer> idMap = readMap(entityMapPath);
    ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
    String outearthPath = dir + "\\outofearth_local.csv";
    String line = "";
    try {
      reader = new BufferedReader(new FileReader(new File(outearthPath)));
      while ((line = reader.readLine()) != null) {
        // String[] strList = line.split(",");
        // long wikiID = getEntityID(strList[0]);

        long wikiID = Long.parseLong(line);

        // if (!idMap.containsKey(wikiID))
        // continue;

        int graphID = idMap.get(wikiID);
        Entity entity = entities.get(graphID);
        entity.IsSpatial = false;
        entity.lon = 0;
        entity.lat = 0;
      }
      GraphUtil.writeEntityToFile(entities, dir + "\\new_entity.txt");
    } catch (Exception e) {
      // TODO: handle exception
      Util.println(line);
      e.printStackTrace();
    }
  }

  public void findEntitiesNotOnEarth() {
    String outputPath = dir + "\\outofearth_local.csv";
    FileWriter writer = null;
    BufferedReader reader = null;
    String line = "";
    int predicateCount = 0;
    try {
      writer = new FileWriter(new File(outputPath));
      reader = new BufferedReader(new FileReader(new File(fullfilePath)));
      while ((line = reader.readLine()) != null) {
        String[] strList = line.split(" ");
        String predicate = strList[1];
        if (predicate.matches(propertyPredicatePattern.pattern())) {
          predicateCount++;
          long propertyID = getPropertyPredicateIdReg(predicate);
          if (propertyID == 376) {
            String object = strList[2];
            if (object.matches(entityPattern.pattern())) {
              long planetID = getQEntityIdReg(object);
              if (planetID != 2) {
                String subject = strList[0];
                if (isQEntityReg(subject)) {
                  long subjectWikiID = getQEntityIdReg(subject);
                  writer.write(subjectWikiID + "\n");
                }
              }
            }
          }
          if (predicateCount % logInterval == 0)
            Util.println(predicateCount);
        }
      }
      reader.close();
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  /**
   * Find some entities are not on earth.
   */
  public void checkLocation() {
    ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
    HashMap<String, String> map = ReadWriteUtil.ReadMap(entityMapPath);
    int count = 0;
    for (Entity entity : entities) {
      if (entity.IsSpatial) {
        if (entity.lon < -180 || entity.lon > 180 || entity.lat < -90 || entity.lat > 90) {
          Util.println(entity + " " + map.get("" + entity.id));
          count++;
        }
      }
    }
    Util.println(count);
  }

  public void getRange() {
    ArrayList<Entity> entities = GraphUtil.ReadEntity(entityPath);
    // ArrayList<Entity> entities = Util.ReadEntity(dir + "\\new_entity.txt");
    Util.println(Util.GetEntityRange(entities));
  }

  public void readGraphTest() {
    ArrayList<ArrayList<Integer>> graph = GraphUtil.ReadGraph(graphPath);
  }

  /**
   * Check graph file first line and real number of vertices
   */
  public void checkGraphVerticesCount() {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(new File(graphPath)));
      String line = reader.readLine();
      int nodeCount = Integer.parseInt(line);
      Util.println(nodeCount);

      int index = 0;

      while ((line = reader.readLine()) != null) {
        index++;
      }
      reader.close();
      Util.println(index);
    } catch (Exception e) {
      // TODO: handle exception
      e.printStackTrace();
    }
  }

  public void extractEntityMap() {
    BufferedReader reader;
    FileWriter writer;
    FileWriter logWriter;
    String line = "";
    long lineIndex = 0;

    HashSet<Long> startIdSet = new HashSet<>();
    ArrayList<Long> map = new ArrayList<>(95000000);
    HashSet<Long> leafVerticesSet = new HashSet<>();

    try {
      reader = new BufferedReader(new FileReader(new File(fullfilePath)));
      writer = new FileWriter(entityMapPath);
      logWriter = new FileWriter(logPath);

      long curWikiID = -1;

      while ((line = reader.readLine()) != null) {
        String[] strList = line.split(" ");
        String subject = strList[0];

        if (subject.matches(entityPattern.pattern())) {
          long startID = getQEntityIdReg(subject);
          if (startID != curWikiID) {
            if (startIdSet.contains(startID)) {
              throw new Exception(startID + "already exists before here!");
            } else {
              map.add(startID);
              startIdSet.add(startID);
              leafVerticesSet.remove(startID);
              // Util.Print(startID);
            }
            curWikiID = startID;
          }
        }

        String object = strList[2];
        if (object.matches(entityPattern.pattern())) {
          long endID = getQEntityIdReg(object);
          if (startIdSet.contains(endID) == false)
            leafVerticesSet.add(endID);
        }

        lineIndex++;
        if (lineIndex % logInterval == 0)
          Util.println(lineIndex);

        if (lineIndex == 10000000)
          break;
      }

      Util.println("leaf count: " + leafVerticesSet.size());
      for (long key : leafVerticesSet)
        map.add(key);

      lineIndex = 0;
      for (long id : map) {
        writer.write(String.format("%d,%d\n", lineIndex, id));
        lineIndex++;
      }

      reader.close();
      writer.close();
      logWriter.close();

    } catch (Exception e) {
      Util.println(String.format("line %d:\n%s", lineIndex, line));
      e.printStackTrace();
    }
  }

  /**
   * Extract the file of '''startGraphId,propertyName,endGraphId'''. Skip instance of.
   */
  public void extractEntityToEntityRelationEdgeFormat() {
    BufferedReader reader;
    FileWriter writer;
    String line = "";
    long lineIndex = 0;

    try {
      int[] idMap = readQIdToGraphIdMap(entityMapPath);
      Map<Integer, String> propertyMap = readPropertyMap(propertyMapPath);

      reader = new BufferedReader(new FileReader(new File(wikiEdgePath)));
      writer = new FileWriter(graphPropertyEdgePath);

      while ((line = reader.readLine()) != null) {
        String[] strList = decodeRow(line);
        String subject = strList[0];
        String predicate = strList[1];
        String object = strList[2];

        if (isQEntity(subject) && isPropertyPredicate(predicate) && isQEntity(object)
            && !predicate.equals(instanceOfStr)) {
          int startQId = getId(subject);
          int startGraphId = idMap[startQId];

          int endQID = getId(object);
          int endGraphID = idMap[endQID];

          int propertyId = getId(predicate);
          String propertyName = propertyMap.get(propertyId);

          writer.write(String.format("%d,%s,%d\n", startGraphId, propertyName, endGraphID));

        }
        lineIndex++;
        if (lineIndex % logInterval == 0) {
          LOGGER.info("" + lineIndex);
        }
      }
      reader.close();
      writer.close();
    } catch (Exception e) {
      Util.println(String.format("line %d:\n%s", lineIndex, line));
      e.printStackTrace();
    }
  }

  /**
   * Generate the graph.txt file (single directional). Do not capture the 'instance of' edge.
   */
  public void extractEntityToEntityRelation() {
    BufferedReader reader;
    FileWriter writer;
    String line = "";
    long lineIndex = 0;

    try {
      int[] idMap = readQIdToGraphIdMap(entityMapPath);

      reader = new BufferedReader(new FileReader(new File(wikiEdgePath)));
      writer = new FileWriter(singleGraphPath);
      writer.write(nodeCount + "\n");

      int prevWikiID = 26; // 26 is the QId of first entity in the file.
      TreeSet<Integer> neighbors = new TreeSet<>();
      // Process node by node. Assume that all the spo for the same node are clustered rather than
      // interleaved. So only needs to use keep one set.
      while ((line = reader.readLine()) != null) {
        // Here do not use decode row for fast processing.
        String[] strList = StringUtils.split(line, ' ');
        String subject = strList[0];

        if (isQEntity(subject)) {
          int startID = getId(subject);
          if (prevWikiID != startID) {
            // write the previous node edges
            int graphId = idMap[prevWikiID];
            writeGraphRow(writer, graphId, neighbors);

            // If an entity does not exist in the Subject with relations to another entity, it needs
            // to have id,0 as its row.
            int curGraphId = idMap[startID];
            for (int i = graphId + 1; i < curGraphId; i++) {
              writer.write(String.format("%d,0\n", i));
            }
            neighbors = new TreeSet<>();
            prevWikiID = startID;
          }

          String predicate = strList[1];
          // Skip 'instance of' edge.
          if (!predicate.equals(instanceOfStr)) {
            String object = strList[2];
            if (isQEntity(object)) {
              int endID = getId(object);
              int graphID = idMap[endID];
              neighbors.add(graphID);
            }
          }
        }

        lineIndex++;
        if (lineIndex % logInterval == 0) {
          LOGGER.info("" + lineIndex);
        }

      }
      Util.close(reader);

      int leafID = idMap[prevWikiID];
      if (neighbors.size() > 0) {
        writeGraphRow(writer, leafID, neighbors);
      }
      leafID++;
      for (; leafID < nodeCount; leafID++) {
        writer.write(String.format("%d,0\n", leafID));
      }

      Util.close(writer);
    } catch (Exception e) {
      Util.println(String.format("line %d:\n%s", lineIndex, line));
      e.printStackTrace();
    }
  }

  private void writeGraphRow(FileWriter writer, int graphId, TreeSet<Integer> neighbors)
      throws Exception {
    writer.write(String.format("%d,%d", graphId, neighbors.size()));
    for (int neighbor : neighbors) {
      writer.write("," + neighbor);
    }
    writer.write("\n");
  }

  /**
   * Generate the final entity file from location.txt.
   */
  public void generateEntityFile() {
    BufferedReader reader = null;
    int lineIndex = 0;
    String line = "";
    try {
      Util.println("read map from " + entityMapPath);
      HashMap<Long, Integer> idMap = readMap(entityMapPath);
      Util.println("initialize entities");
      ArrayList<Entity> entities = new ArrayList<>(idMap.size());
      for (int i = 0; i < idMap.size(); i++)
        entities.add(new Entity(i));

      Util.println("read locations from " + locationPath);
      // Process the spatial entities.
      reader = new BufferedReader(new FileReader(new File(locationPath)));
      while ((line = reader.readLine()) != null) {
        String[] strList = line.split(",");
        long wikiID = Long.parseLong(strList[0]);
        int graphID = idMap.get(wikiID);
        String location = strList[1];
        strList = location.split("Point\\(");
        location = strList[1];
        location = location.replace(")", "");
        strList = location.split(" ");
        double lon = Double.parseDouble(strList[0]);
        double lat = Double.parseDouble(strList[1]);
        Entity entity = entities.get(graphID);
        entity.IsSpatial = true;
        entity.lon = lon;
        entity.lat = lat;
        lineIndex++;
      }
      reader.close();

      GraphUtil.writeEntityToFile(entities, entityPath);
    } catch (Exception e) {
      Util.println(lineIndex);
      Util.println(line);
      e.printStackTrace();
    }
  }

  /**
   * Extract only the spatial entities. The input example is '''<http://www.wikidata.org/entity/Q26>
   * <http://www.wikidata.org/prop/direct/P625> "Point(-5.84
   * 54.590933333333)"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .''' The output format is
   * '''wikiId,POINT(lon lat)'''.
   */
  public void extract() {
    BufferedReader reader;
    FileWriter writer;
    FileWriter logWriter;
    String line = "";
    int p276Count = 0, p625Count = 0; // p276 is not for coordinate
    long index = 0;
    try {
      reader = new BufferedReader(new FileReader(new File(fullfilePath)));
      writer = new FileWriter(locationPath);
      logWriter = new FileWriter(logPath);
      while ((line = reader.readLine()) != null) {
        String[] strList = line.split(" ");
        String predicate = strList[1];
        if (predicate.equals(coordinateStr)) {
          p625Count++;
          if (line.contains("\"")) {
            String subject = strList[0];
            int wikiID = getQEntityIdReg(subject);
            strList = line.split("\"");
            String pointString = strList[1];
            writer.write(wikiID + "," + pointString + "\n");
          } else
            logWriter.write(line + "\n");
        }

        index++;

        if (index % logInterval == 0)
          Util.println(index);

        // if (index == 100000)
        // break;
      }
      reader.close();
      writer.close();
      logWriter.close();

      Util.println("p625Count: " + p625Count);
    } catch (Exception e) {
      Util.println(String.format("line %d:\n%s", index, line));
      e.printStackTrace();
    }
  }

  /**
   * Read the map <wikiID, graphID>.
   *
   * @param mapPath
   * @return
   */
  public static HashMap<Long, Integer> readMap(String mapPath) {
    LOGGER.log(loggingLevel, "readMap from {0}", mapPath);
    BufferedReader reader = null;
    String line = "";
    try {
      reader = new BufferedReader(new FileReader(new File(mapPath)));
      HashMap<Long, Integer> idMap = new HashMap<>();
      while ((line = reader.readLine()) != null) {
        String[] strList = line.split(",");
        int graphID = Integer.parseInt(strList[0]);
        long wikiID = Long.parseLong(strList[1]);
        idMap.put(wikiID, graphID);
      }
      reader.close();
      return idMap;
    } catch (Exception e) {
      if (reader != null)
        try {
          reader.close();
        } catch (IOException e1) {
          e1.printStackTrace();
        }
    }
    return null;
  }


  /**
   * Assume that the string is following the format '''%digit+%'''. Will extract all digits and
   * return them as a number.
   *
   * @param string
   * @return
   */
  public static int getId(String string) {
    return Integer.parseInt(CharMatcher.digit().retainFrom(string));
  }

  /**
   * Extract the id of a property when it is a predicate.
   *
   * @param string
   * @return
   * @throws Exception
   */
  public static int getPropertyPredicateIdReg(String string) throws Exception {
    Matcher matcher = propertyPredicatePattern.matcher(string);
    if (matcher.find()) {
      return Integer.parseInt(matcher.group(1));
    }
    throw new Exception(string + " is not a property in predicate!");
  }

  public static boolean isPropertyPredicate(String predicate) {
    return predicate.startsWith(propertyStr);
  }

  public static boolean isPropertyPredicateReg(String string) {
    return string.matches(propertyPredicatePattern.pattern());
  }

  // public static boolean isProperty(String string) {
  // if (string.matches("<https://www.wikidata.org/wiki/Property:P\\d+>"))
  // return true;
  // else
  // return false;
  // }


  /**
   * Get the id of a property in subject.
   * 
   * @param string
   * @return
   * @throws Exception
   */
  public static long getPropertySubjectIdReg(String string) throws Exception {
    Matcher m = propertyEntityPattern.matcher(string);
    if (m.find()) {
      return Long.parseLong(m.group(1));
    }
    throw new Exception(string + " is not a property entity!");
  }

  public static boolean isPropertySubjectReg(String string) {
    return string.matches(propertyEntityPattern.pattern());
  }


  /**
   * Extract the id of an entity. An entity means Q-entity.
   *
   * @param string
   * @return
   * @throws Exception
   */
  public static int getQEntityIdReg(String string) throws Exception {
    Matcher m = entityPattern.matcher(string);
    if (m.find()) {
      return Integer.parseInt(m.group(1));
    }
    throw new Exception(string + " is not an Q-entity!");
  }

  public static boolean isQEntity(String string) {
    return string.startsWith(entityStr);
  }

  /**
   * Whether is an Q-entity.
   *
   * @param string
   * @return
   */
  public static boolean isQEntityReg(String string) {
    return string.matches(entityPattern.pattern());
  }

  /**
   * Decode the row into [subject, predicate, object, "."].
   *
   * @param line
   * @return
   */
  public static String[] decodeRow(String line) {
    String[] strings = StringUtils.split(line, " ");
    if (strings.length == 4) {
      return strings;
    }
    String[] res = new String[4];
    res[0] = strings[0];
    res[1] = strings[1];
    res[2] = strings[2];
    res[3] = strings[strings.length - 1];
    for (int i = 3; i < strings.length - 1; i++) {
      res[2] += " " + strings[i];
    }
    return res;
  }

  // public static Map<Integer, RelationshipType> readPropertyMapInEdgeLabel(String filepath) {
  // LOGGER.log(loggingLevel, "read property map from " + filepath);
  // HashMap<String, String> map = ReadWriteUtil.ReadMap(filepath);
  // HashMap<Integer, RelationshipType> propertyMap = new HashMap<>();
  // for (String key : map.keySet()) {
  // propertyMap.put(Integer.parseInt(key), RelationshipType.withName(map.get(key)));
  // }
  // return propertyMap;
  // }

  /**
   * Read the property map <PId, StringLabel>.
   *
   * @param filepath
   * @return
   */
  public static Map<Integer, String> readPropertyMap(String filepath) {
    LOGGER.log(loggingLevel, "read property map from " + filepath);
    HashMap<String, String> map = ReadWriteUtil.ReadMap(filepath);
    HashMap<Integer, String> propertyMap = new HashMap<>();
    for (String key : map.keySet()) {
      propertyMap.put(Integer.parseInt(key), map.get(key));
    }
    return propertyMap;
  }


  public static int[] readQIdToGraphIdMap(String filepath) throws Exception {
    List<Integer> entityIdMap = readGraphIdToQIdMap(filepath);
    int maxQId = Collections.max(entityIdMap);
    // get the reversed map from <graphid, Qid> to generate the map <Qid, graphid>.
    LOGGER.log(loggingLevel, "generate reversed map");
    int[] map = new int[maxQId + 1];
    Arrays.fill(map, -1);

    int graphId = 0;
    for (int QId : entityIdMap) {
      map[QId] = graphId;
      graphId++;
    }
    return map;
  }

  /**
   * Read the map <graphId, QId>.
   *
   * @param filepath
   * @return
   * @throws Exception
   */
  public static List<Integer> readGraphIdToQIdMap(String filepath) throws Exception {
    LOGGER.log(loggingLevel, "read map from " + filepath);
    BufferedReader reader = new BufferedReader(new FileReader(filepath));
    String line = null;
    List<Integer> entityIdMap = new LinkedList<>();
    int index = 0;
    while ((line = reader.readLine()) != null) {
      String[] strings = line.split(",");
      int graphId = Integer.parseInt(strings[0]);
      if (index != graphId) {
        reader.close();
        throw new Exception("graph id inconsistency!");
      }

      int Qid = Integer.parseInt(strings[1]);
      entityIdMap.add(Qid);
      index++;
    }
    reader.close();
    return entityIdMap;
  }

  /**
   * Read the map <entity graphId, label String label>.
   *
   * @param filepath
   * @return
   * @throws Exception
   */
  public static String[] readLabelMap(String filepath) throws Exception {
    LOGGER.info("read Label map from " + filepath);
    String[] map = ReadWriteUtil.readMapAsArray(filepath, nodeCountLimit);
    return map;
  }
}
