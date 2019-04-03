package experiment;

import java.awt.Graphics2D;
import java.awt.GridLayout;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import javax.imageio.ImageIO;
import javax.swing.JFrame;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import commons.Config.Datasets;
import commons.DrawRectangles;
import commons.MyRectangle;
import commons.Neo4jGraphUtility;
import commons.RTreeUtility;
import commons.Util;

public class DrawRectanglesFrame extends JFrame {

  DrawRectangles drawRectangles;
  List<MyRectangle> rectangles;
  MyRectangle spaceExtend;
  MyRectangle drawExtend;

  public DrawRectanglesFrame(MyRectangle spaceExtend, MyRectangle drawExtend,
      List<MyRectangle> rectangles) {
    this.spaceExtend = spaceExtend;
    this.drawExtend = drawExtend;
    this.rectangles = rectangles;

    setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    setSize((int) (drawExtend.max_x - drawExtend.min_x),
        (int) (drawExtend.max_y - drawExtend.min_y));
    setResizable(false);
    setTitle("leaf mbrs");

    init();
  }

  public void saveAsImage(String filepath) throws IOException {
    BufferedImage image = new BufferedImage(getWidth(), getHeight(), BufferedImage.TYPE_INT_RGB);
    Graphics2D graphics2D = image.createGraphics();
    paint(graphics2D);
    ImageIO.write(image, "jpeg", new File(filepath));
  }

  public void init() {
    setLocationRelativeTo(null);
    setLayout(new GridLayout(1, 1, 0, 0));

    drawRectangles = new DrawRectangles(spaceExtend, drawExtend, rectangles);
    add(drawRectangles);

    setVisible(true);

    drawRectangles = new DrawRectangles(spaceExtend, drawExtend, rectangles);
  }

  public static void main(String[] args) {
    try {
      // MyRectangle drawExtend = new MyRectangle(0, 0, 1000, 500);

      // use one non-leaf node for test
      // String dbPath =
      // "/Users/zhouyang/Google
      // Drive/Projects/tmp/risotree/Yelp/neo4j-community-3.1.1/data/databases/graph.db";
      // GraphDatabaseService databaseService = Neo4jGraphUtility.getDatabaseService(dbPath);

      String dataset = Datasets.Patents_100_random_80.name();
      String dir = "D:\\temp\\Patents";
      String dbPath = dir + "\\neo4j-community-3.1.1\\data\\databases\\graph.db";
      GraphDatabaseService databaseService = Neo4jGraphUtility.getDatabaseService(dbPath);
      String imageDir = dir + "\\images\\";


      Transaction tx = databaseService.beginTx();
      Node root = RTreeUtility.getRTreeRoot(databaseService, dataset);

      if (root == null) {
        throw new Exception("layer does not exist: " + dataset);
      }

      Iterable<Relationship> rels = root.getRelationships(Direction.OUTGOING);
      Iterator<Relationship> iterator = rels.iterator();
      int index = 0;
      while (iterator.hasNext()) {
        Relationship relationship = iterator.next();
        Node child = relationship.getEndNode();
        DrawRectanglesFrame frame = visualizeChildrenMBR(child, 1000);
        frame.saveAsImage(imageDir + index + ".jpeg");
        index++;
        if (index == 10) {
          break;
        }
      }
      tx.success();
      tx.close();
      databaseService.shutdown();

      // use entity for testing
      // String entityPath = "/Users/zhouyang/Google Drive/Projects/tmp/risotree/Yelp/entity.txt";
      // List<Entity> entities = OwnMethods.ReadEntity(entityPath);
      //
      // for (Entity entity : entities) {
      // if (entity.IsSpatial) {
      // rectangles.add(new MyRectangle(entity.lon, entity.lat, entity.lon + 1, entity.lat + 1));
      // }
      // }

    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static DrawRectanglesFrame visualizeChildrenMBR(Node node, int width) {
    List<MyRectangle> rectangles = new ArrayList<>();
    double[] bbox = (double[]) node.getProperty("bbox");
    Util.println("bbox: " + Arrays.toString(bbox));
    MyRectangle spaceExtend = new MyRectangle(bbox[0], bbox[1], bbox[2], bbox[3]);
    double height = (bbox[3] - bbox[1]) / (bbox[2] - bbox[0]) * width;
    MyRectangle drawExtend = new MyRectangle(0, 0, width, height);
    Util.println("draw extend: " + drawExtend);

    Iterable<Relationship> rels = node.getRelationships();
    Iterator<Relationship> iterator = rels.iterator();
    while (iterator.hasNext()) {
      Node child = iterator.next().getEndNode();
      bbox = (double[]) child.getProperty("bbox");
      rectangles.add(new MyRectangle(bbox[0], bbox[1], bbox[2], bbox[3]));
    }
    DrawRectanglesFrame frame = new DrawRectanglesFrame(spaceExtend, drawExtend, rectangles);
    return frame;
  }

}
