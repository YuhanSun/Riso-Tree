package commons;

public class RisoTreeUtil {

  public static boolean isPNProperty(String key) {
    return key.startsWith(Config.PNPrefix);
  }

}
