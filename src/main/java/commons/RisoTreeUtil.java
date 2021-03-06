package commons;

import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

public class RisoTreeUtil {

  public static boolean isPNProperty(String key) {
    return key.startsWith(Config.PNPrefix + "_");
  }

  public static String getPNSizeName(String PNName) {
    return StringUtils.replace(PNName, Config.PNPrefix, Config.PNSizePrefix, 1);
  }

  public static String getAttachName(String prefix, String attachment) {
    return prefix + "_" + attachment;
  }

  /**
   * Get the hop number of a PN property name. Assume 0-hop is stored with property name PN_label.
   * 
   * @param PNPropertyName
   * @return
   */
  public static int getHopNumber(String PNPropertyName) {
    return StringUtils.countMatches(PNPropertyName, "_") - 1;
  }

  /**
   * Form the label paths set for the ignore case. Because longer path can be ignored. So check the
   * shorter path that has value []. Path can either start with PN or PNSize.
   * 
   * @param path
   * @return the shorter paths with the same label prefix.
   */
  public static Set<String> formIgnoreSearchSet(String path) {
    Set<String> searchPaths = new HashSet<>();
    String[] strings = StringUtils.split(path, '_');
    String curString = strings[0];
    for (int i = 1; i < strings.length - 1; i++) {
      curString += "_" + strings[i];
      searchPaths.add(curString);
    }
    return searchPaths;
  }
}
