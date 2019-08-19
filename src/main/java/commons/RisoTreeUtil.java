package commons;

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
}
