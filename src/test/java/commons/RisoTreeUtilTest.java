package commons;

import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class RisoTreeUtilTest {

  @Test
  public void getPNSizeTest() {
    assertTrue(RisoTreeUtil.getPNSizeName("PN_test").equals("PNSize_test"));
  }

}
