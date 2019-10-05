package commons;

import static org.junit.Assert.assertTrue;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class RisoTreeUtilTest {

  @Test
  public void getPNSizeTest() {
    assertTrue(RisoTreeUtil.getPNSizeName("PN_test").equals("PNSize_test"));
  }

  @Test
  public void formIgnoreSearchSetTest() {
    Set<String> shorterPaths = RisoTreeUtil.formIgnoreSearchSet("PN_1_2");
    assertTrue(shorterPaths.equals(new HashSet<>(Arrays.asList("PN_1"))));

    assertTrue(RisoTreeUtil.formIgnoreSearchSet("PNSize_1_2_3")
        .equals(new HashSet<>(Arrays.asList("PNSize_1_2", "PNSize_1"))));

    assertTrue(RisoTreeUtil.formIgnoreSearchSet("PNSize_1").equals(new HashSet<>()));
  }
}
