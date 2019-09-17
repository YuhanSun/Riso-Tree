package experiment;

import java.util.ArrayList;
import java.util.List;
import commons.Util;

public class ResultRecord {
  public long runTime;
  public long pageHit;

  public ResultRecord(long runTime, long pageHit) {
    this.runTime = runTime;
    this.pageHit = pageHit;
  }

  public static long getRunTimeAvg(List<ResultRecord> resultRecords) {
    List<Long> runTimes = new ArrayList<>(resultRecords.size());
    for (ResultRecord resultRecord : resultRecords) {
      runTimes.add(resultRecord.runTime);
    }
    return Util.Average(runTimes);
  }

  public static long getPageHitAvg(List<ResultRecord> resultRecords) {
    List<Long> pageHits = new ArrayList<>(resultRecords.size());
    for (ResultRecord resultRecord : resultRecords) {
      pageHits.add(resultRecord.pageHit);
    }
    return Util.Average(pageHits);
  }
}
