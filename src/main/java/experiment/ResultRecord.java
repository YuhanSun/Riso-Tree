package experiment;

import java.util.ArrayList;
import java.util.List;
import commons.Util;

public class ResultRecord {
  public long runTime;
  public long pageHit;

  public long range_query_time;
  public long get_iterator_time;
  public long iterate_time;
  public long set_label_time;
  public long remove_label_time;

  public long result_count;
  public long overlap_leaf_node_count;
  public long located_in_count;

  public ResultRecord(long runTime, long pageHit) {
    this.runTime = runTime;
    this.pageHit = pageHit;
  }

  /**
   * Naive approach
   *
   * @param runTime
   * @param pageHit
   * @param getIteratorTime
   * @param iterateTime
   * @param resultCount
   */
  public ResultRecord(long runTime, long pageHit, long getIteratorTime, long iterateTime,
      long resultCount) {
    this(runTime, pageHit);
    this.get_iterator_time = getIteratorTime;
    this.iterate_time = iterateTime;
    this.result_count = resultCount;
  }

  public ResultRecord(long runTime, long pageHit, long getIteratorTime, long iterateTime,
      long setLabelTime, long removeLabelTime, long resultCount) {
    this(runTime, pageHit, getIteratorTime, iterateTime, resultCount);
    this.set_label_time = setLabelTime;
    this.remove_label_time = removeLabelTime;
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
