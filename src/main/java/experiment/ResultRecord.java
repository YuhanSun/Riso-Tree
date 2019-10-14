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

  @Override
  public String toString() {
    String string = "";
    string += String.format("runTime: %d\n", runTime);
    string += String.format("pageHit: %d\n", pageHit);
    string += String.format("range_query_time: %d\n", range_query_time);
    string += String.format("get_iterator_time: %d\n", get_iterator_time);
    string += String.format("iterate_time: %d\n", iterate_time);
    string += String.format("set_label_time: %d\n", set_label_time);
    string += String.format("remove_label_time: %d\n", remove_label_time);
    string += String.format("result_count: %d\n", result_count);
    string += String.format("overlap_leaf_node_count: %d\n", overlap_leaf_node_count);
    string += String.format("located_in_count: %d\n", located_in_count);

    return string;
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


  /**
   * RisoTree approach.
   *
   * @param runTime
   * @param pageHit
   * @param getIteratorTime
   * @param iterateTime
   * @param setLabelTime
   * @param removeLabelTime
   * @param overlapLeafCount
   * @param resultCount
   */
  public ResultRecord(long runTime, long pageHit, long getIteratorTime, long iterateTime,
      long setLabelTime, long removeLabelTime, long overlapLeafCount, long resultCount) {
    this(runTime, pageHit, getIteratorTime, iterateTime, resultCount);
    this.overlap_leaf_node_count = overlapLeafCount;
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

  public static long getRangeQueryTimeAvg(List<ResultRecord> resultRecords) {
    List<Long> rangeQueryTimes = new ArrayList<>(resultRecords.size());
    for (ResultRecord resultRecord : resultRecords) {
      rangeQueryTimes.add(resultRecord.range_query_time);
    }
    return Util.Average(rangeQueryTimes);
  }

  public static long getGetIteratorTimeAvg(List<ResultRecord> resultRecords) {
    List<Long> times = new ArrayList<>(resultRecords.size());
    for (ResultRecord resultRecord : resultRecords) {
      times.add(resultRecord.get_iterator_time);
    }
    return Util.Average(times);
  }

  public static long getIterateTimeAvg(List<ResultRecord> resultRecords) {
    List<Long> times = new ArrayList<>(resultRecords.size());
    for (ResultRecord resultRecord : resultRecords) {
      times.add(resultRecord.iterate_time);
    }
    return Util.Average(times);
  }

  public static long getSetLabelTimeAvg(List<ResultRecord> resultRecords) {
    List<Long> times = new ArrayList<>(resultRecords.size());
    for (ResultRecord resultRecord : resultRecords) {
      times.add(resultRecord.set_label_time);
    }
    return Util.Average(times);
  }

  public static long getRemoveLabelTimeAvg(List<ResultRecord> resultRecords) {
    List<Long> times = new ArrayList<>(resultRecords.size());
    for (ResultRecord resultRecord : resultRecords) {
      times.add(resultRecord.remove_label_time);
    }
    return Util.Average(times);
  }

  public static long getResultCountAvg(List<ResultRecord> resultRecords) {
    List<Long> counts = new ArrayList<>(resultRecords.size());
    for (ResultRecord resultRecord : resultRecords) {
      counts.add(resultRecord.result_count);
    }
    return Util.Average(counts);
  }

  public static long getOverLapLeafCountAvg(List<ResultRecord> resultRecords) {
    List<Long> counts = new ArrayList<>(resultRecords.size());
    for (ResultRecord resultRecord : resultRecords) {
      counts.add(resultRecord.overlap_leaf_node_count);
    }
    return Util.Average(counts);
  }

  public static long getLocatedInCountAvg(List<ResultRecord> resultRecords) {
    List<Long> counts = new ArrayList<>(resultRecords.size());
    for (ResultRecord resultRecord : resultRecords) {
      counts.add(resultRecord.located_in_count);
    }
    return Util.Average(counts);
  }
}
