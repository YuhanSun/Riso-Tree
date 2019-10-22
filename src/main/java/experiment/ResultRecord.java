package experiment;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.graphdb.ExecutionPlanDescription;
import commons.Util;
import graph.Naive_Neo4j_Match;
import graph.RisoTreeQueryPN;
import graph.SpatialFirst_List;

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
  public long candidate_count;

  public ExecutionPlanDescription planDescription;

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
    string += String.format("candidate_count: %d\n", candidate_count);

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

  public ResultRecord(long runTime, long pageHit, long getIteratorTime, long iterateTime,
      long resultCount, ExecutionPlanDescription planDescription) {
    this(runTime, pageHit, getIteratorTime, iterateTime, resultCount);
    this.planDescription = planDescription;
  }

  public ResultRecord(Naive_Neo4j_Match naive_Neo4j_Match) {
    this(naive_Neo4j_Match.run_time, naive_Neo4j_Match.page_access,
        naive_Neo4j_Match.get_iterator_time, naive_Neo4j_Match.iterate_time,
        naive_Neo4j_Match.result_count, naive_Neo4j_Match.planDescription);
  }

  /**
   * Spatial-First-List approach.
   *
   * @param runTime
   * @param pageHit
   * @param rangeQueryTime
   * @param getIteratorTime
   * @param iterateTime
   * @param overlapLeafCount
   * @param resultCount
   */
  public ResultRecord(long runTime, long pageHit, long rangeQueryTime, long getIteratorTime,
      long iterateTime, long overlapLeafCount, long candidateCount, long resultCount) {
    this(runTime, pageHit, getIteratorTime, iterateTime, resultCount);
    this.range_query_time = rangeQueryTime;
    this.overlap_leaf_node_count = overlapLeafCount;
    this.candidate_count = candidateCount;
  }

  public ResultRecord(long runTime, long pageHit, long rangeQueryTime, long getIteratorTime,
      long iterateTime, long overlapLeafCount, long candidateCount, long resultCount,
      ExecutionPlanDescription planDescription) {
    this(runTime, pageHit, rangeQueryTime, getIteratorTime, iterateTime, overlapLeafCount,
        candidateCount, resultCount);
    this.planDescription = planDescription;
  }

  public ResultRecord(SpatialFirst_List spatialFirst_List) {
    this(spatialFirst_List.run_time, spatialFirst_List.page_hit_count,
        spatialFirst_List.range_query_time, spatialFirst_List.get_iterator_time,
        spatialFirst_List.iterate_time, spatialFirst_List.overlap_leaf_count,
        spatialFirst_List.candidate_count, spatialFirst_List.result_count,
        spatialFirst_List.planDescription);
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
  public ResultRecord(long runTime, long pageHit, long rangeQueryTime, long getIteratorTime,
      long iterateTime, long setLabelTime, long removeLabelTime, long overlapLeafCount,
      long candidateCount, long resultCount) {
    this(runTime, pageHit, rangeQueryTime, getIteratorTime, iterateTime, overlapLeafCount,
        candidateCount, resultCount);
    this.set_label_time = setLabelTime;
    this.remove_label_time = removeLabelTime;
  }

  public ResultRecord(long runTime, long pageHit, long rangeQueryTime, long getIteratorTime,
      long iterateTime, long setLabelTime, long removeLabelTime, long overlapLeafCount,
      long candidateCount, long resultCount, ExecutionPlanDescription planDescription) {
    this(runTime, pageHit, rangeQueryTime, getIteratorTime, iterateTime, setLabelTime,
        removeLabelTime, overlapLeafCount, candidateCount, resultCount);
    this.planDescription = planDescription;
  }

  public ResultRecord(RisoTreeQueryPN risoTreeQueryPN) {
    this(risoTreeQueryPN.run_time, risoTreeQueryPN.page_hit_count, risoTreeQueryPN.range_query_time,
        risoTreeQueryPN.get_iterator_time, risoTreeQueryPN.iterate_time,
        risoTreeQueryPN.set_label_time, risoTreeQueryPN.remove_label_time,
        risoTreeQueryPN.overlap_leaf_node_count, risoTreeQueryPN.candidate_count,
        risoTreeQueryPN.result_count, risoTreeQueryPN.planDescription);
  }


  /////////////////////////////////////////////////////////////////////////////////////////
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

  public static long getCandidateCountAvg(List<ResultRecord> resultRecords) {
    List<Long> counts = new ArrayList<>(resultRecords.size());
    for (ResultRecord resultRecord : resultRecords) {
      counts.add(resultRecord.candidate_count);
    }
    return Util.Average(counts);
  }
}
