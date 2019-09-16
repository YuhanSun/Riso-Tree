package experiment;

public class ResultRecord {
  public long runTime;
  public long pageHit;

  public ResultRecord(long runTime, long pageHit) {
    this.runTime = runTime;
    this.pageHit = pageHit;
  }
}
