package Testing.CAT_Tester;

public final class RaceyCounter extends Counter {
  private long _cnt;
  public long get(){ return _cnt; }
  public void add( long x ) { _cnt += x; }
  public String name() { return "Racey"; }
}
