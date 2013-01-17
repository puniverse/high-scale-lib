package Testing.CAT_Tester;

//package org.cliffc.high_scale_lib;
public abstract class Counter {
  public abstract String name();
  public abstract long get();
  public abstract void add( long x );
  public long pre_add ( long x ) { long l = get(); add(x); return l; }
  public long post_add( long x ) { add(x); long l = get(); return l; }
  public long post_inc() { return post_add( 1); }
  public long  pre_dec() { return  pre_add(-1); }
}

