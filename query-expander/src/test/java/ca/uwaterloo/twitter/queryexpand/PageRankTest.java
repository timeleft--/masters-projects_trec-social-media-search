package ca.uwaterloo.twitter.queryexpand;

import org.apache.mahout.math.map.OpenIntFloatHashMap;
import org.apache.mahout.math.map.OpenObjectFloatHashMap;
import org.junit.Test;

import edu.umd.cloud9.io.pair.PairOfInts;
import static org.junit.Assert.*;
import static ca.uwaterloo.twitter.queryexpand.FISQueryExpander.pageRank;

public class PageRankTest {
  
  @Test
  public void testFromBook() {
    //see fig 15.1 page 510
    float damping = 0.75f;
    OpenObjectFloatHashMap<PairOfInts> edges = new OpenObjectFloatHashMap<PairOfInts>();
    int numPages = 6;
    int[] out = {3, 1, 2, 1, 1, 0};
    int maxIters = 20;
    float minDelta = 1E-3f;
    edges.put(new PairOfInts(0,3), 1);
    edges.put(new PairOfInts(0,1), 1);
    edges.put(new PairOfInts(0,2), 1);
    edges.put(new PairOfInts(1,0), 1);
    edges.put(new PairOfInts(2,0), 1);
    edges.put(new PairOfInts(2,5), 1);
    edges.put(new PairOfInts(3,0), 1);
    edges.put(new PairOfInts(4,3), 1);
    
    OpenIntFloatHashMap rank = pageRank(edges, out, numPages, 8, damping, maxIters, minDelta);
    
    assertEquals(2.15, rank.get(0), minDelta);
    assertEquals(0.87, rank.get(1), minDelta);
    assertEquals(0.87, rank.get(2), minDelta);
    assertEquals(1.119, rank.get(3), minDelta);
    assertEquals(0.332, rank.get(4), minDelta);
    assertEquals(0.658, rank.get(5), minDelta);
    
  }
}
