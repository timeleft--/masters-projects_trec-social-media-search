package ca.uwaterloo.twitter.queryexpand;

import java.util.Enumeration;

import weka.core.DistanceFunction;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.neighboursearch.PerformanceStats;

public class CosineDistance implements DistanceFunction {
  
  Instances insts;
  
  public CosineDistance() {
  }
  
  @Override
  public Enumeration listOptions() {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void setOptions(String[] options) throws Exception {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public String[] getOptions() {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void setInstances(Instances insts) {
    this.insts = insts;
  }
  
  @Override
  public Instances getInstances() {
    return insts;
  }
  
  @Override
  public void setAttributeIndices(String value) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public String getAttributeIndices() {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void setInvertSelection(boolean value) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public boolean getInvertSelection() {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public double distance(Instance first, Instance second) {
    
    int n = first.numAttributes();
    if (second.numAttributes() != n) {
      throw new IllegalArgumentException();
    }
    double sumVW = 0;
    double sumV2 = 0;
    double sumW2 = 0;
    
    for (int i = 0; i < n; ++i) {
      double v = first.value(i);
      double w = second.value(i);
      sumVW += v * w;
      sumV2 += v * v;
      sumW2 += w * w;
    }
    
    double result = sumVW / Math.sqrt(sumV2 * sumW2);
    return 1 - result;
  }
  
  @Override
  public double distance(Instance first, Instance second, PerformanceStats stats) throws Exception {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public double distance(Instance first, Instance second, double cutOffValue) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public double distance(Instance first, Instance second, double cutOffValue, PerformanceStats stats) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void postProcessDistances(double[] distances) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void update(Instance ins) {
    throw new UnsupportedOperationException();
  }
  
}
