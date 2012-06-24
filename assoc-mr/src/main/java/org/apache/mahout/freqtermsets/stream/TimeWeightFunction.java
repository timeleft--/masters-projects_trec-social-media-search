package org.apache.mahout.freqtermsets.stream;

import org.apache.mahout.common.Parameters;
import org.apache.mahout.freqtermsets.PFPGrowth;

public abstract class TimeWeightFunction {
  public static class LinearDecay extends TimeWeightFunction {
    private final float coeff;
    private final long windowLength;
    
    LinearDecay(float coeff, long windowLength) {
      this.coeff = coeff;
      this.windowLength = windowLength;
    }
    
    @Override
    public float apply(float orig, long origTimestamp, long currTimestamp) {
      long numWindows = (currTimestamp - origTimestamp) / windowLength;
      double result = orig * Math.pow(coeff, numWindows);
      return (float) result;
    }
    
  }
  
  public abstract float apply(float orig, long origTimestam, long currTimestamp);
  
  public static TimeWeightFunction getDefault(Parameters params) {
    long intervalStart = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_START));
    // Long.toString(PFPGrowth.TREC2011_MIN_TIMESTAMP))); //GMT23JAN2011)));
    long intervalEnd = Long.parseLong(params.get(PFPGrowth.PARAM_INTERVAL_END));
    // Long.toString(Long.MAX_VALUE)));
    long windowSize = Long.parseLong(params.get(PFPGrowth.PARAM_WINDOW_SIZE,
        Long.toString(intervalEnd - intervalStart)));
    TimeWeightFunction result = new LinearDecay(PFPGrowth.FPSTREAM_LINEAR_DECAY_COEFF,
        windowSize);
    return result;
  }
}
