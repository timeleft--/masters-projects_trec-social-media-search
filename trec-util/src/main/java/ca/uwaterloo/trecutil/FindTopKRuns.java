package ca.uwaterloo.trecutil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.util.Pair;

import com.google.common.collect.Lists;

public class FindTopKRuns {
  private static final Float ZERO = new Float(0);
  
  /**
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    Collection<File> resultFiles = FileUtils.listFiles(new File(args[0]),
        new String[] { "txt" },
        true);
    QRelUtil qRelUtil = new QRelUtil(new File(args[1]));
    // TreeMap<Double, String> topKRes = new TreeMap<Double, String>();
    PriorityQueue<Pair<String, Double>> topKRes = new PriorityQueue<Pair<String, Double>>(1000,
        new Comparator<Pair<String, Double>>() {
          
          public int compare(Pair<String, Double> o1, Pair<String, Double> o2) {
            return -Double.compare(o1.getValue(), o2.getValue());
          }
          
        });
    Writer wr = Channels.newWriter(FileUtils.openOutputStream(new File(args[2])).getChannel(),
        "UTF-8");
    try {
      
      int numRes = Integer.parseInt(args[3]);
      
      for (File resF : resultFiles) {
        wr.append("===============================================").append('\n')
            .append(resF.getAbsolutePath()).append('\n');
            
        Map<String, List<Pair<Float, Float>>> resMap = new TreeMap<String, List<Pair<Float, Float>>>();
        BufferedReader rd = new BufferedReader(new FileReader(resF));
        String line;
        String currQid = null;
        float currNonRelevant = Float.MIN_VALUE;
        List<Pair<Float, Float>> relevantiNonRelevantTilli = null;
        HashMap<String, Float> currRel = null;
        int currRank = -1;
        while ((line = rd.readLine()) != null) {
          // query_id, iter, docno, rank, sim, run_id
          String[] fields = line.split("\\s");
          if (!fields[0].equals(currQid)) {
            currRel = qRelUtil.qRel.get(fields[0]);
            if (currRel == null) {
              continue;
            }
            currQid = fields[0];
            currRank = 0;
            currNonRelevant = 0;
            // retrievedNonRelevant = new float[Math.min(numRes,currRel.size())];
            relevantiNonRelevantTilli = Lists.newLinkedList();
            resMap.put(currQid, relevantiNonRelevantTilli);
          }
          // relevant of result
          boolean resi = false;
          if (currRank < numRes) {
            Float reli = currRel.get(fields[2]);
            if (ZERO.equals(reli)) {
              ++currNonRelevant;
            } else if (reli != null) {
              resi = true;
            } else {
              wr.append("Unjudged at " + currRank + ": " + fields[2]);
            }
            relevantiNonRelevantTilli
                .add(new Pair<Float, Float>((resi ? 1f : 0f), currNonRelevant));
          }
          ++currRank;
        }
        
        SummaryStatistics rankEffStats = new SummaryStatistics();
        SummaryStatistics avgPrecStats = new SummaryStatistics();
        wr.append("qid").append("\t").append("rankEff").append("\t").append("MAP").append("\t").append("P@30").append('\n');
        for (String qid : resMap.keySet()) {
          float sizeOfNonRel = -1;
          if (qRelUtil.sizeOfNonRelevant.containsKey(qid)) {
            sizeOfNonRel = qRelUtil.sizeOfNonRelevant.get(qid).floatValue();
          } else {
            continue; // the wretched topic 50
          }
          float rankEff = 0;
          float avgPrec = 0;
          float pAtK = 0;
          float pAt30 = -1;
          relevantiNonRelevantTilli = resMap.get(qid);
          int i = 1;
          for (Pair<Float, Float> atI : relevantiNonRelevantTilli) {
            rankEff += atI.getKey() * (1 - (atI.getValue() / sizeOfNonRel));
            pAtK += atI.getKey();
            avgPrec += atI.getKey() * pAtK;
            if (i == 30) {
              pAt30 = pAtK;
            }
            ++i;
          }
          float sizeOfRel = (qRelUtil.qRel.get(qid).size() - sizeOfNonRel);
          rankEff = rankEff / sizeOfRel; // size of judged rel
          avgPrec = avgPrec / sizeOfRel;
          wr.append(qid).append("\t").append(rankEff + "").append("\t").append(avgPrec + "")
              .append("\t").append(pAt30 + "").append('\n');
          rankEffStats.addValue(rankEff);
          avgPrecStats.addValue(avgPrec);
        }
        wr.append("rankEff stats").append("\n").append(rankEffStats.toString()).append('\n');
        wr.append("avg prec stats").append("\n").append(avgPrecStats.toString()).append('\n');
        // topKRes.put(stats.getMean(), resF.getAbsolutePath());
        topKRes.add(new Pair<String, Double>(resF.getAbsolutePath(), avgPrecStats.getMean()));
        // rankEffStats.getMean()));
      }
      wr.append("========================= TOP K ============================\n");
      int k = 1;
      while (!topKRes.isEmpty()) {
        if (k > 10) {
          break;
        }
        Pair<String, Double> res = topKRes.poll();
        wr.append(k + ": " + res.getKey() + " - mean: " + res.getValue() + "\n");
        ++k;
      }
      wr.flush();
    } finally {
      wr.flush();
      wr.close();
    }
  }
  
}
