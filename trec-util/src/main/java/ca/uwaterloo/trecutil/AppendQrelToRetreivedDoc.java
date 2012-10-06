package ca.uwaterloo.trecutil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.LinkedHashMap;

import org.apache.commons.io.FileUtils;

public class AppendQrelToRetreivedDoc {
  public static void main(String[] args) throws IOException {
    QRelUtil qRelUtil = new QRelUtil(new File(args[0]));
    File docsFilesDir = new File(args[1]);
    File outDir = new File(args[2]);
    for (File docsFile : docsFilesDir.listFiles()) {
      LinkedHashMap<String, Float> qrels = qRelUtil.qRel.get(Integer.parseInt(docsFile.getName()
          .substring(2)) + "");
      BufferedReader rd = new BufferedReader(new FileReader(docsFile));
      Writer wr = Channels.newWriter(
          FileUtils.openOutputStream(new File(outDir, docsFile.getName()))
              .getChannel(),
          "UTF-8");
      try {
        String line;
        while ((line = rd.readLine()) != null) {
          // query_id, iter, docno, rank, sim, run_id
          String[] fields = line.split("\\s");
          Float jud = null;
          if (qrels != null) {
            jud = qrels.get(fields[1]);
          }
          wr.append(line).append("\t").append((jud == null ? "U" : jud + "")).append("\n");
        }
      } finally {
        wr.flush();
        wr.close();
      }
    }
  }
}
