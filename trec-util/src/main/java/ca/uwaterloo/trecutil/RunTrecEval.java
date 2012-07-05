package ca.uwaterloo.trecutil;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunTrecEval {
  public static final Logger LOG = LoggerFactory.getLogger(RunTrecEval.class);
  private static final String TREC_EVAL_EXECUTABLE = "/u2/yaboulnaga/Programs/trec-eval/trec_eval ";
  
  /**
   * @param args
   * @throws InterruptedException
   * @throws IOException
   */
  public static void main(String[] args) throws InterruptedException, IOException {
    Collection<File> resultFiles = FileUtils.listFiles(new File(args[0]),
        new String[] { "txt" },
        true);
    String qrelPath = args[1];
    for (File decrFile : resultFiles) {
      
      if (runTrecEval(decrFile, qrelPath, null, new File(decrFile.getAbsolutePath()+".eval")) != 0) {
        LOG.error("Cannot run trec eval for: "
            + decrFile);
        continue;
      }
      
    }
  }
  
  public static int runTrecEval(File decrFile, String qrelPath, Map<String, Float> metricsOut,
      File outFile) throws IOException, InterruptedException {
    Process decrypt = Runtime
        .getRuntime()
        .exec(TREC_EVAL_EXECUTABLE
            + " " + qrelPath
            + " " + decrFile.getAbsolutePath()); // + logFile);
    
    int chInt;
    
    StringBuilder sb = new StringBuilder();
    StringBuilder line = new StringBuilder();
    while ((chInt = decrypt.getInputStream().read()) != -1) {
      char ch = (char) chInt;
      if (ch == '\n') {
        String key = line.substring(0, line.indexOf(" "));
        String value = line.substring(line.indexOf("all") + 4);
        line.setLength(0);
        Float floatVal;
        try {
          floatVal = Float.parseFloat(value);
          if (metricsOut != null)
            metricsOut.put(key, floatVal);
        } catch (NumberFormatException ignored) {
          // this is the run name or something wrong.. don't want it!
        }
      } else {
        line.append(ch);
      }
      sb.append(ch);
    }
    
    if (outFile != null) {
      FileUtils.writeStringToFile(outFile, sb.toString());
    }
    
    // ////////////////////////////
    
    sb = new StringBuilder();
    while ((chInt = decrypt.getErrorStream().read()) != -1) {
      sb.append((char) chInt);
    }
    
    if (sb.length() != 0)
      LOG.error(sb.toString());
    
    return decrypt.waitFor();
  }
}
