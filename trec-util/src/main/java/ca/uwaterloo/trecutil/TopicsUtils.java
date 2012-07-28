package ca.uwaterloo.trecutil;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class TopicsUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TopicsUtils.class);
  
  public List<String> queries;
  public List<String> topicIds;
  public List<String> queryTimes;
  public List<String> maxTweetIds;
  
  public TopicsUtils(String topicsXmlPath, boolean sortTopicsChronolically) throws JDOMException, IOException {
    SAXBuilder docBuild = new SAXBuilder();
    org.jdom2.Document topicsXML = docBuild.build(new File(topicsXmlPath));
    
    List<Element> topicElts = Lists.newArrayListWithCapacity(50);
    for (Element elt : topicsXML.getRootElement().getChildren()) {
      topicElts.add(elt.clone().detach());
    }
    
    if (sortTopicsChronolically) {
      Collections.sort(topicElts, new Comparator<Element>() {
        SimpleDateFormat dFmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy");
        
        @Override
        public int compare(Element o1, Element o2) {
          try {
            long t1 = dFmt.parse(o1.getChildText("querytime").trim()).getTime();
            long t2 = dFmt.parse(o2.getChildText("querytime").trim()).getTime();
            return Double.compare(t1, t2);
          } catch (ParseException e) {
            LOG.error(e.getMessage(), e);
            return 0;
          }
        }
      });
    }
    
    queries = Lists.newArrayListWithCapacity(topicElts.size());
    topicIds = Lists.newArrayListWithCapacity(topicElts.size());
    queryTimes = Lists.newArrayListWithCapacity(topicElts.size());
    maxTweetIds = Lists.newArrayListWithCapacity(topicElts.size());
    for (Element topic : topicElts) {
      Element queryElt = topic.getChild("title");
      if (queryElt == null) {
        // 2012 format
        queryElt = topic.getChild("query");
      }
      queries.add(queryElt.getText());
      String topicId = topic.getChildText("num");
      topicId = topicId.substring(topicId.indexOf(':') + 1).trim();
      topicIds.add(topicId);
      queryTimes.add(topic.getChildText("querytime"));
      maxTweetIds.add(topic.getChildText("querytweettime"));
    }
    
   
  }
}
