/u2/yaboulnaga/Programs/hadoop-1.0.3/bin/hadoop jar target/assoc-mr-0.0.2-SNAPSHOT-job.jar org.apache.mahout.freqtermsets.FPGrowthDriver --input file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/csv_hour-5min/ --output file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/assoc-mr_hourly-chunks/ -mi -j 3 -mf 2 -pct 95 -ws 3600000 -st 1295740800000 --minSupport 2

#--countIn  file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/assoc-mr_0605-1000/ -g 10000 -mi --gfisIn file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/assoc-mr_0607-2100/

#file:///u2/yaboulnaga/Shared/code/trec2012/assoc-mr/

#--input hdfs://localhost:9000/trec2012/csv_hour-5min/
 
