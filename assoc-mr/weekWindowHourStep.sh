/u2/yaboulnaga/Programs/hadoop-1.0.3/bin/hadoop jar target/assoc-mr-0.0.2-SNAPSHOT-job.jar org.apache.mahout.freqtermsets.FPGrowthDriver --input file:///u2/yaboulnaga/data/twitter-trec2011/rawtweets_csv_hour-5min/ --output file:///u2/yaboulnaga/data/twitter-trec2011/assoc-mr_week-window_hour-step_no-incr-supp_yes-incr-k/ -mf 77 --minSupport 33 -pct 100 -g 256 -ws 604800000 -ss 3600000

