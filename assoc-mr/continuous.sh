#!/bin/bash
for i in {1..17}
do
j=$(expr $i - 1)	
echo $j
st=$(echo "$j * 86400000" | bc)
st=$(expr $st + 1295740800000)
et=$(expr $st + 86400000)
	echo "day $i , starts $st and ends $et"
	/u2/yaboulnaga/Programs/hadoop-1.0.3/bin/hadoop jar target/assoc-mr-0.0.2-SNAPSHOT-job.jar org.apache.mahout.freqtermsets.FPGrowthDriver --input file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/csv_hour-5min/ --output file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/assoc-mr_chuncks-5min_day$i/ -mi -j 4 -mf 3 --minSupport 2 -pct 99 -g 256 -ws 300000  -st $st -et $et
        /u2/yaboulnaga/Programs/hadoop-1.0.3/bin/hadoop jar target/assoc-mr-0.0.2-SNAPSHOT-job.jar org.apache.mahout.freqtermsets.FPGrowthDriver --input file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/csv_hour-5min/ --output file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/assoc-mr_chuncks-15min_day$i/ -mi -j 4 -mf 3 --minSupport 2 -pct 99 -g 256 -ws 900000  -st $st -et $et
        /u2/yaboulnaga/Programs/hadoop-1.0.3/bin/hadoop jar target/assoc-mr-0.0.2-SNAPSHOT-job.jar org.apache.mahout.freqtermsets.FPGrowthDriver --input file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/csv_hour-5min/ --output file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/assoc-mr_chuncks-30min_day$i/ -mi -j 4 -mf 3 --minSupport 2 -pct 99 -g 256 -ws 1800000  -st $st -et $et
        /u2/yaboulnaga/Programs/hadoop-1.0.3/bin/hadoop jar target/assoc-mr-0.0.2-SNAPSHOT-job.jar org.apache.mahout.freqtermsets.FPGrowthDriver --input file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/csv_hour-5min/ --output file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/assoc-mr_chuncks-1hr_day$i/ -mi -j 4 -mf 3 --minSupport 2 -pct 99 -g 256 -ws 3600000  -st $st -et $et
        /u2/yaboulnaga/Programs/hadoop-1.0.3/bin/hadoop jar target/assoc-mr-0.0.2-SNAPSHOT-job.jar org.apache.mahout.freqtermsets.FPGrowthDriver --input file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/csv_hour-5min/ --output file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/assoc-mr_chuncks-2hr_day$i/ -mi -j 4 -mf 3 --minSupport 2 -pct 99 -g 256 -ws 7200000  -st $st -et $et
        /u2/yaboulnaga/Programs/hadoop-1.0.3/bin/hadoop jar target/assoc-mr-0.0.2-SNAPSHOT-job.jar org.apache.mahout.freqtermsets.FPGrowthDriver --input file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/csv_hour-5min/ --output file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/assoc-mr_chuncks-4hr_day$i/ -mi -j 4 -mf 3 --minSupport 2 -pct 99 -g 256 -ws 14400000  -st $st -et $et
        /u2/yaboulnaga/Programs/hadoop-1.0.3/bin/hadoop jar target/assoc-mr-0.0.2-SNAPSHOT-job.jar org.apache.mahout.freqtermsets.FPGrowthDriver --input file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/csv_hour-5min/ --output file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/assoc-mr_chuncks-8hr_day$i/ -mi -j 4 -mf 3 --minSupport 2 -pct 99 -g 256 -ws 28800000  -st $st -et $et
        /u2/yaboulnaga/Programs/hadoop-1.0.3/bin/hadoop jar target/assoc-mr-0.0.2-SNAPSHOT-job.jar org.apache.mahout.freqtermsets.FPGrowthDriver --input file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/csv_hour-5min/ --output file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/assoc-mr_chuncks-16hr_day$i/ -mi -j 4 -mf 3 --minSupport 2 -pct 99 -g 256 -ws 57600000  -st $st -et $et
        /u2/yaboulnaga/Programs/hadoop-1.0.3/bin/hadoop jar target/assoc-mr-0.0.2-SNAPSHOT-job.jar org.apache.mahout.freqtermsets.FPGrowthDriver --input file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/csv_hour-5min/ --output file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/assoc-mr_chuncks-24hr_day$i/ -mi -j 4 -mf 3 --minSupport 2 -pct 99 -g 256 -ws 115200000  -st $st -et $et
done
