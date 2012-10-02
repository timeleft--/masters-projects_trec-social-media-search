#!/bin/bash
for i in {1..17}
do
j=$(expr $i - 1)	
echo $j
st=$(echo "$j * 86400000" | bc)
st=$(expr $st + 1295740800000)
et=$(expr $st + 86400000)
	echo "day $i , starts $st and ends $et"
       /u2/yaboulnaga/Programs/hadoop-1.0.3/bin/hadoop jar target/assoc-mr-0.0.2-SNAPSHOT-job.jar org.apache.mahout.freqtermsets.FPGrowthDriver --input file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/rawtweets_csv_hour-5min/ --output file:///u2/yaboulnaga/Shared/datasets/twitter-trec2011/assoc-mr_chuncks-24hr_day$i/ -j 4 -mf 3 --minSupport 2 -pct 99 -g 256 -ws 86400000 -st $st -et $et
done
