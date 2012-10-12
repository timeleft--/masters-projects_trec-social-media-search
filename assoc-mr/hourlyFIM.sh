#!/bin/bash
#hrs=$(echo "24 * 17" | bc)
for i in {1..408}
do
j=$(expr $i + 1)	
echo $j
et=$(echo "$j * 3600000" | bc)
et=$(expr $et + 1295740800000)
	echo "hour $i , ends $et"
        /u2/yaboulnaga/Programs/hadoop-1.0.3/bin/hadoop jar target/assoc-mr-0.0.2-SNAPSHOT-job.jar org.apache.mahout.freqtermsets.FPGrowthDriver --input file:///u2/yaboulnaga/data/twitter-trec2011/rawtweets_csv_hour-5min/ --output file:///u2/yaboulnaga/data/twitter-trec2011/assoc-mr_hr-$i/ -mf 3 --minSupport 2 -pct 99 -g 256 -ws 3600000  -et $et
done
