#! /bin/bash
clear
counter=1
while [ $counter -lt 8 ]; do
    
    	
	num=$((counter*10))
	echo "$num"
	shuf -zen $num ~/Lab4/inp/* | xargs -0 cp -t ~/Lab4/dest
	hdfs dfs -rm -r ~/input/act4inp
	hdfs dfs -put ~/Lab4/dest/ ~/input/act4inp
	v="$counter"
	hdfs dfs -rm -r ~/Lab4/act4output$v
	rm -rf ~/Lab4/act4output$v
#	declare RESULT=$(time hadoop jar activity4.jar activity4 ~/input/act4inp ~/act4output$v)
	(time hadoop jar wc.jar Stripes_n3 ~/input/act4inp ~/Lab4/act4output$v) 2>&1 | tail -3 >> logfilestore2
	#(time hadoop jar latin.jar latin_occur ~/data ~/output201 lemmas.csv) 2>&1 | tail -3 >>logfile
	#tail -n 3 temp.txt > outputfin.txt
	hdfs dfs -get ~/Lab4/act4output$v
	rm -rf ~/Lab4/dest/*
	let counter=counter+1
done
