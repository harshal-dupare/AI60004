#!/bin/bash

for n in 1 2 3 5 6
do
	for k in 10 100
	do
		for t in 1 8 20 30 40 80
		do 
			echo "for $t $n $k"

			# for e in 1 2 3 4 5
			# do
			# 	python3 final.py 20_newsgroups $t $n $k
			# done

			# python3 assignment-1-18MA20015-ddl.py 20_newsgroups $t $n $k > "ddl/t$t-n$n-k$k.txt"
			
			diff "fin1/t$t-n$n-k$k.txt" "ddl/t$t-n$n-k$k.txt"
			echo "---------------------------------------------------------------------------------------------------------------------------"
			echo "---------------------------------------------------------------------------------------------------------------------------"
		done
	done
done
