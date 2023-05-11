#!/bin/bash

words=("efforts" "big" "data" "analysis")
k_list=(10)

for str in ${words[@]}; do
for k in ${k_list[@]}; do
    echo OUTPUT FOR : QUERY_WORD = $str, K = $k
    spark-submit assignment-3-18MA20015.py input.txt $str $k stopword.txt 
done
done