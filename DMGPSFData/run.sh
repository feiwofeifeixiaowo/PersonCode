#!/bin/bash
#declare -i num=360
for k in $( seq 245 355)
do
        currday=`date +%Y-%m-%d -d "$a -$k days"`
            echo "now date is ："$currday
                spark-submit DMGPSFData_8.jar $currday 
            done
