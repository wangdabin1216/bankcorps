#!/bin/bash

paradt=$1

currdt=`date +"%Y%m%d"`

#echo $currdt
#echo $paradt

date_ft=`echo ${#paradt}`
if [ $date_ft -eq 10 ];then
  yyyy=`expr substr "$1" 1 4`
  mm=`expr substr "$1" 6 2`
  dd=`expr substr "$1" 9 2`
  echo $yyyy$mm$dd
elif [ $date_ft -eq 8 ];then
  echo $paradt
else
  echo `expr ${date} - 1`
fi
