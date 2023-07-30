#!/usr/bin/env bash

path=./hdfs/data/data1
name_of_directory=data1

filename_excel=daily_market_price.xlsx
source_dir=./local/data/market

source_path=$source_dir/$filename_excel
target_path=$path/$filename_excel

log=$path/log_file.txt

if [[ -d $path ]]; then
  echo "There is $name_of_directory Directory Exists!"
  cp $source_path $target_path
  if [[ ! -e $log ]]; then
    printf "Log File" >> $log
  fi
  date >> $log
  echo "File Moved Successfully" >> $log
else
  echo "$name_of_directory Directory Not Exists!"
  mkdir -p $path
  exit 1
fi