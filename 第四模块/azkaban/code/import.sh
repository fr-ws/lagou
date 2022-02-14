#!/bin/sh
today=`date '+%Y%m%d'`
#today=`date -dyesterday '+%Y%m%d'`
hive -e "use homework;load data inpath '/user_click/$today/clicklog.dat' into table user_clicks partition(dt='$today');"