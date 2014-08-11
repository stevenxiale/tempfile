sqoop export --connect jdbc:mysql://10.13.20.44/sqoop -m 1 --table widgets --export-dir /user/hadoop/widgets/ --input-fields-terminated-by ',' --username hadoop --password hadoop;
========
./sqoop export --connect jdbc:mysql://120.197.94.140/newstat -m 1 --table stat_pic_daily_hive --export-dir /data/stat/hive/warehouse/stat_pic_daily_hive/ --input-fields-terminated-by '\0001' --username  --password ;
