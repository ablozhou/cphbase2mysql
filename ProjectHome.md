A tool to copy from hbase to mysql table

如果要将hbase数据导出到mysql，工具方面，可以用两种方法。第一种，将hbase导出成hdfs平面文件，然后用sqoop导出到mysql。第二种，将hbase数据导出到hive，再用sqoop导出到mysql。

如果想提高效率和进行更多控制，可以直接编程的方式，从hbase中读出数据，导出到mysql。