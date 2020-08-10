package ca.mcit.bigdata.hive

import java.sql.{Connection, DriverManager}

object HiveClient extends App {
  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)
  val connection: Connection = DriverManager.getConnection("jdbc:hive2://172.16.129.58:10000/kishore","cloudera","cloudera")
  val stmt = connection.createStatement()
  stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict")

  stmt.execute("set hive.exec.dynamic.partition=true")

  stmt.execute("drop table IF EXISTS kishore.enriched_trip")

  stmt.execute("create table kishore.enriched_trip ( " +
    "route_id Int,         " +
    "service_id String,   " +
    "trip_id String,      " +
    "trip_headsign String," +
    "direction_id Int,    " +
    "shape_id Int,        " +
    "note_fr String,      " +
    "note_en String,      " +
    "date string,          " +
    "exception_type string," +
    "start_time String,    " +
    "end_time String,      " +
    "headway_sec String)" +

    "PARTITIONED BY (wheelchair_accessible Int)" +
    "TBLPROPERTIES('parquet.compression'='GZIP')")

  stmt.execute("Insert overwrite table kishore.enriched_trip PARTITION(wheelchair_accessible) " +
    "select A.route_id ,A.service_id , A.trip_id,A.trip_headsign,A.direction_id ,A.shape_id , A.note_fr ,A.note_en , B.date ,B.exception_type ,C.start_time , C.end_time ,C.headway_secs,A.wheelchair_accessible " +
    "from kishore.ext_trips A " + "left join kishore.ext_calendar_dates B on A.service_id = B.service_id " +
    "left join kishore.ext_frequencies C on A.trip_id = C.trip_id")

  stmt.close()
  connection.close()
}
