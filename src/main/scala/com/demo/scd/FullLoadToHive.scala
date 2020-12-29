package com.demo.scd

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{SaveMode, SparkSession}

object FullLoadToHive {
  def main(args: Array[String]): Unit = {
    val sparkSession=SparkSession.builder().enableHiveSupport().master("local").appName("scd1").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession.sql("create database if not exists ordersdb")

    val rootConfig = ConfigFactory.load("application.conf").getConfig("conf")
    val mysqlConfig = rootConfig.getConfig("mysql_conf")

    var jdbcParams = Map("url" ->  getMysqlJdbcUrl(mysqlConfig),
       "dbtable" -> "scd1.orders",
      "user" -> mysqlConfig.getString("username"),
      "password" -> mysqlConfig.getString("password")
    )
    var jdbcParamswrite = Map("url" ->  getMysqlJdbcUrl(mysqlConfig),
      "dbtable" -> "hudi_dms.ordertab",
      "user" -> mysqlConfig.getString("username"),
      "password" -> mysqlConfig.getString("password")
    )

    println("\nReading data from MySQL DB using SparkSession.read.format(),")
    val txnDF = sparkSession
      .read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .options(jdbcParams)                                                  // options can pass map
      .load()
    txnDF.show()

    txnDF.write.mode(SaveMode.Overwrite).format("hive").saveAsTable("ordersdb.orders")
    var hivedf=sparkSession.table("ordersdb.orders")
hivedf.show()

    sparkSession.stop()
  }

  // Creating Redshift JDBC URL
  def getMysqlJdbcUrl(mysqlConfig: Config): String = {
    val host = mysqlConfig.getString("hostname")
    val port = mysqlConfig.getString("port")
    val database = mysqlConfig.getString("database")
    s"jdbc:mysql://$host:$port/$database?autoReconnect=true&useSSL=false"
  }



}
