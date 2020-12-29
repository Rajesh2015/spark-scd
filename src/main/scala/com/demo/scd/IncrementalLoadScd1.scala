package com.demo.scd

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

object IncrementalLoadScd1 {

  def main(args: Array[String]): Unit = {
    val sparkSession=SparkSession.builder().enableHiveSupport().master("local").appName("scd1").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
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
    val sourcedf = sparkSession
      .read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .options(jdbcParams)
      .load()


    sourcedf.show()

    var targetdf=sparkSession.table("ordersdb.orders")
      .toDF("order_id_tgt","order_qty_tgt","customer_name_tgt")
    targetdf.show()
//Finding unchanged data
    val unchangedDatadf=sourcedf.join(targetdf,sourcedf("order_id")===targetdf("order_id_tgt"),"inner")
      .filter(sourcedf("order_qty")===targetdf("order_qty_tgt")
        and  sourcedf("customer_name")===targetdf("customer_name_tgt"))
      .drop("order_id_tgt","order_qty_tgt","customer_name_tgt")
    unchangedDatadf.show()

    //Finding changed data

    val changedDatadf=sourcedf.join(targetdf,sourcedf("order_id")===targetdf("order_id_tgt"),"inner")
      .filter(sourcedf("order_qty")=!=targetdf("order_qty_tgt")
        or  sourcedf("customer_name")=!=targetdf("customer_name_tgt"))
       .drop("order_id_tgt","order_qty_tgt","customer_name_tgt")

    changedDatadf.show()

//Newly inserted data
    val newDataDf=sourcedf.join(targetdf,sourcedf("order_id")===targetdf("order_id_tgt"),"left_anti")
                                            .drop("order_id_tgt","order_qty_tgt","customer_name_tgt")

    newDataDf.show()
    val delDf=targetdf.join(sourcedf,targetdf("order_id_tgt")===sourcedf("order_id"),"left_anti")
                                        .drop("order_id_tgt","order_qty_tgt","customer_name_tgt")

    delDf.show()

    val finaldf=unchangedDatadf.union(changedDatadf).union(newDataDf).orderBy("order_id")
    finaldf.coalesce(1).write.parquet("s3n://rajesh-test-buck/test")


    sparkSession.close()
  }
  // Creating Redshift JDBC URL
  def getMysqlJdbcUrl(mysqlConfig: Config): String = {
    val host = mysqlConfig.getString("hostname")
    val port = mysqlConfig.getString("port")
    val database = mysqlConfig.getString("database")
    s"jdbc:mysql://$host:$port/$database?autoReconnect=true&useSSL=false"
  }

}
