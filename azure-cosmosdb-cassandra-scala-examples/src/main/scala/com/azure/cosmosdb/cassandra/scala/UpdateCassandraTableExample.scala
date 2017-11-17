package com.azure.cosmosdb.cassandra.scala

import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import com.azure.cosmosdb.cassandra.scala.util._

/**
  * Validates update operation on cassandra table
  */
object UpdateCassandraTableExample {

  def main(args: Array[String]): Unit = {
    var sc = AppConfig.getCassandraSparkContext()

    //Recreating the keyspace
    Utils.recreateKeyspace(sc, AppConfig.keyspace);

    //Recreating the table
    Utils.recreateTable(sc, AppConfig.keyspace, AppConfig.orderTable)

    insertOrderItemData(sc)
    updateOrderItemData(sc)
    validateOrderItemData(sc)
    sc.stop()
  }

  def insertOrderItemData(sc: SparkContext): Unit= {
    val collection = sc.parallelize(Seq(
      (1, "Singapore", "Andy", "Soap", 1, 10),
      (2, "Melbourne", "Wolverine", "Knife", 1, 180.2),
      (3, "Delhi", "Raja", "Mask", 3, 510)
    ))
    collection.saveToCassandra(AppConfig.keyspace, AppConfig.orderTable,
      SomeColumns("id", "shop", "customer_name", "item", "quantity", "price"))
  }

  /**
    * updating values of non primary key columns
    * @param sc SparkContext
    */
  def updateOrderItemData(sc: SparkContext): Unit = {
    val collection = sc.parallelize((Seq(
      (1, "Singapore", "Andy Joe", "Soap", 3, 100, true),
      (2, "Melbourne", "Wolverine", "Knife", 5, 180.2, true)
    )))

    collection.saveToCassandra(AppConfig.keyspace, AppConfig.orderTable,
      SomeColumns("id", "shop", "customer_name", "item", "quantity", "price", "coupon_used"))
  }

  /**
    * validate updated column values
    * @param sc SparkContext
    */
  def validateOrderItemData(sc: SparkContext): Unit= {
    //Check records are updated
    val singaporeData = sc.cassandraTable(AppConfig.keyspace, AppConfig.orderTable).where("id=?", 1).where("shop=?", "Singapore").collect()

    assert(singaporeData.length == 1)
    assert(singaporeData(0).getInt("quantity").equals(3))
    assert(singaporeData(0).getString("customer_name").equals("Andy Joe"))

    val melbourneData = sc.cassandraTable(AppConfig.keyspace, AppConfig.orderTable).where("id=?", 2).where("shop=?", "Melbourne").collect()

    assert(melbourneData.length == 1)
    assert(melbourneData(0).getInt("quantity").equals(5))
    assert(melbourneData(0).getBoolean("coupon_used").equals(true))
  }

}
