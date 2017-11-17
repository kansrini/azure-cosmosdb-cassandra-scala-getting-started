package com.azure.cosmosdb.cassandra.scala

import java.net.InetAddress
import java.util.UUID

import com.datastax.spark.connector._
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.util.Random
import com.azure.cosmosdb.cassandra.scala.util._

case class OrderItem (id: Int, shop: String, customerName: String, item: String, mobile: String, price: Double,
                      quantity: Int, coupon_used: Boolean, shop_ip: InetAddress, transaction_id: UUID )

object InsertCassandraDataExample {

  def main(args: Array[String]): Unit = {
    val sc = AppConfig.getCassandraSparkContext()

    //Recreating the keyspace
    Utils.recreateKeyspace(sc, AppConfig.keyspace);

    //Recreating the table
    Utils.recreateTable(sc, AppConfig.keyspace, AppConfig.orderTable)

    //insert single Row
    val collection = sc.parallelize(Seq(OrderItem(99, "Redmond", "Alex", "ABC", "234534651", 23.3, 1, false,
      InetAddress.getLocalHost, UUID.randomUUID())))
    collection.saveToCassandra(AppConfig.keyspace, AppConfig.orderTable)
    validateInsertedRow(sc, 99, "Redmond")

    //Insert muiltiple rows into orderitem table
    val recordSize = 10
    var orderItems = buildOrderItemRecords(sc, recordSize)
    orderItems.saveToCassandra(AppConfig.keyspace, AppConfig.orderTable)

    sc.stop
  }

  /**
    * Insert data in batch for given record size
    * @param sc SparkContext
    * @param recordSize number of records to be inserted
    * @return
    */
  def buildOrderItemRecords(sc: SparkContext, recordSize: Int): RDD[OrderItem] = {
    var id = 0;
    val chars = ('a' to 'z') ++ ('A' to 'Z')
    val numbers = ('0' to '9')

    sc.parallelize(0 until recordSize).map { s =>
      id += 1
      val shop = Utils.randomStringFromCharList(15, chars)
      val customerName = Utils.randomStringFromCharList(10, chars)
      val item = Utils.randomStringFromCharList(10, chars)
      val mobile = Utils.randomStringFromCharList(10, numbers)
      val price = Random.nextDouble()
      val quantity = Utils.randomStringFromCharList(3, numbers).toInt
      val coupon_used = math.random < 0.5

      val ticket_price = 12.50f
      val shop_ip = InetAddress.getLocalHost
      val transaction_id = UUID.randomUUID()

      OrderItem (id ,shop, customerName, item, mobile, price, quantity, coupon_used, shop_ip, transaction_id)
    }
  }

  def validateInsertedRow(sc: SparkContext, id: Int, shop: String): Unit = {
    val row = sc.cassandraTable(AppConfig.keyspace, AppConfig.orderTable).where("id=?", id).where("shop=?", shop).collect()

    assert(row.length == 1)
  }

}