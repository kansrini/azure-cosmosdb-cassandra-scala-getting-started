package com.azure.cosmosdb.cassandra.scala.util

import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkContext
import scala.util.Random

object Utils {

  //Create table command for order_item
  val createOrderTableCommand = "CREATE TABLE IF NOT EXISTS  %s.order_item (" +
    "id int, shop text, item text, coupon_used boolean, customer_name text, mobile text, price double, quantity int, " +
    "shop_ip inet, transaction_id uuid, PRIMARY KEY ((id, shop), item)) WITH CLUSTERING ORDER BY (item ASC) IF NOT EXISTS"

  //Create cache table with default TTL 10s
  val createCachTable = "create table IF NOT EXISTS  %s.cache_data (" +
    "key text, value text, PRIMARY KEY(key))WITH default_time_to_live = 10  IF NOT EXISTS"

  val dropTableCommand = "drop table IF EXISTS %s.%s"

  val dropKeyspaceCommand = "drop keyspace IF EXISTS %s"

  val createKeyspaceCommand = "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 }";

  /**
    * Utility to re-create the table. This method drops the table and create again
    * @param sc SparkContext
    * @param keyspace Keyspace name
    */
  def recreateKeyspace(sc: SparkContext, keyspace: String): Unit = {

    var connector = CassandraConnector.apply(sc.getConf)
    var session = connector.openSession()
    try {
      session.execute(dropKeyspaceCommand.format(keyspace))
      session.execute(createKeyspaceCommand.format(keyspace))

    } finally {
      session.close()
    }
  }

  /**
    * Utility to re-create the table. This method drops the table and create again
    * @param sc SparkContext
    * @param keyspace Keyspace name
    * @param table Table name
    */
  def recreateTable(sc: SparkContext, keyspace: String, table: String): Unit = {

    var connector = CassandraConnector.apply(sc.getConf)
    var session = connector.openSession()
    try {
      dropTable(session, keyspace, table)

      if (table.equals(AppConfig.orderTable)) {
        createOrderByTable(session, keyspace, table)
      }
      else {
        createCacheTable(session, keyspace, table)
      }

    } finally {
      session.close()
    }
  }

  /**
    * Utility to drop the table
    * @param session Spark-Cassandra-Connector Session
    * @param keyspace Keyspace name
    * @param table Table name
    */
  def dropTable(session: Session, keyspace: String, table: String): Unit = {
    try {

      session.execute(dropTableCommand.format(keyspace, table))
    } catch {
      case e: Exception => println("Ignoring table already exists response in drop table %s".format(table));
    }
  }

  /**
    * Utlity method to create order_item table
    * @param session Spark-Cassandra-Connector Session
    * @param keyspace Keyspace name
    * @param table Table name
    */
  def createOrderByTable(session: Session, keyspace: String, table: String): Unit = {
    try {
      session.execute(createOrderTableCommand.format(keyspace))
    } catch {
      case e: Exception => println("Ignoring table already exists response in create table %s".format(table));
    }
  }

  /**
    * Utility method to create cache table with TTL 10s for all the data stored in there
    * @param session Spark-Cassandra-Connector Session
    * @param keyspace Keyspace name
    * @param table Table name
    */
  def createCacheTable(session: Session, keyspace: String, table: String) {
    try {
      session.execute(createCachTable.format(keyspace))
    } catch {
      case e: Exception => println("Ignoring table already exists response in create table %s".format(table));
    }
  }

  /**
    * Utility method to generate random string of given length and allowed char set
    * @param length
    * @param chars
    * @return
    */
  def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val randomNum = Random.nextInt(chars.length)
      sb.append(chars(randomNum))
    }
    sb.toString
  }
}
