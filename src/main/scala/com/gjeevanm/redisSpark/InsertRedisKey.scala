package com.gjeevanm.redisSpark

import com.redislabs.provider.redis._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object InsertRedisKey extends App {
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  val conf = new SparkConf()
    .set("spark.redis.host", "localhost")
    .set("spark.redis.port", "6379")
    .set("spark.redis.auth", "")

  val spark = SparkSession.builder().config(conf).master("local[*]").getOrCreate()
  val sc = spark.sparkContext

  /*
  Pushing keys to Redis
  ttl - 0 never gets deleted
   */
  val inputRDD = sc.parallelize(Seq(("name", "Alex"), ("name", "Bob")))

  sc.toRedisHASH(inputRDD, "table1", 0)


  println("Completed Successfully ..")

  /*
   retrieve specific pattern keys
   */

  val outputRDD = sc.fromRedisKV("key*")

  /*
  Printing the keys
   */
  outputRDD.foreach(println(_))

  /*
   Expire key - Set ttl to 1
   */
  sc.toRedisKV(outputRDD, 1)


}
