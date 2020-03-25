package com.gjeevanm.redisSpark

import com.redislabs.provider.redis.rdd.Keys
import com.redislabs.provider.redis.util.PipelineUtils._
import com.redislabs.provider.redis.{RedisConfig, RedisEndpoint}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.PipelineBase

object DeleteSpecificKeyinRedis extends App {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  val redisServerDnsAddress = "localhost"
  val redisPortNumber = 6379
  val redisPassword = ""
  val redisConfig = new RedisConfig(new RedisEndpoint(redisServerDnsAddress, redisPortNumber, redisPassword))

  val sparkconf = new SparkConf()
    .set("spark.redis.host", "localhost")
    .set("spark.redis.port", "6379")
    .set("spark.redis.auth", "")

  val spark = SparkSession
    .builder()
    .config(sparkconf)
    .appName("redis-app")
    .master("local[*]")
    .getOrCreate()

  import com.redislabs.provider.redis._

  val hsetTable = "hashtable"
  val inputRDD = spark.sparkContext.parallelize(Seq(("key1", "value1"), ("key2", "value"), ("key3", ""), ("key4", "")))
  spark.sparkContext.toRedisHASH(inputRDD, hsetTable)

  spark.sparkContext.fromRedisHash(hsetTable).foreach {
    key =>
      if (key._2 == "") {
        deleteKeys(hsetTable, key._1)
      }
  }

  def deleteKeys(hsetkey: String, keys: String): Unit = {

    implicit val readWriteConfig: ReadWriteConfig = ReadWriteConfig.fromSparkConf(sparkconf)

    spark.sparkContext.fromRedisKeyPattern(keys)
      .foreachPartition {
        part =>
          Keys.groupKeysByNode(redisConfig.hosts, part)
            .foreach {
              case (n, ks) =>
                val conn = n.connect()
                foreachWithPipeline(conn, ks) {
                  (pl, k) =>
                    (pl: PipelineBase).hdel(hsetkey, k)
                }
                conn.close()
            }
      }


  }

}
