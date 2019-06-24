package org.apache.spark.sql.delta

import org.apache.spark.sql.SparkSession

object ConcurrencyTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").getOrCreate()

    for (_ <- 1 to 3) {
      val thread = new AppendThread(spark)
      thread.start()
    }
  }

  class AppendThread(spark: SparkSession) extends Thread {

    override def run(): Unit = {
      val data = spark.range(0, 5)
      var done = false
      while (!done) {
        try {
          data.write.format("delta").mode("append").save("/tmp/delta-test")
          done = true
        } catch {
          case _: Exception =>
            println("Concurrent append failed")
            Thread.sleep(1000)
        }
      }
    }
  }

}
