import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day25WindowOperations {

  def main(args: Array[String]): Unit = {

    // =====================================================
    // DAY 25 - WINDOW OPERATIONS
    // =====================================================

    val conf = new SparkConf()
      .setAppName("Day25WindowOperations")
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.ui.showConsoleProgress", "false")

    val ssc = new StreamingContext(conf, Seconds(2))

    // Hide Spark logs for clean screenshots
    ssc.sparkContext.setLogLevel("OFF")

    // Checkpoint for window operations
    ssc.checkpoint("/tmp/day25-checkpoint")

    // =====================================================
    // SOCKET STREAM
    //
    // Format:
    // amount
    //
    // Example:
    // 1000
    // 2500
    // 500
    // =====================================================

    val sales = ssc
      .socketTextStream("localhost", 9999)
      .flatMap { line =>
        try {
          Some(line.trim.toDouble)
        } catch {
          case _: Exception => None
        }
      }

    // =====================================================
    // 1. countByWindow
    //
    // Window size = 10 seconds
    // Sliding interval = 2 seconds
    //
    // Counts transactions received during
    // the latest 10-second window.
    // =====================================================

    val transactionCount = sales.countByWindow(
      Seconds(10),
      Seconds(2)
    )

    transactionCount.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("==========================================")
        println("          COUNT BY WINDOW")
        println("==========================================")
        println("Window Size     : 10 seconds")
        println("Slide Interval  : 2 seconds")
        println("Transactions    : " + rdd.collect().head)
        println("==========================================")
      }
    }

    // =====================================================
    // 2. reduceByKeyAndWindow
    //
    // Create sales category key.
    // The value is the transaction amount.
    //
    // This calculates rolling sales total.
    // =====================================================

    val keyedSales = sales.map(amount => ("SALES", amount))

    val rollingSales = keyedSales.reduceByKeyAndWindow(
      (a: Double, b: Double) => a + b,
      Seconds(10),
      Seconds(2)
    )

    rollingSales.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("==========================================")
        println("        REDUCE BY KEY AND WINDOW")
        println("==========================================")
        println("Rolling Sales Total (10 sec):")

        rdd.collect().foreach {
          case (key, total) =>
            println(key + " -> " + total)
        }

        println("==========================================")
      }
    }

    // =====================================================
    // 3. SUDDEN INCREASE DETECTION
    //
    // If 10-second transaction count >= 5,
    // flag the window as HIGH ACTIVITY.
    // =====================================================

    transactionCount.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        val count = rdd.collect().head

        println()
        println("========== ACTIVITY MONITOR ==========")

        if (count >= 5) {
          println("ALERT: Sudden increase in transactions!")
          println("Transactions in 10-sec window: " + count)
        } else {
          println("Normal transaction activity")
          println("Transactions in 10-sec window: " + count)
        }

        println("=======================================")
      }
    }

    // =====================================================
    // START STREAMING
    // =====================================================

    ssc.start()

    println()
    println("==========================================")
    println("       DAY 25 - WINDOW STREAMING")
    println("==========================================")
    println("Batch Interval : 2 seconds")
    println("Window Size    : 10 seconds")
    println("Slide Interval : 2 seconds")
    println("Socket         : localhost:9999")
    println("Status         : WAITING FOR SALES")
    println()
    println("Send transaction amounts using nc:")
    println("1000")
    println("2500")
    println("500")
    println("==========================================")
    println()

    ssc.awaitTermination()
  }
}
