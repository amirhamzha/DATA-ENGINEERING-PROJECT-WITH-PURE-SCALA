package scheduler

import java.io.File
import java.time.{LocalDateTime, LocalDate}
import scala.sys.process._
import scala.util.{Try, Using}
import scala.concurrent.{Future, duration}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

/**
 * Pure Scala scheduler for the data pipeline
 * Runs Bronze → Silver → Gold jobs on schedule
 * 
 * Usage:
 *   scala PipelineScheduler.scala daily      # Run daily at midnight
 *   scala PipelineScheduler.scala now        # Run immediately
 *   scala PipelineScheduler.scala interval 3600  # Run every 3600 seconds (1 hour)
 */

object PipelineScheduler:
  
  // Configuration
  val JAR_PATH = "target/scala-3.7.4/internship-pipeline.jar"
  val DATA_DIR = "src/main/scala/data"
  val OUTPUT_BASE = "."
  val LOG_DIR = "scheduler_logs"
  
  case class LogWriter(taskName: String):
    def log(message: String): Unit =
      val timestamp = LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
      println(s"[$timestamp] $message")
  
  def runBronzeJob(): Int =
    val logger = LogWriter("bronze_job")
    logger.log("Starting Bronze job...")
    
    val cmd = Seq(
      "java", "-jar", JAR_PATH, "bronze",
      s"$DATA_DIR/user.csv",
      s"$DATA_DIR/wallet.csv",
      s"$DATA_DIR/agent.csv",
      s"$DATA_DIR/transaction.csv",
      s"$OUTPUT_BASE/Bronze_json/bronze"
    )
    
    Try {
      val process = cmd.run(ProcessLogger(line => logger.log(s"  [bronze] $line")))
      val exitCode = process.exitValue()
      if exitCode == 0 then
        logger.log("✓ Bronze job completed successfully")
      else
        logger.log(s"✗ Bronze job failed with exit code $exitCode")
      exitCode
    }.getOrElse {
      logger.log("✗ Bronze job failed with exception")
      1
    }
  
  def runSilverJob(): Int =
    val logger = LogWriter("silver_job")
    logger.log("Starting Silver job...")
    
    val cmd = Seq(
      "java", "-jar", JAR_PATH, "silver",
      s"$DATA_DIR/user.csv",
      s"$DATA_DIR/wallet.csv",
      s"$DATA_DIR/agent.csv",
      s"$DATA_DIR/transaction.csv",
      s"$OUTPUT_BASE/Silver_json/silver"
    )
    
    Try {
      val process = cmd.run(ProcessLogger(line => logger.log(s"  [silver] $line")))
      val exitCode = process.exitValue()
      if exitCode == 0 then
        logger.log("✓ Silver job completed successfully")
      else
        logger.log(s"✗ Silver job failed with exit code $exitCode")
      exitCode
    }.getOrElse {
      logger.log("✗ Silver job failed with exception")
      1
    }
  
  def runGoldJob(): Int =
    val logger = LogWriter("gold_job")
    logger.log("Starting Gold job...")
    
    val cmd = Seq(
      "java", "-jar", JAR_PATH, "gold",
      s"$OUTPUT_BASE/Silver_json/silver/enriched_transactions.jsonl",
      s"$OUTPUT_BASE/Gold_json/gold"
    )
    
    Try {
      val process = cmd.run(ProcessLogger(line => logger.log(s"  [gold] $line")))
      val exitCode = process.exitValue()
      if exitCode == 0 then
        logger.log("✓ Gold job completed successfully")
      else
        logger.log(s"✗ Gold job failed with exit code $exitCode")
      exitCode
    }.getOrElse {
      logger.log("✗ Gold job failed with exception")
      1
    }
  
  def runPipeline(): Unit =
    val timestamp = LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    println("\n" + "="*80)
    println(s"[$timestamp] Starting pipeline run (Bronze → Silver → Gold)")
    println("="*80)
    
    // Create log directory if it doesn't exist
    new File(LOG_DIR).mkdirs()
    
    // Run jobs in sequence
    val bronzeExit = runBronzeJob()
    Thread.sleep(2000) // 2 second delay
    
    val silverExit = runSilverJob()
    Thread.sleep(2000)
    
    val goldExit = runGoldJob()
    
    println("="*80)
    if bronzeExit == 0 && silverExit == 0 && goldExit == 0 then
      println(s"[${LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))}] ✓ Pipeline run COMPLETED successfully")
    else
      println(s"[${LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))}] ✗ Pipeline run FAILED (check logs above)")
    println("="*80 + "\n")
  
  def scheduleDaily(): Unit =
    println("""
    |╔════════════════════════════════════════════════════════════════════════════════╗
    |║ Scala Pipeline Scheduler - Daily Mode                                         ║
    |║ Runs daily at midnight (00:00)                                                ║
    |║ Press Ctrl+C to stop                                                          ║
    |╚════════════════════════════════════════════════════════════════════════════════╝
    """.stripMargin)
    
    var lastRun = LocalDate.now().minusDays(1)
    
    while true do
      val now = LocalDate.now()
      val currentTime = LocalDateTime.now()
      
      // Check if it's a new day and past midnight
      if now.isAfter(lastRun) && currentTime.getHour == 0 && currentTime.getMinute < 1 then
        runPipeline()
        lastRun = now
      
      Thread.sleep(60000) // Check every minute
  
  def scheduleInterval(seconds: Int): Unit =
    println(s"""
    |╔════════════════════════════════════════════════════════════════════════════════╗
    |║ Scala Pipeline Scheduler - Interval Mode                                      ║
    |║ Runs every ${seconds} seconds                                                    ║
    |║ Press Ctrl+C to stop                                                          ║
    |╚════════════════════════════════════════════════════════════════════════════════╝
    """.stripMargin)
    
    while true do
      runPipeline()
      println(s"Waiting ${seconds}s until next run...\n")
      Thread.sleep(seconds * 1000L)
  
  def main(args: Array[String]): Unit =
    if args.isEmpty then
      println("""
      |Usage: scala PipelineScheduler.scala [command]
      |
      |Commands:
      |  daily              Run at midnight every day (default)
      |  now                Run immediately (once)
      |  interval <seconds> Run every N seconds
      |
      |Examples:
      |  scala PipelineScheduler.scala daily
      |  scala PipelineScheduler.scala now
      |  scala PipelineScheduler.scala interval 3600
      """.stripMargin)
    else
      args(0) match
        case "daily" =>
          scheduleDaily()
        
        case "now" =>
          runPipeline()
        
        case "interval" =>
          if args.length < 2 then
            println("Error: interval requires a number of seconds")
            println("Usage: scala PipelineScheduler.scala interval <seconds>")
            System.exit(1)
          val seconds = Try(args(1).toInt).getOrElse {
            println(s"Error: '${args(1)}' is not a valid number")
            System.exit(1)
            0
          }
          scheduleInterval(seconds)
        
        case cmd =>
          println(s"Unknown command: $cmd")
          println("Use 'daily', 'now', or 'interval <seconds>'")
          System.exit(1)
