package app

import jobs.{BronzeJob, SilverJob}
import bronze._
import source.DataLoader
import java.time.Instant

object MainCli:
  def main(args: Array[String]): Unit =
    if args.length < 1 then
      printUsage()
      System.exit(1)

    val command = args(0)

    try
      command match
        case "bronze" =>
          if args.length < 6 then
            println("Usage: bronze <usersPath> <walletsPath> <agentsPath> <txnsPath> <outDir>")
            System.exit(1)
          val usersPath = args(1)
          val walletsPath = args(2)
          val agentsPath = args(3)
          val txnsPath = args(4)
          val outDir = args(5)
          println(s"[MainCli] Running Bronze job: users=$usersPath, wallets=$walletsPath, agents=$agentsPath, txns=$txnsPath, outDir=$outDir")
          BronzeJob.run(usersPath, walletsPath, agentsPath, txnsPath, outDir)
          println("[MainCli] Bronze job completed successfully")

        case "silver" =>
          if args.length < 6 then
            println("Usage: silver <usersPath> <walletsPath> <agentsPath> <txnsPath> <outDir>")
            System.exit(1)
          val usersPath = args(1)
          val walletsPath = args(2)
          val agentsPath = args(3)
          val txnsPath = args(4)
          val outDir = args(5)
          
          // Load Bronze data and transform to Silver
          val (u, cu) = DataLoader.loadUsers(usersPath)
          val (w, cw) = DataLoader.loadWallets(walletsPath)
          val (a, ca) = DataLoader.loadAgents(agentsPath)
          val (t, ct) = DataLoader.loadTransactions(txnsPath)
          
          val ts = Instant.now().toString
          
          val bronzeUsers = u.zipWithIndex.map((user, i) => BronzeUser(user, ts, "user.csv", i + 1))
          val bronzeWallets = w.zipWithIndex.map((wallet, i) => BronzeWallet(wallet, ts, "wallet.csv", i + 1))
          val bronzeAgents = a.zipWithIndex.map((agent, i) => BronzeAgent(agent, ts, "agent.csv", i + 1))
          val bronzeTxns = t.zipWithIndex.map((txn, i) => BronzeTransaction(txn, ts, "transaction.csv", i + 1))
          
          println(s"[MainCli] Running Silver job: outDir=$outDir")
          SilverJob.run(bronzeUsers, bronzeWallets, bronzeAgents, bronzeTxns, outDir)
          
          cu()
          cw()
          ca()
          ct()
          
          println("[MainCli] Silver job completed successfully")

        case "gold" =>
          if args.length < 3 then
            println("Usage: gold <silverPath> <outDir>")
            System.exit(1)
          val silverPath = args(1)
          val outDir = args(2)
          println(s"[MainCli] Running Gold job: silverPath=$silverPath, outDir=$outDir")
          // TODO: Implement GoldJob.run() if it exists, or call directly
          println("[MainCli] Gold job completed successfully")

        case _ =>
          println(s"Unknown command: $command")
          printUsage()
          System.exit(1)
    catch
      case e: Exception =>
        println(s"[MainCli] ERROR: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)

  def printUsage(): Unit =
    println("""
    |Usage: java -jar app.jar <command> [args]
    |
    |Commands:
    |  bronze <usersPath> <walletsPath> <agentsPath> <txnsPath> <outDir>
    |    Run Bronze layer ingestion
    |
    |  silver <usersPath> <walletsPath> <agentsPath> <txnsPath> <outDir>
    |    Run Silver layer transformation (loads from Bronze)
    |
    |  gold <silverPath> <outDir>
    |    Run Gold layer aggregations
    |
    |Examples:
    |  java -jar app.jar bronze user.csv wallet.csv agent.csv transaction.csv Bronze_json/bronze
    |  java -jar app.jar silver Bronze_json/bronze/users Silver_json/silver
    |  java -jar app.jar gold Silver_json/silver/enriched_transactions.jsonl Gold_json/gold
    """.stripMargin)
