package jobs

import source.DataLoader
import utils.FileUtils
import java.time.Instant
import java.time.format.DateTimeFormatter
import scala.util.{Try, Success, Failure}
import scala.util.control.NonFatal
import bronze._
import model._
import java.nio.file.Paths
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

object BronzeJob:

  private def isoNow(): String = Instant.now().toString

  /** Simple manual JSON serializer for small case classes (production -> use a library) */
  private def toJson(map: Map[String, Any]): String =
    // very small, safe serializer for strings/numbers/bools/null
    val body = map.map { case (k,v) =>
      val value = v match
        case s: String => "\"" + s.replace("\"","\\\"") + "\""
        case i: Int => i.toString
        case d: Double => d.toString
        case b: Boolean => b.toString
        case null => "null"
        case other => "\"" + other.toString.replace("\"","\\\"") + "\""
      "\"" + k + "\":" + value
    }.mkString(",")
    "{" + body + "}"

  def run(
    usersPath: String,
    walletsPath: String,
    agentsPath: String,
    txnsPath: String,
    outDir: String = "Bronze_json/bronze",
    batchSize: Int = 1000
  ): Unit =

    val ingestTs = isoNow()
    println(s"[BronzeJob] starting ingestion at $ingestTs")

    // -------- Users ----------
    val (usersIter, closeUsers) = DataLoader.loadUsers(usersPath)
    try
      val usersOut = s"$outDir/users/users_${ingestTs.replace(':','-')}.jsonl"
      println(s"[BronzeJob] writing users bronze -> $usersOut")
      // Process with index to capture line_no (1-based)
      val userLines = usersIter.zipWithIndex.map { case (u, idx) =>
        val bronze = BronzeUser(u, ingestTs, Paths.get(usersPath).getFileName.toString, idx + 1)
        // serialize: raw fields inside "raw" object
        val json = toJson(Map(
          "ingestion_ts" -> bronze.ingestion_ts,
          "source_file"  -> bronze.source_file,
          "line_no"      -> bronze.line_no.toString,

          "user_id"    -> bronze.raw.user_id,
          "mobile_no"  -> bronze.raw.mobile_no,
          "nid_number" -> bronze.raw.nid_number,
          "kyc_status" -> bronze.raw.kyc_status,
          "risk_score" -> bronze.raw.risk_score,
          "risk_level" -> bronze.raw.risk_level,
          "status"     -> bronze.raw.status,
          "created_at" -> bronze.raw.created_at
        ))
        json
      }
      FileUtils.writeLines(usersOut, userLines)
      println(s"[BronzeJob] users complete")
    catch
      case NonFatal(e) => println(s"[BronzeJob][ERROR] users ingestion failed: ${e.getMessage}")
    finally
      closeUsers()

    // -------- Wallets ----------
    val (walletsIter, closeWallets) = DataLoader.loadWallets(walletsPath)
    try
      val walletsOut = s"$outDir/wallets/wallets_${ingestTs.replace(':','-')}.jsonl"
      println(s"[BronzeJob] writing wallets bronze -> $walletsOut")
      val walletLines = walletsIter.zipWithIndex.map { case (w, idx) =>
        val bronze = BronzeWallet(w, ingestTs, Paths.get(walletsPath).getFileName.toString, idx + 1)
        toJson(Map(
          "ingestion_ts" -> bronze.ingestion_ts,
          "source_file" -> bronze.source_file,
          "line_no" -> bronze.line_no,
          "raw_wallet_id" -> bronze.raw.wallet_id,
          "raw_user_id" -> bronze.raw.user_id,
          "raw_currency" -> bronze.raw.currency,
          "raw_balance" -> bronze.raw.balance,
          "raw_status" -> bronze.raw.status,
        ))
      }
      FileUtils.writeLines(walletsOut, walletLines)
      println(s"[BronzeJob] wallets complete")
    catch
      case NonFatal(e) => println(s"[BronzeJob][ERROR] wallets ingestion failed: ${e.getMessage}")
    finally
      closeWallets()

    // -------- Agents ----------
    val (agentsIter, closeAgents) = DataLoader.loadAgents(agentsPath)
    try
      val agentsOut = s"$outDir/agents/agents_${ingestTs.replace(':','-')}.jsonl"
      println(s"[BronzeJob] writing agents bronze -> $agentsOut")
      val agentLines = agentsIter.zipWithIndex.map { case (a, idx) =>
        val bronze = BronzeAgent(a, ingestTs, Paths.get(agentsPath).getFileName.toString, idx + 1)
        toJson(Map(
          "ingestion_ts" -> bronze.ingestion_ts,
          "source_file" -> bronze.source_file,
          "line_no" -> bronze.line_no,
          "raw_agent_id" -> bronze.raw.agent_id,
          "raw_agent_name" -> bronze.raw.agent_name,
          "raw_location" -> bronze.raw.location,
          "raw_region" -> bronze.raw.region,
          "raw_commission_rate" -> bronze.raw.commission_rate
        ))
      }
      FileUtils.writeLines(agentsOut, agentLines)
      println(s"[BronzeJob] agents complete")
    catch
      case NonFatal(e) => println(s"[BronzeJob][ERROR] agents ingestion failed: ${e.getMessage}")
    finally
      closeAgents()

    // -------- Transactions ----------
    val (txnsIter, closeTxns) = DataLoader.loadTransactions(txnsPath)
    try
      val txnsOut = s"$outDir/transactions/transactions_${ingestTs.replace(':','-')}.jsonl"
      println(s"[BronzeJob] writing transactions bronze -> $txnsOut")
      val txnLines = txnsIter.zipWithIndex.map { case (t, idx) =>
        val bronze = BronzeTransaction(t, ingestTs, Paths.get(txnsPath).getFileName.toString, idx + 1, None)
        toJson(Map(
          "ingestion_ts" -> bronze.ingestion_ts,
          "source_file" -> bronze.source_file,
          "line_no" -> bronze.line_no,
          "raw_txn_id" -> bronze.raw.txn_id,
          "raw_sender_wallet" -> bronze.raw.sender_wallet,
          "raw_receiver_wallet" -> bronze.raw.receiver_wallet,
          "raw_txn_type" -> bronze.raw.txn_type,
          "raw_amount" -> bronze.raw.amount,
          "raw_channel" -> bronze.raw.channel,
          "raw_timestamp" -> bronze.raw.timestamp,
          "raw_is_suspicious" -> bronze.raw.is_suspicious
        ))
      }
      FileUtils.writeLines(txnsOut, txnLines)
      println(s"[BronzeJob] transactions complete")
    catch
      case NonFatal(e) => println(s"[BronzeJob][ERROR] transactions ingestion failed: ${e.getMessage}")
    finally
      closeTxns()

    println(s"[BronzeJob] ingestion finished")

end BronzeJob
