package jobs

import utils.FileUtils
import scala.io.Source
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object GoldJob:

  case class GoldRec(
      date: String,
      riskLevel: String,
      agentRegion: String,
      amount: Double,
      isHigh: Boolean,
      isSuspicious: Boolean
  )

  /** Convert string values from JSON line (simple, assumes consistent Silver JSONL format) */
  private def parseLine(line: String): Option[GoldRec] =
    if line.trim.isEmpty then return None

    try
      // remove braces and split by comma
      val kvs = line.trim.stripPrefix("{").stripSuffix("}").split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)").toVector
      val map = kvs.map { kv =>
        val Array(k, v) = kv.split(":", 2)
        k.trim.stripPrefix("\"").stripSuffix("\"") -> v.trim.stripPrefix("\"").stripSuffix("\"")
      }.toMap

      val date = map.getOrElse("processingDate", "1970-01-01").take(10)
      val riskLevel = map.getOrElse("senderRiskLevel", "UNKNOWN")
      val agentRegion = map.getOrElse("agentRegion", "UNKNOWN")
      val amount = map.get("amount").map(_.toDouble).getOrElse(0.0)
      val isHigh = map.get("isHighValue").map(_.toLowerCase == "true").getOrElse(amount > 30000.0)
      val isSuspicious = map.get("isSuspicious").map(_.toLowerCase == "true").getOrElse(false)

      Some(GoldRec(date, riskLevel, agentRegion, amount, isHigh, isSuspicious))
    catch
      case _: Throwable => None // skip malformed lines

  def run(silverPath: String, goldOutDir: String): Unit =
    println("[GOLD] Starting Gold job (fast, Silver-style)")

    val dateFormatter = DateTimeFormatter.ISO_LOCAL_DATE

    // read all lines as Vector (1000 rows is fine)
    val src = Source.fromFile(silverPath)
    val lines = try src.getLines().toVector finally src.close()

    println(s"[GOLD] Silver lines read: ${lines.size}")

    // parse all lines into GoldRec
    val recs = lines.flatMap(parseLine)
    println(s"[GOLD] Valid Silver records: ${recs.size}")

    // --- Daily KPI ---
    val daily = recs.groupBy(_.date).toSeq.sortBy(_._1).map { case (date, rs) =>
      val totalTxnCount = rs.size
      val totalAmount = rs.map(_.amount).sum
      val highValueTxnCount = rs.count(_.isHigh)
      val suspiciousTxnCount = rs.count(_.isSuspicious)
      s"""{"date":"$date","totalTxnCount":"$totalTxnCount","totalAmount":"$totalAmount","highValueTxnCount":"$highValueTxnCount","suspiciousTxnCount":"$suspiciousTxnCount"}"""
    }
    FileUtils.writeLines(s"$goldOutDir/gold_daily_kpi.jsonl", daily.iterator)

    // --- Risk KPI ---
    val risk = recs.groupBy(r => (r.date, r.riskLevel)).toSeq.sortBy(r => (r._1._1, r._1._2)).map { case ((date, riskLevel), rs) =>
      val txnCount = rs.size
      val totalAmount = rs.map(_.amount).sum
      val highValueTxnCount = rs.count(_.isHigh)
      val suspiciousTxnCount = rs.count(_.isSuspicious)
      s"""{"date":"$date","riskLevel":"$riskLevel","txnCount":"$txnCount","totalAmount":"$totalAmount","highValueTxnCount":"$highValueTxnCount","suspiciousTxnCount":"$suspiciousTxnCount"}"""
    }
    FileUtils.writeLines(s"$goldOutDir/gold_risk_kpi.jsonl", risk.iterator)

    // --- Agent Region KPI ---
    val agent = recs.groupBy(r => (r.date, r.agentRegion)).toSeq.sortBy(r => (r._1._1, r._1._2)).map { case ((date, region), rs) =>
      val txnCount = rs.size
      val totalAmount = rs.map(_.amount).sum
      val highValueTxnCount = rs.count(_.isHigh)
      val suspiciousTxnCount = rs.count(_.isSuspicious)
      s"""{"date":"$date","agentRegion":"$region","txnCount":"$txnCount","totalAmount":"$totalAmount","highValueTxnCount":"$highValueTxnCount","suspiciousTxnCount":"$suspiciousTxnCount"}"""
    }
    FileUtils.writeLines(s"$goldOutDir/gold_agent_region_kpi.jsonl", agent.iterator)

    println(s"[GOLD] Gold KPI files written to $goldOutDir")
