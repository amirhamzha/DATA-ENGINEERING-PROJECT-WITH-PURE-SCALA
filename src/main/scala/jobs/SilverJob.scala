package jobs
import bronze._
import silver._
import utils.FileUtils
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
    
object SilverJob:

  def run(
      bronzeUsers: Iterator[BronzeUser],
      bronzeWallets: Iterator[BronzeWallet],
      bronzeAgents: Iterator[BronzeAgent],
      bronzeTransactions: Iterator[BronzeTransaction],
      outDir: String
  ): Unit =

    println("[SILVER] Starting Silver layer")
    val silverUsers = bronzeUsers.flatMap(SilverTransformer.bronzeToSilverUser).toVector
    val silverWallets = bronzeWallets.flatMap(SilverTransformer.bronzeToSilverWallet).toVector
    val silverAgents = bronzeAgents.flatMap(SilverTransformer.bronzeToSilverAgent).toVector

    // ---- IMPORTANT (Option A) ----
    // Convert bronzeTransactions to a collection so we can iterate multiple times
    val bronzeTxnsVector = bronzeTransactions.toVector
    println(s"bronze transactions count: ${bronzeTxnsVector.size}")

    // Build silverTransactions from the vector (if you need it elsewhere)
    val silverTransactions = bronzeTxnsVector.flatMap(SilverTransformer.bronzeToSilverTransaction)
    println(s"Silver transactions count: ${silverTransactions.size}")

    // Deduplicate transactions by txnId (keep first seen)
    val dedupTxns = silverTransactions
      .groupBy(_.txnId)
      .map { case (_, seq) => seq.head }
      .toVector
    println(s"Deduplicated transactions: ${dedupTxns.size}")

    // ---- Step 2: Build Lookup Maps ----
    val userMap = silverUsers.map(u => u.userId -> u).toMap
    val walletMap = silverWallets.map(w => w.walletId -> w).toMap
    val agentMap = silverAgents.map(a => a.agentId -> a).toMap

    // ---- Step 3: Enrich transactions (simple, keep one row per txn) ----
    val unmatchedPath = s"$outDir/unmatched_transactions.jsonl"
    val enrichedTxns = dedupTxns.map { t =>
    val senderWalletOpt = walletMap.get(t.senderWallet)
    val receiverWalletOpt = walletMap.get(t.receiverWallet)
    val senderUserOpt   = senderWalletOpt.flatMap(w => userMap.get(w.userId))
    val receiverUserOpt = receiverWalletOpt.flatMap(w => userMap.get(w.userId))

    // If either side refers to an agent id, try agent lookup
    val senderAgentOpt = agentMap.get(t.senderWallet)
    val receiverAgentOpt = agentMap.get(t.receiverWallet)

    // Determine agent region if any agent involved
    val agentRegion = senderAgentOpt.orElse(receiverAgentOpt).map(_.region).getOrElse("UNKNOWN")

      // Populate fields with best-effort resolution, do not drop rows
    val senderUserId = senderUserOpt.map(_.userId).getOrElse("UNKNOWN")
    val senderRiskScore = senderUserOpt.map(_.riskScore).getOrElse(0.0)
    val senderRiskLevel = senderUserOpt.map(_.riskLevel).getOrElse("UNKNOWN")
    val receiverUserId = receiverUserOpt.map(_.userId).getOrElse("UNKNOWN")

      // If completely unresolved, audit for later inspection
    if senderUserOpt.isEmpty && receiverUserOpt.isEmpty then
        val auditLine = s"{" +
          s"\"txnId\":\"${t.txnId}\"," +
          s"\"sender\":\"${t.senderWallet}\"," +
          s"\"receiver\":\"${t.receiverWallet}\"," +
          s"\"reason\":\"no_user_or_wallet_found\"}"
        FileUtils.appendLine(unmatchedPath, auditLine)

      SilverEnrichedTransaction(
        txnId = t.txnId,
        amount = t.amount,
        senderUserId = senderUserId,
        senderRiskScore = senderRiskScore,
        senderRiskLevel = senderRiskLevel,
        receiverUserId = receiverUserId,
        agentRegion = agentRegion,
        channel = t.channel,
        isSuspicious = t.isSuspicious,
        isHighValue = t.amount >= 0,
        eventTime = t.eventTime,
        processingDate = LocalDate.now()
      )
    }

    
    
    // Formatters: date = YYYY-MM-DD, datetime = yyyy-MM-dd HH:mm:ss
    val dateFormatter = DateTimeFormatter.ISO_LOCAL_DATE
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    val jsonLines = enrichedTxns.map { e =>
      s"""{"txnId":"${e.txnId}","amount":${e.amount},"senderUserId":"${e.senderUserId}","senderRiskScore":${e.senderRiskScore},"senderRiskLevel":"${e.senderRiskLevel}","receiverUserId":"${e.receiverUserId}","agentRegion":"${e.agentRegion}","channel":"${e.channel}","isSuspicious":${e.isSuspicious},"isHighValue":${e.isHighValue},"eventTime":"${e.eventTime.format(dateTimeFormatter)}","processingDate":"${e.processingDate.format(dateFormatter)}"}"""
    }
    // ---- Step 4: Write JSONL ----
    val outPath = s"$outDir/enriched_transactions.jsonl"

    // optional: log count before writing
    val writtenCount = jsonLines.size
    println(s"[SILVER] Enriched transactions to write: $writtenCount")

    // write as iterator to FileUtils (streaming write)
    FileUtils.writeLines(outPath, jsonLines.iterator)

    println(s"[SILVER] Written: $outPath")
    println("[SILVER] Completed successfully")
