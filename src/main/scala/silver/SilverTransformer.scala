package silver

import bronze._
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Try

object SilverTransformer:

  private val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val createdAtDtFmt = DateTimeFormatter.ofPattern("M/d/yy H:mm")

  // Convert Bronze User → SilverUser
  def bronzeToSilverUser(bu: BronzeUser): Option[SilverUser] =
    try
      val risk = bu.raw.risk_score.toDouble
      def parseCreatedAt(s: String): LocalDate =
        // Try datetime format first (e.g. "10/1/25 9:00"), then date-only formats
        val dtOpt = Try(LocalDateTime.parse(s, createdAtDtFmt).toLocalDate).toOption
        val dOpt = Try(LocalDate.parse(s, dateFormatter)).toOption
        dtOpt.orElse(dOpt).getOrElse(LocalDate.now())

      Some(
        SilverUser(
          userId = bu.raw.user_id,
          mobileNo = bu.raw.mobile_no,
          nidNumber = bu.raw.nid_number,
          kycStatus = bu.raw.kyc_status,
          riskScore = risk,
          riskLevel = bu.raw.risk_level,
          status = bu.raw.status,
          createdAt = parseCreatedAt(bu.raw.created_at),
          isHighRisk = risk > 80
        )
      )
    catch
      case _: Throwable => None // invalid row → skip

  // Bronze Wallet → SilverWallet
  def bronzeToSilverWallet(bw: BronzeWallet): Option[SilverWallet] =
    try
      Some(
        SilverWallet(
          walletId = bw.raw.wallet_id,
          userId = bw.raw.user_id,
          currency = bw.raw.currency,
          balance = bw.raw.balance.toDouble,
          status = bw.raw.status
        )
      )
    catch
      case _: Throwable => None

  // Bronze Agent → SilverAgent
  def bronzeToSilverAgent(ba: BronzeAgent): Option[SilverAgent] =
    try
      Some(
        SilverAgent(
          agentId = ba.raw.agent_id,
          agentName = ba.raw.agent_name,
          location = ba.raw.location,
          region = ba.raw.region,
          commissionRate = ba.raw.commission_rate.toDouble
        )
      )
    catch
      case _: Throwable => None

  // Bronze Transaction → SilverTransaction
  def bronzeToSilverTransaction(bt: BronzeTransaction): Option[SilverTransaction] =
    try
      Some(
        SilverTransaction(
          txnId = bt.raw.txn_id,
          senderWallet = bt.raw.sender_wallet,
          receiverWallet = bt.raw.receiver_wallet,
          txnType = bt.raw.txn_type,
          amount = bt.raw.amount.toDouble,
          channel = bt.raw.channel,
          eventTime = LocalDateTime.parse(bt.raw.timestamp, dtf),
          isSuspicious = bt.raw.is_suspicious == "1"
          
          
        )
      )
    catch
      case _: Throwable => None
