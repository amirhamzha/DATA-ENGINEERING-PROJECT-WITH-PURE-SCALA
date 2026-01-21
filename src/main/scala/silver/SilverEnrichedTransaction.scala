package silver

import java.time.{LocalDate, LocalDateTime}

case class SilverEnrichedTransaction(
  txnId: String,
  amount: Double,
  senderUserId: String,
  senderRiskScore: Double,
  senderRiskLevel: String,
  receiverUserId: String,
  agentRegion: String,
  channel: String,
  isSuspicious: Boolean,
  isHighValue: Boolean,
  eventTime: LocalDateTime,
  processingDate: LocalDate
)
