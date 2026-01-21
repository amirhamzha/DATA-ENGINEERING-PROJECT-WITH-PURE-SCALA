// package silver

package silver

import java.time.LocalDateTime

case class SilverTransaction(
  txnId: String,
  senderWallet: String,
  receiverWallet: String,
  txnType: String,
  amount: Double,
  channel: String,
  eventTime: LocalDateTime,
  isSuspicious: Boolean,
  
)
