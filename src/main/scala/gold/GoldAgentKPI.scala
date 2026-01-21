package gold

case class GoldAgentRegionKPI(
  date: String,
  agentRegion: String,
  txnCount: Long,
  totalAmount: Double,
  highValueTxnCount: Long,
  suspiciousTxnCount: Long
)
