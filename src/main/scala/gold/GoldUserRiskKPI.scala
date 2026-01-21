package gold

case class GoldRiskKPI(
  date: String,
  riskLevel: String,
  txnCount: Long,
  totalAmount: Double,
  highValueTxnCount: Long,
  suspiciousTxnCount: Long
)
