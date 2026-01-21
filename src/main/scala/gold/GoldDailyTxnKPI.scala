package gold

case class GoldDailyTxnKPI(
  date: String,
  totalTxnCount: Long,
  totalAmount: Double,
  highValueTxnCount: Long,
  suspiciousTxnCount: Long
)
