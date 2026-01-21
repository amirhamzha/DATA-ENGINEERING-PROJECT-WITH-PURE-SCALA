package silver

case class SilverWallet(
  walletId: String,
  userId: String,
  currency: String,
  balance: Double,
  status: String
)
