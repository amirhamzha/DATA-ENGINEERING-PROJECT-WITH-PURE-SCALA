package bronze
import model.Wallet

case class BronzeWallet(
  raw: Wallet,
  ingestion_ts: String,
  source_file: String,
  line_no: Int
)
