package bronze
import model.Transaction

case class BronzeTransaction(
  raw: Transaction,
  ingestion_ts: String,
  source_file: String,
  line_no: Int,
  raw_line: Option[String] = None   // optional, useful for audits
)
