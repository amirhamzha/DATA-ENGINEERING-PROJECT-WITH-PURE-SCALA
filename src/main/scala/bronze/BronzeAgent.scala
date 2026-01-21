package bronze
import model.Agent

case class BronzeAgent(
  raw: Agent,
  ingestion_ts: String,
  source_file: String,
  line_no: Int
)
