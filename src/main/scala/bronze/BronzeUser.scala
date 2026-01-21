package bronze
import model.User

case class BronzeUser(
  raw: User,
  ingestion_ts: String,
  source_file: String,
  line_no: Int
)
