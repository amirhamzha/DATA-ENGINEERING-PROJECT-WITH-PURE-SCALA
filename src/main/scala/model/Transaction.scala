package model
import scala.compiletime.ops.int

case class Transaction(
txn_id: String,
sender_wallet: String,
receiver_wallet: String,
txn_type: String,
amount: String,
channel: String,
timestamp: String,
is_suspicious: String
)

