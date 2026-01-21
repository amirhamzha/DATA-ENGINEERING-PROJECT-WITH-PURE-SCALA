package model

import scala.compiletime.ops.int
import scala.compiletime.ops.string

case class User(
user_id: String,
mobile_no: String,
nid_number: String,
kyc_status: String,
risk_score: String,
risk_level: String,
status: String,
created_at: String
)