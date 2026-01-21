package model

import java.time.LocalDateTime

case class Wallet(
  wallet_id: String,
  user_id: String,
  currency: String,        // BDT
  balance: String,
  status: String,          // ACTIVE / INACTIVE
)