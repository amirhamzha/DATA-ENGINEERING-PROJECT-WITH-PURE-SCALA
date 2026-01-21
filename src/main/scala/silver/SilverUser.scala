package silver

import java.time.LocalDate

case class SilverUser(
  userId: String,
  mobileNo: String,
  nidNumber: String,
  kycStatus: String,
  riskScore: Double,
  riskLevel: String,
  status: String,
  createdAt: LocalDate,
  isHighRisk: Boolean
)
