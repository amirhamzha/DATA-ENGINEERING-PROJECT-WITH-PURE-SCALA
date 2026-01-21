package model

case class Agent(
  agent_id: String,
  agent_name: String,
  location: String,       // e.g. Motijheel
  region: String,         // e.g. Dhaka Division
  commission_rate: String   
)