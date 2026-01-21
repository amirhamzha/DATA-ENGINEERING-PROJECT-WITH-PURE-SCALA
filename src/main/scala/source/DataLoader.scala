package source

import model._
import java.sql.Timestamp

object DataLoader:

  /** Lazy loader for Users with manual close */
  def loadUsers(path: String): (Iterator[User], () => Unit) =
    // Users have 8 columns
    val (rows, close) = CsvReader.read(path, expectedColumns = 8)
    val users = rows.map { row =>
      User(
        row(0),
        row(1),
        row(2),
        row(3),
        row(4),
        row(5),
        row(6),
        row(7)
      )
    }
    (users, close)

  /** Lazy loader for Wallets */
  def loadWallets(path: String): (Iterator[Wallet], () => Unit) =
    // Wallets have 5 columns
    val (rows, close) = CsvReader.read(path, expectedColumns = 5)
    val wallets = rows.map { row =>
      Wallet(
        row(0),          // wallet_id
        row(1),          // user_id
        row(2),          // currency
        row(3), // balance
        row(4)           // status
      )
    }
    (wallets, close)

  /** Lazy loader for Agents */
  def loadAgents(path: String): (Iterator[Agent], () => Unit) =
    // Agents have 5 columns
    val (rows, close) = CsvReader.read(path, expectedColumns = 5)
    val agents = rows.map { row =>
      Agent(
        row(0),          // agent_id
        row(1),          // agent_name
        row(2),          // location
        row(3),          // region
        row(4)  // commission_rate
      )
    }
    (agents, close)

  /** Lazy loader for Transactions */
  def loadTransactions(path: String): (Iterator[Transaction], () => Unit) =
    // Transactions have 8 columns
    val (rows, close) = CsvReader.read(path, expectedColumns = 8)
    val txns = rows.map { row =>
      Transaction(
        row(0),          // txn_id
        row(1),          // sender_wallet
        row(2),          // receiver_wallet
        row(3),          // txn_type
        row(4), // amount
        row(5),          // channel
        row(6),          // timestamp as String
        row(7)           // is_suspicious as String
      )
    }
    (txns, close)

end DataLoader
