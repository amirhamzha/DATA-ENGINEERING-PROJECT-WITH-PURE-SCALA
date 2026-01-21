package app
import model.User
import bronze._
import utils.FileUtils
import jobs.BronzeJob
import source.DataLoader
import source.CsvReader
import  jobs.SilverJob
import java.time.Instant

@main def run(): Unit =
  val (users, closeUsers) = DataLoader.loadUsers("src/main/scala/data/user.csv")
  val (wallets, closeWallets) = DataLoader.loadWallets("src/main/scala/data/wallet.csv")
  val (agents, closeAgents) = DataLoader.loadAgents("src/main/scala/data/agent.csv")
  val (txns, closeTxns) = DataLoader.loadTransactions("src/main/scala/data/transaction.csv")

  // Use iterators
  println(s"Users loaded: ${users.size}")       // converts iterator to full traversal
  println(s"Wallets loaded: ${wallets.size}")
  println(s"Agents loaded: ${agents.size}")
  println(s"Transactions loaded: ${txns.size}")

  // Manually close files after usage
  closeUsers()
  closeWallets()
  closeAgents()
  closeTxns()


  BronzeJob.run(
    usersPath = "src/main/scala/data/user.csv",
    walletsPath = "src/main/scala/data/wallet.csv",
    agentsPath = "src/main/scala/data/agent.csv",
    txnsPath = "src/main/scala/data/transaction.csv",
    outDir = "Bronze_json/bronze"
  )

// --------- PREPARE FOR SILVER (reuse your DataLoader style) ----------
  val (u2, cu2) = DataLoader.loadUsers("src/main/scala/data/user.csv")
  val (w2, cw2) = DataLoader.loadWallets("src/main/scala/data/wallet.csv")
  val (a2, ca2) = DataLoader.loadAgents("src/main/scala/data/agent.csv")
  val (t2, ct2) = DataLoader.loadTransactions("src/main/scala/data/transaction.csv")

  val ts = Instant.now().toString

  val bronzeUsers = u2.zipWithIndex.map((u,i) => BronzeUser(u, ts, "user.csv", i+1))
  val bronzeWallets = w2.zipWithIndex.map((w,i) => BronzeWallet(w, ts, "wallet.csv", i+1))
  val bronzeAgents = a2.zipWithIndex.map((a,i) => BronzeAgent(a, ts, "agent.csv", i+1))
  val bronzeTxns = t2.zipWithIndex.map((t,i) => BronzeTransaction(t, ts, "transaction.csv", i+1))

  // --------- SILVER ----------
  SilverJob.run(
    bronzeUsers = bronzeUsers,
    bronzeWallets = bronzeWallets,
    bronzeAgents = bronzeAgents,
    bronzeTransactions = bronzeTxns,
    outDir = "Silver_json/silver"
  )

  cu2()
  cw2()
  ca2()
  ct2()

  println("Bronze + Silver pipeline completed.")


  import jobs.GoldJob

// After SilverJob.run(...)
  GoldJob.run(
  silverPath = "Silver_json\\silver\\enriched_transactions.jsonl",
  goldOutDir = "Gold_json/gold"
  )
