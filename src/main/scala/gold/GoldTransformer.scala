package gold

import silver.SilverEnrichedTransaction

object GoldTransformer:

  // -----------------------------
  // 1. Daily Platform KPI
  // -----------------------------
  def buildDailyTxnKPI(
      txns: Seq[SilverEnrichedTransaction]
  ): Seq[GoldDailyTxnKPI] =

    txns
      .groupBy(_.processingDate)
      .map { case (date, rows) =>
        val totalTxnCount = rows.size.toLong
        val totalAmount = rows.map(_.amount).sum
        val highValueTxnCount = rows.count(_.isHighValue).toLong
        val suspiciousTxnCount = rows.count(_.isSuspicious).toLong

        GoldDailyTxnKPI(
          date = date.toString(),
          totalTxnCount = totalTxnCount,
          totalAmount = totalAmount,
          highValueTxnCount = highValueTxnCount,
          suspiciousTxnCount = suspiciousTxnCount
        )
      }
      .toSeq
      .sortBy(_.date)

  // -----------------------------
  // 2. Risk Level KPI
  // -----------------------------
  def buildRiskKPI(
      txns: Seq[SilverEnrichedTransaction]
  ): Seq[GoldRiskKPI] =

    txns
      .groupBy(t => (t.processingDate, t.senderRiskLevel))
      .map { case ((date, riskLevel), rows) =>
        val txnCount = rows.size.toLong
        val totalAmount = rows.map(_.amount).sum
        val highValueTxnCount = rows.count(_.isHighValue).toLong
        val suspiciousTxnCount = rows.count(_.isSuspicious).toLong

        GoldRiskKPI(
          date = date.toString(),
          riskLevel = riskLevel,
          txnCount = txnCount,
          totalAmount = totalAmount,
          highValueTxnCount = highValueTxnCount,
          suspiciousTxnCount = suspiciousTxnCount
        )
      }
      .toSeq
      .sortBy(r => (r.date, r.riskLevel))

  // -----------------------------
  // 3. Agent Region KPI
  // -----------------------------
  def buildAgentRegionKPI(
      txns: Seq[SilverEnrichedTransaction]
  ): Seq[GoldAgentRegionKPI] =

    txns
      .groupBy(t => (t.processingDate, t.agentRegion))
      .map { case ((date, region), rows) =>
        val txnCount = rows.size.toLong
        val totalAmount = rows.map(_.amount).sum
        val highValueTxnCount = rows.count(_.isHighValue).toLong
        val suspiciousTxnCount = rows.count(_.isSuspicious).toLong

        GoldAgentRegionKPI(
          date = date.toString(),
          agentRegion = region,
          txnCount = txnCount,
          totalAmount = totalAmount,
          highValueTxnCount = highValueTxnCount,
          suspiciousTxnCount = suspiciousTxnCount
        )
      }
      .toSeq
      .sortBy(r => (r.date, r.agentRegion))
