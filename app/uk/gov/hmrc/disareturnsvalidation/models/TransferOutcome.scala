package uk.gov.hmrc.disareturnsvalidation.models

import java.time.Instant

sealed trait TransferOutcome {
  def subscriptionId: String
}

object TransferOutcome {
  case class Sent(subscriptionId: String) extends TransferOutcome
  case class Retry(
      subscriptionId: String,
      reason: String,
      nextAttemptAt: Instant
  ) extends TransferOutcome
  case class Dead(subscriptionId: String, reason: String) extends TransferOutcome
}
