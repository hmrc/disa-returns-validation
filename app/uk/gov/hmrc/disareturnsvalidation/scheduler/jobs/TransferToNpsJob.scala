/*
 * Copyright 2026 HM Revenue & Customs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.hmrc.disareturnsvalidation.scheduler.jobs

import play.api.Configuration
import uk.gov.hmrc.disareturnsvalidation.connectors.{
  NpsConnector,
  StorageConnector
}
import uk.gov.hmrc.disareturnsvalidation.models.{
  Subscription,
  SubscriptionToTransfer,
  TransferOutcome
}
import uk.gov.hmrc.disareturnsvalidation.repositories.SubscriptionToTransferRepository
import uk.gov.hmrc.mongo.workitem.WorkItem

import java.time.Instant
import javax.inject.Inject
import scala.concurrent.{ExecutionContext, Future}

class TransferToNpsJob @Inject() (
    repo: SubscriptionToTransferRepository,
    storageConnector: StorageConnector,
    npsConnector: NpsConnector,
    config: Configuration
)(implicit ec: ExecutionContext) {

  private val pullSize: Int = config.get[Int]("transfer.pullSize")
  private val maxBatch: Int = config.get[Int]("transfer.maxBatchSize")

  def runOnce(): Future[Unit] =
    repo.pullOutstanding(Instant.now(), max = pullSize).flatMap { items =>
      val groups = items.groupBy(_.item.submissionId).toSeq

      groups.foldLeft(Future.unit) { case (acc, (submissionId, group)) =>
        acc.flatMap(_ => processSubmissionGroup(submissionId, group))
      }
    }

  private def processSubmissionGroup(
      submissionId: String,
      workItems: Seq[WorkItem[SubscriptionToTransfer]]
  ): Future[Unit] = {
    val currentTime = Instant.now()

    val subscriptionIds = workItems.map(_.item.subscriptionId).distinct

    for {
      subscriptions <- storageConnector.fetchSubscriptionBatchByIds(subscriptionIds)
      subById = subscriptions.map(s => s.subscriptionId -> s).toMap
      (found, missing) = workItems.partition(wi => subById.contains(wi.item.subscriptionId))
      foundPairs = found.map(wi => wi -> subById(wi.item.subscriptionId))
      _ <- handleMissing(missing, currentTime)
      _ <- runBatches(submissionId, foundPairs, currentTime)
    } yield ()
  }

  private def handleMissing(
      missing: Seq[WorkItem[SubscriptionToTransfer]],
      now: Instant
  ): Future[Unit] = {
    if (missing.isEmpty) Future.unit
    else {
      val retryAt = now.plusSeconds(60)
      Future
        .sequence {
          missing.map { wi =>
            repo.markAsFailed(
              id = wi.id,
              failureCount = wi.failureCount + 1,
              reason = "NOT_FOUND_IN_STORAGE",
              availableAt = retryAt
            )
          }
        }
        .map(_ => ())
    }
  }

  private def runBatches(
      submissionId: String,
      workItemsWithSubs: Seq[(WorkItem[SubscriptionToTransfer], Subscription)],
      currentTime: Instant
  ): Future[Unit] = {

    val batches: Seq[Seq[(WorkItem[SubscriptionToTransfer], Subscription)]] =
      workItemsWithSubs.grouped(maxBatch).toSeq

    batches.foldLeft(Future.unit) { (acc, batch) =>
      acc.flatMap(_ => sendBatch(submissionId, batch, currentTime))
    }
  }

  private def sendBatch(
      submissionId: String,
      batch: Seq[(WorkItem[SubscriptionToTransfer], Subscription)],
      currentTime: Instant
  ): Future[Unit] = {

    val subscriptions: Seq[Subscription] = batch.map(_._2)

    for {
      outcomes <- npsConnector.sendBatch(submissionId, subscriptions)
      _ <- storageConnector.reportTransferResults(submissionId, outcomes)
      _ <- applyOutcomesToWorkItems(batch, outcomes, currentTime)
    } yield ()
  }

  private def applyOutcomesToWorkItems(
      batch: Seq[(WorkItem[SubscriptionToTransfer], Subscription)],
      outcomes: Seq[TransferOutcome],
      currentTime: Instant
  ): Future[Unit] = {

    val workItemBySubscriptionId: Map[String, WorkItem[SubscriptionToTransfer]] =
      batch.map { case (wi, sub) => sub.subscriptionId -> wi }.toMap

    val outcomesById: Map[String, TransferOutcome] =
      outcomes.map(o => o.subscriptionId -> o).toMap

    val updates: Seq[Future[Unit]] =
      workItemBySubscriptionId.toSeq.map { case (subscriptionId, wi) =>
        outcomesById.get(subscriptionId) match {
          case Some(TransferOutcome.Sent(_)) =>
            repo.complete(wi.id, currentTime)

          case Some(TransferOutcome.Retry(_, reason, nextAttemptAt)) =>
            repo.markAsFailed(
              id = wi.id,
              failureCount = wi.failureCount + 1,
              reason = reason,
              availableAt = nextAttemptAt
            )

          case Some(TransferOutcome.Dead(_, reason)) =>
            repo.complete(wi.id, currentTime)

          case None =>
            repo.markAsFailed(
              id = wi.id,
              failureCount = wi.failureCount + 1,
              reason = "NO_OUTCOME_RETURNED",
              availableAt = currentTime.plusSeconds(60)
            )
        }
      }

    Future.sequence(updates).map(_ => ())
  }
}
