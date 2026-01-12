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

package uk.gov.hmrc.disareturnsvalidation.repositories

import org.mongodb.scala.bson.ObjectId
import play.api.Configuration
import uk.gov.hmrc.disareturnsvalidation.models.SubscriptionToTransfer
import uk.gov.hmrc.mongo.MongoComponent
import uk.gov.hmrc.mongo.workitem.{WorkItem, WorkItemFields, WorkItemRepository}

import java.time.{Duration, Instant}
import javax.inject.{Inject, Singleton}
import scala.concurrent.{ExecutionContext, Future}

@Singleton
class SubscriptionToTransferRepository @Inject() (
    configuration: Configuration,
    mongoComponent: MongoComponent
)(implicit ec: ExecutionContext)
    extends WorkItemRepository[SubscriptionToTransfer](
      collectionName = "subscriptionToTransferWorkItems",
      mongoComponent = mongoComponent,
      itemFormat = SubscriptionToTransfer.format,
      workItemFields = WorkItemFields.default
    ) {
  override def now(): Instant = Instant.now()
  override val inProgressRetryAfter: Duration =
    configuration.get[Duration]("queue.retryAfter")

  def pullOutstanding(
      now: Instant,
      max: Int
  ): Future[Seq[WorkItem[SubscriptionToTransfer]]] = ???

  def complete(id: ObjectId, now: Instant): Future[Unit] = ???

  def markAsFailed(
      id: ObjectId,
      failureCount: Int,
      reason: String,
      availableAt: Instant
  ): Future[Unit] = ???
}
