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

package uk.gov.hmrc.disareturnsvalidation.controllers

import play.api.libs.json.{JsError, JsSuccess, JsValue, Json}
import play.api.mvc.{Action, ControllerComponents}
import play.mvc.Action
import uk.gov.hmrc.disareturnsvalidation.models.SubscriptionToTransfer
import uk.gov.hmrc.disareturnsvalidation.repositories.SubscriptionToTransferRepository
import uk.gov.hmrc.play.bootstrap.backend.controller.BackendController

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

class TransferController(
    cc: ControllerComponents,
    repo: SubscriptionToTransferRepository
)(implicit ec: ExecutionContext)
    extends BackendController(cc) {

  def enqueueSubmissionForTransfer(submissionId: String): Action[JsValue] =
    Action.async(parse.json) { implicit request =>
      request.body.validate[Seq[String]] match {
        case JsSuccess(subscriptionIds, _) if subscriptionIds.nonEmpty =>
          val now = Instant.now()

          val newWorkItems: Seq[Future[_]] =
            subscriptionIds.map { subscriptionId =>
              repo.pushNew(
                item = SubscriptionToTransfer(
                  subscriptionId = subscriptionId,
                  submissionId = submissionId
                ),
                availableAt = now
              )
            }

          Future.sequence(newWorkItems).map { _ =>
            Accepted(
              Json.obj(
                "submissionId" -> submissionId,
                "enqueuedCount" -> subscriptionIds.size
              )
            )
          }

        case JsSuccess(_, _) =>
          Future.successful(
            BadRequest("subscriptionIds list must not be empty")
          )

        case JsError(errors) =>
          Future.successful(
            BadRequest(
              Json.obj(
                "message" -> "Invalid request body, expected JSON array of subscriptionIds",
                "errors" -> JsError.toJson(errors)
              )
            )
          )
      }
    }
}
