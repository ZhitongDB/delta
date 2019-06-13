/*
 * Copyright 2019 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.execution

import io.delta.DeltaTable

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.commands.DeleteCommand
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.internal.SQLConf

trait DeltaTableOperations { self: DeltaTable =>

  def delete(condition: String): Unit = {
    delete(functions.expr(condition))
  }

  def delete(condition: Column): Unit = {
    val sparkSession = SparkSession.getActiveSession.getOrElse(self.toDF.sparkSession)
    val delete: LogicalPlan = Delete(self.toDF.queryExecution.analyzed, Some(condition.expr))
    val analyzer = sparkSession.sessionState.analyzer
    val resolvedDelete = analyzer.executeAndCheck(delete)

    val deleteCommand =
      PreprocessTableDelete(sparkSession.sessionState.conf)(resolvedDelete)
        .asInstanceOf[RunnableCommand]

    deleteCommand.run(sparkSession)
  }

  def delete(): Unit = {
    val sparkSession = SparkSession.getActiveSession.getOrElse(self.toDF.sparkSession)
    val delete: LogicalPlan = Delete(self.toDF.queryExecution.analyzed, None)
    val analyzer = sparkSession.sessionState.analyzer
    val resolvedDelete = analyzer.executeAndCheck(delete)
    val deleteCommand =
      PreprocessTableDelete(sparkSession.sessionState.conf)(resolvedDelete)
        .asInstanceOf[RunnableCommand]

    deleteCommand.run(sparkSession)
  }
}

/**
 * Perform DELETE on a table, in which all the rows are deleted if and only the condition is true
 *
 * @param child the logical plan representing target table
 * @param condition: Only rows that match the condition will be deleted.
 */
case class Delete(
  child: LogicalPlan,
  condition: Option[Expression])
  extends UnaryNode {
  override def output: Seq[Attribute] = Seq.empty
}

/**
 * Preprocess the [[Delete]] plan
 */
case class PreprocessTableDelete(conf: SQLConf) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
      val d = plan.asInstanceOf[Delete]
      val index = EliminateSubqueryAliases(d.child) match {
        case DeltaFullTable(tahoeFileIndex) =>
          tahoeFileIndex
        case o =>
          throw DeltaErrors.notADeltaSourceException("DELETE", Some(o))
      }
      DeleteCommand(index, d.child, d.condition)
  }
}

