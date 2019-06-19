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

import org.apache.spark.sql.delta.{DeltaErrors, DeltaFullTable}
import org.apache.spark.sql.delta.commands.DeleteCommand
import io.delta.DeltaTable

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, ExtractValue, GetStructField}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.internal.SQLConf


trait DeltaTableOperations { self: DeltaTable =>

  def delete(condition: String): Unit = {
    delete(functions.expr(condition))
  }

  def delete(condition: Column): Unit = {
    val sparkSession = SparkSession.getActiveSession.getOrElse(self.toDF.sparkSession)
    val delete: LogicalPlan = DeleteOp(self.toDF.queryExecution.analyzed, Some(condition.expr))
    val analyzer = sparkSession.sessionState.analyzer
    val resolvedDelete = analyzer.executeAndCheck(delete)

    val deleteCommand =
      PreprocessTableDelete(sparkSession.sessionState.conf)(resolvedDelete)
        .asInstanceOf[RunnableCommand]

    deleteCommand.run(sparkSession)
  }

  def delete(): Unit = {
    val sparkSession = SparkSession.getActiveSession.getOrElse(self.toDF.sparkSession)
    val delete: LogicalPlan = DeleteOp(self.toDF.queryExecution.analyzed, None)
    val analyzer = sparkSession.sessionState.analyzer
    val resolvedDelete = analyzer.executeAndCheck(delete)
    val deleteCommand =
      PreprocessTableDelete(sparkSession.sessionState.conf)(resolvedDelete)
        .asInstanceOf[RunnableCommand]

    deleteCommand.run(sparkSession)
  }

  def update(set: Map[String, Column]): Unit = {
    val sparkSession = SparkSession.getActiveSession.getOrElse(self.toDF.sparkSession)
    val setColumns = getSetColumnsForColumn(set)
    val updateTable = makeUpdateTable(sparkSession, self, None, setColumns)
    val analyzer = sparkSession.sessionState.analyzer
    val resolvedUpdate = analyzer.executeAndCheck(updateTable)
    val updateCommand =
  }

  protected def getSetColumnsForColumn(set: Map[String, Column]): Seq[(String, Column)] = {
    var setColumns: Seq[(String, Column)] = Seq()
    for ((col, expr) <- set) {
      setColumns = setColumns :+ ((col, expr))
    }
    setColumns
  }

  protected def makeUpdateTable(
    sparkSession: SparkSession,
    target: DeltaTable,
    onCondition: Option[Column],
    setColumns: Seq[(String, Column)]): UpdateOp = {
    val updateColumns = setColumns.map { x => UnresolvedAttribute.quotedString(x._1) }
    val updateExpressions = setColumns.map{ x => x._2.expr }
    onCondition match {
      case Some(c) =>
        new UpdateOp(
          target.toDF.queryExecution.analyzed,
          updateColumns,
          updateExpressions,
          Some(c.expr))
      case None =>
        new UpdateOp(
          target.toDF.queryExecution.analyzed,
          updateColumns,
          updateExpressions,
          None)
    }
  }
}

/**
 * Perform DELETE on a table, in which all the rows are deleted if and only the condition is true
 *
 * @param child the logical plan representing target table
 * @param condition: Only rows that match the condition will be deleted.
 */
case class DeleteOp(
  child: LogicalPlan,
  condition: Option[Expression])
  extends UnaryNode {
  override def output: Seq[Attribute] = Seq.empty
}

/**
 * Perform UPDATE on a table
 *
 * @param child the logical plan representing target table
 * @param updateColumns: the to-be-updated target columns
 * @param updateExpressions: the corresponding update expression if the condition is matched
 * @param condition: Only rows that match the condition will be updated
 */
case class UpdateOp(
  child: LogicalPlan,
  updateColumns: Seq[Attribute],
  updateExpressions: Seq[Expression],
  condition: Option[Expression])
  extends UnaryNode {

  override def output: Seq[Attribute] = Seq.empty

  assert(updateColumns.size == updateExpressions.size)
}

/**
 * Preprocess the [[DeleteOp]] plan
 */
case class PreprocessTableDelete(conf: SQLConf) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val delete = plan.asInstanceOf[DeleteOp]
    val index = EliminateSubqueryAliases(delete.child) match {
      case DeltaFullTable(tahoeFileIndex) =>
        tahoeFileIndex
      case o =>
        throw DeltaErrors.notADeltaSourceException("DELETE", Some(o))
    }

    DeleteCommand(index, delete.child, delete.condition)
  }
}

/**
 * Preprocess the [[UpdateOp]] plan
 * - Adjust the column order, which could be out of order, based on the destination table
 * - Generate expressions to compute the value of all target columns in Delta table, while taking
 *   into account that the specified SET clause may only update some columns or nested fields of
 *   columns.
 */
case class PreprocessTableUpdate(conf: SQLConf)
  extends Rule[LogicalPlan] with UpdateExpressionsSupport {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    apply0(plan)
  }

  def apply0(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case UpdateOp(target, updateColumns, updateExprs, condition) =>
      val index = EliminateSubqueryAliases(target) match {
        case DeltaFullTable(tahoeFileIndex) =>
          tahoeFileIndex
        case o =>
          throw DeltaErrors.notADeltaSourceException("UPDATE", Some(o))
      }

      val targetColNameParts = updateColumns.map(_.asInstanceOf[UnresolvedAttribute].nameParts)
      val updateOps = targetColNameParts.zip(updateExprs).map {
        case (nameParts, expr) => UpdateOperation(nameParts, expr)
      }
      val alignedUpdateExprs = generateUpdateExpressions(target.output, updateOps, conf.resolver)
      UpdateCommand(index, target, alignedUpdateExprs, condition)
  }
}



