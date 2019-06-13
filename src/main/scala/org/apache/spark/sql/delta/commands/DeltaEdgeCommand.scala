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

package org.apache.spark.sql.delta.commands

import java.net.URI

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.files.TahoeBatchFileIndex
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{And, EqualNullSafe, Expression, ExpressionSet, InSubquery, IsNotNull, IsNull, ListQuery, Literal, Not}
import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}

/**
 * A helper trait for delta edge commands.
 */
trait DeltaEdgeCommand extends DeltaCommand {
  /**
   * Generates a map of file names to add file entries for operations where we will need to
   * rewrite files such as delete, merge, update. We expect file names to be unique, because
   * each file contains a UUID.
   *
   * Note(burak): I wonder if we'll ever hit scalability issues here. We can eventually change this
   * method to use the RocksDB ExternalMap.
   */
  protected def generateCandidateFileMap(
    basePath: Path,
    candidateFiles: Seq[AddFile]): Map[String, AddFile] = {
    val nameToAddFileMap = candidateFiles.map(add =>
      DeltaFileOperations.absolutePath(basePath.toString, add.path).toString -> add).toMap
    assert(nameToAddFileMap.size == candidateFiles.length,
      s"File name collisions found among:\n${candidateFiles.map(_.path).mkString("\n")}")
    nameToAddFileMap
  }

  /**
   * This method provides the RemoveFile actions that are necessary for files that are touched and
   * need to be rewritten in methods like Delete, Update, and Merge.
   *
   * @param deltaLog The DeltaLog of the table that is being operated on
   * @param nameToAddFileMap A map generated using `generateCandidateFileMap`.
   * @param filesToRewrite Absolute paths of the files that were touched. We will search for these
   *                       in `candidateFiles`. Obtained as the output of the `input_file_name`
   *                       function.
   * @param operationTimestamp The timestamp of the operation
   */
  protected def removeFilesFromPaths(
    deltaLog: DeltaLog,
    nameToAddFileMap: Map[String, AddFile],
    filesToRewrite: Seq[String],
    operationTimestamp: Long): Seq[RemoveFile] = {
    filesToRewrite.map { absolutePath =>
      val addFile = getTouchedFile(deltaLog.dataPath, absolutePath, nameToAddFileMap)
      addFile.removeWithTimestamp(operationTimestamp)
    }
  }

  /**
   * Build a base relation of files that need to be rewritten as part of an update/delete/merge
   * operation.
   */
  protected def buildBaseRelation(
    spark: SparkSession,
    txn: OptimisticTransaction,
    actionType: String,
    rootPath: Path,
    inputLeafFiles: Seq[String],
    nameToAddFileMap: Map[String, AddFile]): HadoopFsRelation = {
    val deltaLog = txn.deltaLog
    val fs = rootPath.getFileSystem(spark.sessionState.newHadoopConf())
    val scannedFiles = inputLeafFiles.map(f => getTouchedFile(rootPath, f, nameToAddFileMap))
    val fileIndex = new TahoeBatchFileIndex(
      spark, actionType, scannedFiles, deltaLog, rootPath, txn.snapshot)
    HadoopFsRelation(
      fileIndex,
      partitionSchema = txn.metadata.partitionSchema,
      dataSchema = txn.metadata.schema,
      bucketSpec = None,
      deltaLog.snapshot.fileFormat,
      txn.metadata.format.options)(spark)
  }

  /**
   * Find the AddFile record corresponding to the file that was read as part of a
   * delete/update/merge operation.
   *
   * @param filePath The path to a file. Can be either absolute or relative
   * @param nameToAddFileMap Map generated through `generateCandidateFileMap()`
   */
  protected def getTouchedFile(
    basePath: Path,
    filePath: String,
    nameToAddFileMap: Map[String, AddFile]): AddFile = {
    val absolutePath = DeltaFileOperations.absolutePath(basePath.toUri.toString, filePath).toString
    nameToAddFileMap.getOrElse(absolutePath, {
      throw new IllegalStateException(s"File ($absolutePath) to be rewritten not found " +
        s"among candidate files:\n${nameToAddFileMap.keys.mkString("\n")}")
    })
  }

  /**
   * Make the given In predicate null-free by adding IsNotNull filters on both operands of the In
   * predicate, namely, value and list.
   *
   * Two points to note:
   *
   * 1. The semantics of In is changed after being turned null-free. Specifically,
   *    all the null results produced by the original In predicate will now become false.
   *
   * 2. If we know that neither value or list can have nulls, then it still produces the
   *    same result after being turned null-free.
   *
   * @param value left-hand operand of In, e.g., value in (list)
   * @param target the child logical plan of value
   * @param list right-hand operand of In, e.g., value in (list)
   */
  private def makeNullFree(
    value: Expression, target: LogicalPlan, list: ListQuery): InSubquery = {
    val references = list.childOutputs.flatMap(_.references.toIterator) :+ value
    val notNulls = ExpressionSet(references.map(IsNotNull))
    val newListQuery = list.copy(children = list.children ++ notNulls)
    //    assert(SubExprUtils.isInPredicateNullFree(Seq(value), newListQuery, target),
    //      s"Unable to make the list query null-free: ${list.treeString}")

    InSubquery(Seq(value), newListQuery)
  }

  /**
   * Return an condition expression that is equivalent to `Not(condition) or IsNull(condition)`.
   *
   * Internally, this can be expressed with `Not(EqualNullSafe(condition, TrueLiteral))` which
   * automatically eliminates the common subexpression. However, subqueries require a slight
   * preprocessing.
   *
   * Given a condition C, we are not allowed to evaluate Not(EqualNullSafe(C, True)) when C contains
   * In/NotIn subquery predicates because SparkSql doesn't have native null-aware joins. But we can
   * do so when we know that the the output of In predicates doesn't have nulls, i.e., null-free.
   * So we transform the condition from C to C' using two transformation rules so that:
   * - All In predicates in C' are null-free, and
   * - EqualNullSafe(C', True) === EqualNullSafe(C, True)
   * Finally we return Not(EqualNullSafe(C', True)).
   *
   * Note that this method can be expensive when the condition has a NotIn as Rule 1 involves
   * actual data processing.
   *
   * @param spark spark session
   * @param condition the input condition
   * @param target the target logical plan
   * @param deltaLog the target deltalog for metrics-logging purposes
   * @param operation the operation name for metrics-logging purposes
   */
  protected def notOrIsNull(
    spark: SparkSession,
    condition: Expression,
    target: LogicalPlan,
    deltaLog: DeltaLog,
    operation: String): Expression = {
    // The downward direction is important, because Rule 1 needs to be applied before Rule 2.
    val newCond = condition transformDown {
      // Rule 1 - If condition has a NotIn predicate, we evaluate the ListQuery and see if
      //          it contains any nulls
      //            - if it does, then this NotIn expression can only be evaluated into
      //              null or false, both of which will become false after applying
      //              EqualNullSafe(condition, True), so we just turn it into a false.
      //            - if it doesn't, then the only case that can make NotIn evaluate to null is when
      //              value is null. As null will become false after
      //              EqualNullSafe(condition, True), we safely turn this NotIn into
      //              And(IsNotNull(value), makeNullFree(NotIn)).
      //                - when value is null, this expression is just false.
      //                - when value is not null, this expression is just makeNullFree(NotIn), which
      //                  is equivalent to the original NotIn, because we already know that neither
      //                  value or list is null.
      case Not(InSubquery(Seq(value), list @ ListQuery(plan, _, _, output)))
        if (!SubExprUtils.isInPredicateNullFree(Seq(value), list, target)) =>
        val data = Dataset.ofRows(spark, plan)
        assert(output.size == 1, "The output of a ListQuery should have exactly 1 column.")
        val count = recordDeltaOperation(
          deltaLog,
          s"delta.dml.$operation.checkNullsForNotIn") {
          data.filter(new Column(IsNull(output.head))).count()
        }
        if (count > 0) {
          // ListQuery contains null, safe to convert it to false
          Literal(false, BooleanType)
        } else {
          val nullFreeIn = makeNullFree(value, target, list)
          And(Not(nullFreeIn), IsNotNull(value))
        }
      // Rule 2 - If the condition has a In predicate, this rule makes it null-free. This
      //          effectively turns all the null results of In into false. This is OK as long as
      //          there is at most one Not outside of this In predicate. Right now Spark SQL
      //          analyzer blocks expression with a In inside of a Not, except for NotIn.
      //          If this In is part of NotIn, it won't trigger this rule because this In would
      //          have been already turned null-free by Rule 1.
      case InSubquery(Seq(value), list @ ListQuery(plan, conditions, exprId, output))
        if (!SubExprUtils.isInPredicateNullFree(Seq(value), list, target)) =>
        makeNullFree(value, target, list)
    }
    Not(EqualNullSafe(newCond, Literal(true, BooleanType)))
  }
}


object SubExprUtils {
  /**
   * A InSubquery predicate is null-free if neither of the two operands
   * can be null, which in turns means this predicate can't evaluate to null.
   *
   * Return true if neither values or list contains nulls.
   */
  def isInPredicateNullFree(values: Seq[Expression], l: ListQuery, target: LogicalPlan): Boolean = {
    val outputNotNulls = ExpressionSet((l.plan.output ++ values).map(IsNotNull))
    // Look for IsNotNulls in either the constraints or the conditions
    outputNotNulls.subsetOf(l.plan.constraints ++ l.children ++ target.constraints)
  }
}

/**
 * Some utility methods on files, directories, and paths.
 */
object DeltaFileOperations {
  /**
   * Create an absolute path from `child` using the `basePath` if the child is a relative path.
   * Return `child` if it is an absolute path.
   *
   * @param basePath Base path to prepend to `child` if child is a relative path.
   *                 Note: It is assumed that the basePath do not have any escaped characters and
   *                 is directly readable by Hadoop APIs.
   * @param child    Child path to append to `basePath` if child is a relative path.
   *                 Note: t is assumed that the child is escaped, that is, all special chars that
   *                 need escaping by URI standards are already escaped.
   * @return Absolute path without escaped chars that is directly readable by Hadoop APIs.
   */
  def absolutePath(basePath: String, child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      p
    } else {
      val merged = new Path(basePath, p)
      // URI resolution strips the final `/` in `p` if it exists
      val mergedUri = merged.toUri.toString
      if (child.endsWith("/") && !mergedUri.endsWith("/")) {
        new Path(new URI(mergedUri + "/"))
      } else {
        merged
      }
    }
  }
}



