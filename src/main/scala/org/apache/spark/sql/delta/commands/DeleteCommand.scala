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

import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, DeltaTableUtils, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeFileIndex}

import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{EqualNullSafe, Expression, InputFileName, Literal, Not}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.types.BooleanType

/**
 * Performs a Delete based on the search condition
 *
 * Algorithm:
 *   1) Scan all the files (after column pruning and filter pushdown) and determine which files have
 *      the rows that needs to be deleted.
 *   2) Traverse the affected files (after column pruning) and rebuild the touched files.
 *   3) Use the Tahoe protocol to atomically write the remaining rows to new files and remove
 *      the affected files that are identified in step 1.
 */
case class DeleteCommand(
    tahoeFileIndex: TahoeFileIndex,
    target: LogicalPlan,
    condition: Option[Expression])
    extends RunnableCommand with DeltaCommand {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(target)

  // At this point we should have already passed the ACL checks, so it is safe to whitelist.
  final override def run(sparkSession: SparkSession): Seq[Row] = {
    recordDeltaOperation(tahoeFileIndex.deltaLog, "delta.dml.delete") {
      val deltaLog = tahoeFileIndex.deltaLog
      deltaLog.assertRemovable()
      deltaLog.withNewTransaction { txn =>
        performDelete(sparkSession, deltaLog, txn)
      }
      // Re-cache all cached plans(including this relation itself, if it's cached) that refer to
      // this data source relation.
      sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, target)
    }

    Seq.empty[Row]
  }

  private def performDelete(
      sparkSession: SparkSession, deltaLog: DeltaLog, txn: OptimisticTransaction) = {
    import sparkSession.implicits._

    var numTouchedFiles: Long = 0
    var numRewrittenFiles: Long = 0
    var scanTimeMs: Long = 0
    var rewriteTimeMs: Long = 0

    val startTime = System.nanoTime()
    val numFilesTotal = deltaLog.snapshot.numOfFiles

    val deleteActions: Seq[Action] = condition match {
      case None =>
        // Case 1: Delete the whole table if the condition is true
        val allFiles = txn.filterFiles(Nil)

        numTouchedFiles = allFiles.size
        scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000

        val operationTimestamp = System.currentTimeMillis()
        allFiles.map(_.removeWithTimestamp(operationTimestamp))
      case Some(cond) =>
        val (metadataPredicates, otherPredicates) =
          DeltaTableUtils.splitMetadataAndDataPredicates(
            cond, txn.metadata.partitionColumns, sparkSession)

        if (otherPredicates.isEmpty) {
          // Case 2: The condition can be evaluated using metadata only.
          //         Delete a set of files without the need of scanning any data files.
          val operationTimestamp = System.currentTimeMillis()
          val candidateFiles = txn.filterFiles(metadataPredicates)

          scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000
          numTouchedFiles = candidateFiles.size

          candidateFiles.map(_.removeWithTimestamp(operationTimestamp))
        } else {
          // Case 3: Delete the rows based on the condition.
          val candidateFiles = txn.filterFiles(metadataPredicates ++ otherPredicates)

          numTouchedFiles = candidateFiles.size
          val nameToAddFileMap = generateCandidateFileMap(deltaLog.dataPath, candidateFiles)

          val fileIndex = new TahoeBatchFileIndex(
            sparkSession, "delete", candidateFiles, deltaLog, tahoeFileIndex.path, txn.snapshot)
          // Keep everything from the resolved target except a new TahoeFileIndex
          // that only involves the affected files instead of all files.
          val newTarget = DeltaTableUtils.replaceFileIndex(target, fileIndex)
          val data = Dataset.ofRows(sparkSession, newTarget)
          val filesToRewrite =
            withStatusCode("DELTA", s"Finding files to rewrite for DELETE operation") {
              if (numTouchedFiles == 0) {
                Array.empty[String]
              } else {
                data.filter(new Column(cond)).select(new Column(InputFileName())).distinct()
                  .as[String].collect()
              }
            }

          scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000
          if (filesToRewrite.isEmpty) {
            // Case 3.1: no row matches and no delete will be triggered
            Nil
          } else {
            // Case 3.2: some files need an update to remove the deleted files
            // Do the second pass and just read the affected files
            val baseRelation = buildBaseRelation(
              sparkSession, txn, "delete", tahoeFileIndex.path, filesToRewrite, nameToAddFileMap)
            // Keep everything from the resolved target except a new TahoeFileIndex
            // that only involves the affected files instead of all files.
            val newTarget = DeltaTableUtils.replaceFileIndex(target, baseRelation.location)

            val targetDF = Dataset.ofRows(sparkSession, newTarget)
            val filterCond = Not(EqualNullSafe(cond, Literal(true, BooleanType)))
            val updatedDF = targetDF.filter(new Column(filterCond))

            val rewrittenFiles = withStatusCode(
              "DELTA", s"Rewriting ${filesToRewrite.size} files for DELETE operation") {
              txn.writeFiles(updatedDF)
            }

            numRewrittenFiles = rewrittenFiles.size
            rewriteTimeMs = (System.nanoTime() - startTime) / 1000 / 1000 - scanTimeMs

            val operationTimestamp = System.currentTimeMillis()
            removeFilesFromPaths(deltaLog, nameToAddFileMap, filesToRewrite, operationTimestamp) ++
              rewrittenFiles
          }
        }
    }
    if (deleteActions.nonEmpty) {
      txn.commit(deleteActions, DeltaOperations.Delete(condition.map(_.sql).toSeq))
    }

    recordDeltaEvent(
      deltaLog,
      "delta.dml.delete.stats",
      data = DeleteMetric(
        condition = condition.map(_.sql).getOrElse("true"),
        numFilesTotal,
        numTouchedFiles,
        numRewrittenFiles,
        scanTimeMs,
        rewriteTimeMs)
    )
  }

  /**
   * Used to report details about delete.
   *
   * @param condition: what was the delete condition
   * @param numFilesTotal: how big is the table
   * @param numTouchedFiles: how many files did we touch
   * @param numRewrittenFiles: how many files had to be rewritten
   * @param scanTimeMs: how long did finding take
   * @param rewriteTimeMs: how long did rewriting take
   *
   * @note All the time units are milliseconds.
   */
  case class DeleteMetric(
      condition: String,
      numFilesTotal: Long,
      numTouchedFiles: Long,
      numRewrittenFiles: Long,
      scanTimeMs: Long,
      rewriteTimeMs: Long)
}

object DeleteCommand {
  val FILE_NAME_COLUMN = "_input_file_name_"
}

