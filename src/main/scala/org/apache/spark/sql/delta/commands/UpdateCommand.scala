package org.apache.spark.sql.delta.commands

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.expressions.{Alias, EqualNullSafe, Expression, If, InputFileName, Literal, Not, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeFileIndex}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, DeltaTableUtils, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.{Action, AddFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BooleanType

/**
 * Performs an Update using UPDATE EXPRESSION based on the search condition
 *
 * Algorithm:
 *   1) Scan all the files (after column pruning) and determine which files have
 *      the rows that needs to be updated.
 *   2) Traverse the affected files (after column pruning) and rebuild the touched files.
 *   3) Use the Tahoe protocol to atomically write the remaining rows to new files and remove
 *      the affected files that are identified in step 1.
 *
 * TODO: If the condition only contains partition predicates and the updated expressions
 *       DO NOT change the original value of data columns, we just need to copy the files,
 *       instead of re-generating the files.
 */
case class UpdateCommand(
  tahoeFileIndex: TahoeFileIndex,
  target: LogicalPlan,
  updateExpressions: Seq[Expression],
  condition: Option[Expression])
  extends RunnableCommand with DeltaCommand {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(target)

  final override def run(sparkSession: SparkSession): Seq[Row] = {
    recordDeltaOperation(tahoeFileIndex.deltaLog, "delta.dml.update") {
      val deltaLog = tahoeFileIndex.deltaLog
      deltaLog.assertRemovable()
      deltaLog.withNewTransaction { txn =>
        performUpdate(sparkSession, deltaLog, txn)
      }
      // Re-cache all cached plans(including this relation itself, if it's cached) that refer to
      // this data source relation.
      sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, target)
    }
    Seq.empty[Row]
  }

  private def performUpdate(
    sparkSession: SparkSession, deltaLog: DeltaLog, txn: OptimisticTransaction): Unit = {
    import sparkSession.implicits._

    var numTouchedFiles: Long = 0
    var numRewrittenFiles: Long = 0
    var scanTimeMs: Long = 0
    var rewriteTimeMs: Long = 0

    val startTime = System.nanoTime()
    val numFilesTotal = deltaLog.snapshot.numOfFiles

    val updateCondition = condition.getOrElse(Literal(true, BooleanType))
    val (metadataPredicates, dataPredicates) =
      DeltaTableUtils.splitMetadataAndDataPredicates(
        updateCondition, txn.metadata.partitionColumns, sparkSession)
    val candidateFiles = txn.filterFiles(metadataPredicates ++ dataPredicates)
    val nameToAddFile = generateCandidateFileMap(deltaLog.dataPath, candidateFiles)

    scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000

    val actions: Seq[Action] = if (candidateFiles.isEmpty) {
      // Case 1: Do nothing if no row qualifies the partition predicates
      // that are part of Update condition
      Nil
    } else if (dataPredicates.isEmpty) {
      // Case 2: Update all the rows from the files that are in the specified partitions
      // when the data filter is empty
      numTouchedFiles = candidateFiles.length

      val filesToRewrite = candidateFiles.map(_.path)
      val operationTimestamp = System.currentTimeMillis()
      val deleteActions = candidateFiles.map(_.removeWithTimestamp(operationTimestamp))

      val rewrittenFiles = rewriteFiles(sparkSession, txn, tahoeFileIndex.path,
        filesToRewrite, nameToAddFile, updateCondition)

      numRewrittenFiles = rewrittenFiles.size
      rewriteTimeMs = (System.nanoTime() - startTime) / 1000 / 1000 - scanTimeMs

      deleteActions ++ rewrittenFiles
    } else {
      // Case 3: Find all the affected files using the user-specified condition
      val fileIndex = new TahoeBatchFileIndex(
        sparkSession, "update", candidateFiles, deltaLog, tahoeFileIndex.path, txn.snapshot)
      // Keep everything from the resolved target except a new TahoeFileIndex
      // that only involves the affected files instead of all files.
      val newTarget = DeltaTableUtils.replaceFileIndex(target, fileIndex)
      val data = Dataset.ofRows(sparkSession, newTarget)
      val filesToRewrite =
        withStatusCode("DELTA", s"Finding files to rewrite for UPDATE operation") {
          data.filter(new Column(updateCondition)).select(new Column(InputFileName()))
            .distinct().as[String].collect()
        }

      scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000
      numTouchedFiles = filesToRewrite.length

      if (filesToRewrite.isEmpty) {
        // Case 3.1: Do nothing if no row qualifies the UPDATE condition
        Nil
      } else {
        // Case 3.2: Delete the old files and generate the new files containing the updated
        // values
        val operationTimestamp = System.currentTimeMillis()
        val deleteActions =
          removeFilesFromPaths(deltaLog, nameToAddFile, filesToRewrite, operationTimestamp)
        val rewrittenFiles =
          withStatusCode("DELTA", s"Rewriting ${filesToRewrite.size} files for UPDATE operation") {
            rewriteFiles(sparkSession, txn, tahoeFileIndex.path,
              filesToRewrite, nameToAddFile, updateCondition)
          }

        numRewrittenFiles = rewrittenFiles.size
        rewriteTimeMs = (System.nanoTime() - startTime) / 1000 / 1000 - scanTimeMs

        deleteActions ++ rewrittenFiles
      }
    }

    if (actions.nonEmpty) {
      val maxToStringFields = SQLConf.get.maxToStringFields
      txn.commit(actions, DeltaOperations.Update(condition.map(_.simpleString(maxToStringFields))))
    }

    recordDeltaEvent(
      deltaLog,
      "delta.dml.update.stats",
      data = UpdateMetric(
        condition = condition.map(_.sql).getOrElse("true"),
        numFilesTotal,
        numTouchedFiles,
        numRewrittenFiles,
        scanTimeMs,
        rewriteTimeMs)
    )
  }

  /**
   * Scan all the affected files and write out the updated files
   */
  private def rewriteFiles(
    spark: SparkSession,
    txn: OptimisticTransaction,
    rootPath: Path,
    inputLeafFiles: Seq[String],
    nameToAddFileMap: Map[String, AddFile],
    condition: Expression): Seq[AddFile] = {
    // Containing the map from the relative file path to AddFile
    val baseRelation = buildBaseRelation(
      spark, txn, "update", rootPath, inputLeafFiles, nameToAddFileMap)
    val newTarget = DeltaTableUtils.replaceFileIndex(target, baseRelation.location)
    val targetDf = Dataset.ofRows(spark, newTarget)
    val updatedDataFrame = if (SubqueryExpression.hasInOrExistsSubquery(condition)) {
      // Exists and in subqueries are not allowed in a Projection, so we will have to filter the
      // data twice and get two sets of rows:
      //   1. rows that match the conditions with updated values
      //   2. rows that do not match the conditions with original values
      // Finally we union the two sets of rows to produce the final updated table.
      // Update the rows that match the conditions
      val updatedRows = targetDf.filter(
        new Column(condition)).select(buildUpdatedColumns(Literal(true, BooleanType)): _*)
      // Retain the rows that don't match the condition
      val reversedCondition = Not(EqualNullSafe(condition, Literal(true, BooleanType)))
      val unchangedRows = targetDf.filter(new Column(reversedCondition))
      // Put the two sets of rows together
      updatedRows.union(unchangedRows)
    } else {
      val updatedColumns = buildUpdatedColumns(condition)
      targetDf.select(updatedColumns: _*)
    }

    txn.writeFiles(updatedDataFrame)
  }

  /**
   * Build the new columns. If the condition matches, generate the new value using
   * the corresponding UPDATE EXPRESSION; otherwise, keep the original column value
   */
  private def buildUpdatedColumns(condition: Expression): Seq[Column] = {
    updateExpressions.zip(target.output).map { case (update, original) =>
      val updated = If(condition, update, original)
      new Column(Alias(updated, original.name)())
    }
  }

  /**
   * Used to report details about update.
   *
   * @param condition: what was the update condition
   * @param numFilesTotal: how big is the table
   * @param numTouchedFiles: how many files did we touch
   * @param numRewrittenFiles: how many files had to be rewritten
   * @param scanTimeMs: how long did finding take
   * @param rewriteTimeMs: how long did rewriting take
   *
   * @note All the time units are milliseconds.
   */
  case class UpdateMetric(
    condition: String,
    numFilesTotal: Long,
    numTouchedFiles: Long,
    numRewrittenFiles: Long,
    scanTimeMs: Long,
    rewriteTimeMs: Long)
}

object UpdateCommand {
  val FILE_NAME_COLUMN = "_input_file_name_"
}

