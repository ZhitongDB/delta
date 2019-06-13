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

import java.net.URI

import io.delta.DeltaTable
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualNullSafe, Expression, ExpressionSet, InputFileName, InSubquery, IsNotNull, IsNull, ListQuery, Literal, Not, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{Action, AddFile, RemoveFile}
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeFileIndex}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BooleanType

trait DeltaTableOperations { self: DeltaTable =>

  def delete(condition: String): Unit = {
    delete(functions.expr(condition))
  }

  def delete(condition: Column): Unit = {
    val sparkSession = SparkSession.getActiveSession.getOrElse(self.toDF.sparkSession)
    val delete: LogicalPlan = Delete(self.toDF.queryExecution.analyzed, Some(condition.expr))
    val analyzer = sparkSession.sessionState.analyzer
    val resolvedDelete = analyzer.execute(delete)
    val deleteCommand =
      PreprocessTableDelete(sparkSession.sessionState.conf)(resolvedDelete)
        .asInstanceOf[RunnableCommand]

    deleteCommand.run(sparkSession)
  }

  def delete(): Unit = {
    val sparkSession = SparkSession.getActiveSession.getOrElse(self.toDF.sparkSession)
    val delete: LogicalPlan = Delete(self.toDF.queryExecution.analyzed, None)
    val analyzer = sparkSession.sessionState.analyzer
    val resolvedDelete = analyzer.execute(delete)
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
//    if (SparkClientContext.clientEnabled) {
//      plan  // defer delta analysis to server side
//    } else {
      apply0(plan)
//    }
  }

  def apply0(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case Delete(child, condition) =>
      val index = EliminateSubqueryAliases(child) match {
        case DeltaFullTable(tahoeFileIndex) =>
          tahoeFileIndex
        case o =>
          throw DeltaErrors.notADeltaSourceException("DELETE", Some(o))
      }

//      condition match {
//        case Some(cond) if !conf.getConf(DatabricksSQLConf.DELTA_COMMAND_SUBQUERY_ENABLED) &&
//          SubqueryExpression.hasSubquery(cond) =>
//          throw DeltaErrors.subqueryNotSupportedException("DELETE condition", cond)
//
//        case Some(cond) if SubqueryExpression.hasMultiColumnInPredicate(cond) =>
//          throw DeltaErrors.multiColumnInPredicateNotSupportedException("DELETE")
//
//        case Some(cond) if SubqueryExpression.hasNestedSubquery(cond) =>
//          throw DeltaErrors.nestedSubqueryNotSupportedException("DELETE")
//
//        case Some(cond)
//          if !conf.getConf(DatabricksSQLConf.ALLOW_NULL_FREE_IN_PREDICATE_WITHIN_NOT) &&
//            SubqueryExpression.hasInSubquery(cond) =>
//        // In subqueries can only be supported when ALLOW_NULL_FREE_IN_PREDICATE_WITHIN_NOT is on
//          throw DeltaErrors.inSubqueryNotSupportedException("DELETE")
//
//        case _ =>
//      }

      DeleteCommand(index, child, condition)
  }
}

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
  extends RunnableCommand with DeltaEdgeCommand {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(target)

  // At this point we should have already passed the ACL checks, so it is safe to whitelist.
//  CheckPermissions.trusted
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
              } else if (SubqueryExpression.hasSubquery(cond)) {
                // Obtain the file name before filter() because filter() may incur a join when
                // the filter condition contains a subquery and we can no longer get the input
                // file names on the join output.
                data
                  .withColumn(DeleteCommand.FILE_NAME_COLUMN, new Column(InputFileName()))
                  .filter(new Column(cond))
                  .select(DeleteCommand.FILE_NAME_COLUMN)
                  .distinct()
                  .as[String]
                  .collect()
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
            val filterCond = notOrIsNull(sparkSession, cond, target, txn.deltaLog, "delete")
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
        if (!SubExpU.isInPredicateNullFree(Seq(value), list, target)) =>
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
        if (!SubExpU.isInPredicateNullFree(Seq(value), list, target)) =>
        makeNullFree(value, target, list)
    }
    Not(EqualNullSafe(newCond, Literal(true, BooleanType)))
  }
}

object SubExpU {
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

  /**
   * Given a path `child`:
   *   1. Returns `child` if the path is already relative
   *   2. Tries relativizing `child` with respect to `basePath`
   *      a) If the `child` doesn't live within the same base path, returns `child` as is
   *      b) If `child` lives in a different FileSystem, throws an exception
   * Note that `child` may physically be pointing to a path within `basePath`, but may logically
   * belong to a different FileSystem, e.g. DBFS mount points and direct S3 paths.
   */
  def tryRelativizePath(fs: FileSystem, basePath: Path, child: Path): Path = {
    // Child Paths can be absolute and use a separate fs
    val childUri = child.toUri
    // We can map multiple schemes to the same `FileSystem` class, but `FileSystem.getScheme` is
    // usually just a hard-coded string. Hence, we need to use the scheme of the URI that we use to
    // create the FileSystem here.
    if (child.isAbsolute) {
      try {
        new Path(fs.makeQualified(basePath).toUri.relativize(fs.makeQualified(child).toUri))
      } catch {
        case e: IllegalArgumentException =>
          throw new IllegalStateException(
            s"""Failed to relativize the path ($child). This can happen when absolute paths make
               |it into the transaction log, which start with the scheme s3://, wasbs:// or adls://.
               |This is a bug that has existed before DBR 5.0. To fix this issue, please upgrade
               |your writer jobs to DBR 5.0 and please run:
               |%scala com.databricks.delta.Delta.fixAbsolutePathsInLog("$child")
             """.stripMargin)
      }
    } else {
      child
    }
  }

  def generateCandidateFileMap(
    basePath: Path,
    candidateFiles: Seq[AddFile]): Map[String, AddFile] = {
    val nameToAddFileMap = candidateFiles.map(add =>
      DeltaFileOperations.absolutePath(basePath.toString, add.path).toString -> add).toMap
    assert(nameToAddFileMap.size == candidateFiles.length,
      s"File name collisions found among:\n${candidateFiles.map(_.path).mkString("\n")}")
    nameToAddFileMap
  }
}

object Dataset {
  def apply[T: Encoder](sparkSession: SparkSession, logicalPlan: LogicalPlan): Dataset[T] = {
    val dataset = new Dataset(sparkSession, logicalPlan, implicitly[Encoder[T]])
    // Eagerly bind the encoder so we verify that the encoder matches the underlying
    // schema. The user will get an error if this is not the case.
    // optimization: it is guaranteed that [[InternalRow]] can be converted to [[Row]] so
    // do not do this check in that case. this check can be expensive since it requires running
    // the whole [[Analyzer]] to resolve the deserializer
//    if (dataset.exprEnc.clsTag.runtimeClass != classOf[Row]) {
//      dataset.deserializer
//    }
    dataset
  }

  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    val qe = sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    new Dataset[Row](sparkSession, qe, RowEncoder(qe.analyzed.schema))
  }
}
