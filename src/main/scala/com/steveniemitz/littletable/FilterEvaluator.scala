package com.steveniemitz.littletable

import com.google.bigtable.v2.{ColumnRange, RowFilter, ValueRange}
import com.google.protobuf.ByteString
import java.nio.charset.StandardCharsets
import java.util.concurrent.ThreadLocalRandom
import vendored.com.google.re2j.RE2
import scala.collection.JavaConverters._
import scala.util.control.ControlThrowable

private object FilterEvaluator {
  private def pass(noop: RowFilter): Row => Row = identity[Row]
  private def drop(noop: RowFilter): Row => Row = row => ResultRow.empty(row.key)
  private final val dropF = drop(RowFilter.getDefaultInstance)
  private final val passF = pass(RowFilter.getDefaultInstance)

  private final case class SinkReturn(row: Row) extends Throwable with ControlThrowable {
    override def fillInStackTrace(): Throwable = this
  }

  private def prepare(filter: RowFilter): Row => Row = {
    val preparer: RowFilter => Row => Row = filter.getFilterCase match {
      case RowFilter.FilterCase.APPLY_LABEL_TRANSFORMER => prepareApplyLabelTransformer
      case RowFilter.FilterCase.BLOCK_ALL_FILTER => drop
      case RowFilter.FilterCase.CHAIN => prepareRowChain
      case RowFilter.FilterCase.CELLS_PER_COLUMN_LIMIT_FILTER => preparePerColumnLimitFilter
      case RowFilter.FilterCase.CELLS_PER_ROW_LIMIT_FILTER => prepareCellsPerRowLimitFilter
      case RowFilter.FilterCase.CELLS_PER_ROW_OFFSET_FILTER => prepareCellsPerRowOffsetFilter
      case RowFilter.FilterCase.COLUMN_QUALIFIER_REGEX_FILTER => prepareColumnQualifierRegexFilter
      case RowFilter.FilterCase.COLUMN_RANGE_FILTER => prepareColumnRangeFilter
      case RowFilter.FilterCase.CONDITION => prepareCondition
      case RowFilter.FilterCase.FAMILY_NAME_REGEX_FILTER => prepareFamilyNameRegexFilter
      case RowFilter.FilterCase.FILTER_NOT_SET => pass
      case RowFilter.FilterCase.INTERLEAVE => prepareRowInterleave
      case RowFilter.FilterCase.PASS_ALL_FILTER => pass
      case RowFilter.FilterCase.ROW_KEY_REGEX_FILTER => prepareRowKeyRegexFilter
      case RowFilter.FilterCase.ROW_SAMPLE_FILTER => prepareRowSampleFilter
      case RowFilter.FilterCase.SINK => prepareSink
      case RowFilter.FilterCase.STRIP_VALUE_TRANSFORMER => prepareStripValueTransformer
      case RowFilter.FilterCase.TIMESTAMP_RANGE_FILTER => prepareTimestampRangeFilter
      case RowFilter.FilterCase.VALUE_RANGE_FILTER => prepareValueRangeFilter
      case RowFilter.FilterCase.VALUE_REGEX_FILTER => prepareValueRegexFilter
    }
    preparer(filter)
  }

  def evaluate(rows: Iterator[Row], filter: RowFilter): Iterator[Row] = {
    val evalFn = prepare(filter)
    rows.flatMap { r => evaluate(r, evalFn) }
  }

  private def evaluate(row: Row, evalFn: Row => Row): Option[Row] = {
    row.transact { r =>
      val newRow =
        try evalFn(r)
        catch {
          case SinkReturn(returnRow) => returnRow
        }

      newRow.transact { newRow =>
        if (!newRow.hasCells) None
        else Some(newRow)
      }
    }
  }

  private def simpleCellFilter(cellFilter: Cell => Boolean): Row => Row = { row =>
    {
      val newCells = row.transact {
        _.cells.filter(cellFilter)
      }
      ResultRow.create(row.key, newCells)
    }
  }

  private def prepareRowKeyRegexFilter(filter: RowFilter): Row => Row = {
    val regex = filter.getRowKeyRegexFilter
    val re2 = RE2.compileBinary(regex.toByteArray)

    row =>
      if (re2.matchBinary(row.key.toByteArray)) row
      else ResultRow.empty(row.key)
  }

  private def prepareRowChain(filter: RowFilter): Row => Row = {
    val chain = filter.getChain
    val filterFns = chain.getFiltersList.asScala.map(prepare)

    row => filterFns.foldLeft(row) { (r, fn) => fn(r) }
  }

  private def prepareRowInterleave(filter: RowFilter): Row => Row = {
    val interleave = filter.getInterleave
    val filterFns = interleave.getFiltersList.asScala.map(prepare)

    row => {
      val cellBuffer = row.transact(_.cells.toBuffer)
      val reiterableRow = ResultRow.reiterable(row.key, cellBuffer)

      val newRows = filterFns.map(_.apply(reiterableRow))
      val newCells = newRows.iterator.flatMap { r => r.transact { _.cells } }

      ResultRow.createFromUnsortedCells(row.key, newCells)
    }
  }

  private def prepareColumnRangeFilter(filter: RowFilter): Row => Row = {
    val cr = filter.getColumnRangeFilter
    val familyMatch = cr.getFamilyName
    val (startCol, startColInclusive) = cr.getStartQualifierCase match {
      case ColumnRange.StartQualifierCase.START_QUALIFIER_CLOSED =>
        Some(cr.getStartQualifierClosed) -> true
      case ColumnRange.StartQualifierCase.START_QUALIFIER_OPEN =>
        Some(cr.getStartQualifierOpen) -> false
      case ColumnRange.StartQualifierCase.STARTQUALIFIER_NOT_SET => None -> true
    }
    val (endCol, endColInclusive) = cr.getEndQualifierCase match {
      case ColumnRange.EndQualifierCase.END_QUALIFIER_CLOSED =>
        Some(cr.getEndQualifierClosed) -> true
      case ColumnRange.EndQualifierCase.END_QUALIFIER_OPEN => Some(cr.getEndQualifierOpen) -> false
      case ColumnRange.EndQualifierCase.ENDQUALIFIER_NOT_SET => None -> true
    }

    val startCmp = ByteStringComparator.isAfterComparer(startCol, startColInclusive)
    val endCmp = ByteStringComparator.isBeforeComparer(endCol, endColInclusive)

    simpleCellFilter { c =>
      c.columnFamily == familyMatch &&
      startCmp(c.columnQualifier) &&
      endCmp(c.columnQualifier)
    }
  }

  private def prepareCondition(filter: RowFilter): Row => Row = {
    val cond = filter.getCondition
    val preparedPredicate = prepare(cond.getPredicateFilter)
    val preparedTrue =
      if (cond.getTrueFilter.getFilterCase == RowFilter.FilterCase.FILTER_NOT_SET) dropF
      else prepare(cond.getTrueFilter)

    val preparedFalse =
      if (cond.getFalseFilter.getFilterCase == RowFilter.FilterCase.FILTER_NOT_SET) dropF
      else prepare(cond.getFalseFilter)

    row => {
      row.transact { row =>
        val predicateResult = evaluate(row, preparedPredicate)
        if (predicateResult.isDefined) {
          evaluate(row, preparedTrue).getOrElse(ResultRow.empty(row.key))
        } else {
          evaluate(row, preparedFalse).getOrElse(ResultRow.empty(row.key))
        }
      }
    }
  }

  private def prepareValueRangeFilter(filter: RowFilter): Row => Row = {
    val vr = filter.getValueRangeFilter
    val (startCol, startColInclusive) = vr.getStartValueCase match {
      case ValueRange.StartValueCase.START_VALUE_CLOSED => Some(vr.getStartValueClosed) -> true
      case ValueRange.StartValueCase.START_VALUE_OPEN => Some(vr.getStartValueOpen) -> false
      case ValueRange.StartValueCase.STARTVALUE_NOT_SET => None -> true
    }
    val (endCol, endColInclusive) = vr.getEndValueCase match {
      case ValueRange.EndValueCase.END_VALUE_CLOSED => Some(vr.getEndValueClosed) -> true
      case ValueRange.EndValueCase.END_VALUE_OPEN => Some(vr.getEndValueOpen) -> false
      case ValueRange.EndValueCase.ENDVALUE_NOT_SET => None -> true
    }

    val startCmp = ByteStringComparator.isAfterComparer(startCol, startColInclusive)
    val endCmp = ByteStringComparator.isBeforeComparer(endCol, endColInclusive)

    simpleCellFilter(c => startCmp(c.value) && endCmp(c.value))
  }

  private def prepareStripValueTransformer(filter: RowFilter): Row => Row =
    row => {
      val newCells = row.transact {
        _.cells.map { c =>
          val newCell = c.copy()
          newCell.value = ByteString.EMPTY
          newCell
        }
      }
      ResultRow.create(row.key, newCells)
    }

  private def prepareTimestampRangeFilter(filter: RowFilter): Row => Row = {
    val range = filter.getTimestampRangeFilter
    val startMicros = range.getStartTimestampMicros
    val endMicros = range.getEndTimestampMicros

    simpleCellFilter { c =>
      (startMicros == 0 || c.timestamp >= startMicros) &&
      (endMicros == 0 || c.timestamp < endMicros)
    }
  }

  private def preparePerColumnLimitFilter(filter: RowFilter): Row => Row = {
    val limit = filter.getCellsPerColumnLimitFilter

    row => {
      val newCells = row.transact { r =>
        GroupByLimitIterator(r.cells, limit) { (a, b) =>
          a.columnQualifier == b.columnQualifier && a.columnFamily == b.columnFamily
        }
      }

      ResultRow.create(row.key, newCells.flatten)
    }
  }

  private def prepareCellsPerRowLimitFilter(filter: RowFilter): Row => Row = {
    val limit = filter.getCellsPerRowLimitFilter

    row => {
      val newCells = row.transact {
        _.cells.take(limit)
      }
      ResultRow.create(row.key, newCells)
    }
  }

  private def prepareCellsPerRowOffsetFilter(filter: RowFilter): Row => Row = {
    val offset = filter.getCellsPerRowOffsetFilter

    row => {
      val newCells = row.transact {
        _.cells.drop(offset)
      }
      ResultRow.create(row.key, newCells)
    }
  }

  private def prepareRowSampleFilter(filter: RowFilter): Row => Row = {
    val probability = filter.getRowSampleFilter

    row => {
      if (ThreadLocalRandom.current().nextDouble() < probability) row
      else ResultRow.empty(row.key)
    }
  }

  private def prepareColumnQualifierRegexFilter(filter: RowFilter): Row => Row = {
    val regex = RE2.compileBinary(filter.getColumnQualifierRegexFilter.toByteArray)

    simpleCellFilter { c => regex.matchBinary(c.columnQualifier.toByteArray) }
  }

  private def prepareFamilyNameRegexFilter(filter: RowFilter): Row => Row = {
    val regex = RE2.compileBinary(filter.getFamilyNameRegexFilterBytes.toByteArray)

    simpleCellFilter { c =>
      regex.matchBinary(c.columnFamily.getBytes(StandardCharsets.ISO_8859_1))
    }
  }

  private def prepareValueRegexFilter(filter: RowFilter): Row => Row = {
    val regex = RE2.compileBinary(filter.getValueRegexFilter.toByteArray)

    simpleCellFilter { c => regex.matchBinary(c.value.toByteArray) }
  }

  private def prepareApplyLabelTransformer(filter: RowFilter): Row => Row = {
    val label = filter.getApplyLabelTransformer

    row => {
      val newCells = row.transact { r =>
        r.cells.map { c =>
          val newCell = c.copy(labels = c.labels + label)
          newCell.value = c.value
          newCell
        }
      }
      ResultRow.create(row.key, newCells)
    }
  }

  private def prepareSink(filter: RowFilter): Row => Row = row => throw SinkReturn(row)
}
