package cn.ysw.spark.sources

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/**
 * 支持Batch和Stream的数据写实现
 */
class CKWriter(writeUuidOrQueryId: String, schema: StructType, batchMode: SaveMode, streamMode: OutputMode, options: CKOptions) extends StreamWriter {
  private val isStreamMode:Boolean = if (null!=streamMode&&null==batchMode) true else false
  override def useCommitCoordinator(): Boolean = true
  override def onDataWriterCommit(message: WriterCommitMessage): Unit = {}
  override def createWriterFactory(): DataWriterFactory[InternalRow] = new CKDataWriterFactory(writeUuidOrQueryId, schema, batchMode, streamMode, options)
  /** Batch writer commit */
  override def commit(messages: Array[WriterCommitMessage]): Unit = {}
  /** Batch writer abort */
  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
  /** Streaming writer commit */
  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
  /** Streaming writer abort */
  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
}
