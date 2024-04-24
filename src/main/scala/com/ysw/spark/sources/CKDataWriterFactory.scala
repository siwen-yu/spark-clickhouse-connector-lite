package com.ysw.spark.sources

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/**
 * 写数据工厂，用来实例化CKDataWriter
 */
class CKDataWriterFactory(writeUUID: String, schema: StructType, batchMode: SaveMode, streamMode: OutputMode, options: CKOptions) extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = new CKDataWriter(writeUUID, schema, batchMode, streamMode, options)
}
