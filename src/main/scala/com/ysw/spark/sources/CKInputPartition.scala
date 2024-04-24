package com.ysw.spark.sources

import com.clickhouse.client.ClickHouseNode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType

/**
 * 基于批处理方式的ClickHouse分区实现
 */
class CKInputPartition(node: ClickHouseNode, schema: StructType, options: CKOptions) extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] = new CKInputPartitionReader(node, schema, options)
}
