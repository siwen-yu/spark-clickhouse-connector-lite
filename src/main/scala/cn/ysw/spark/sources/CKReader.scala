package cn.ysw.spark.sources

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.util

class CKReader(options: CKOptions) extends DataSourceReader {
  final val log = LoggerFactory.getLogger(classOf[CKReader])
  //with SupportsPushDownRequiredColumns with SupportsPushDownFilters {
  private val customSchema: java.lang.String = options.getCustomSchema
  private val helper = new CKHelper(options)

  import scala.collection.JavaConverters._

  private val schema = if (StringUtils.isEmpty(customSchema)) {
    helper.getSparkTableSchema()
  } else {
    helper.getSparkTableSchema(new util.LinkedList[String](customSchema.split(",").toList.asJava))
  }

  override def readSchema(): StructType = schema

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    import scala.collection.JavaConverters._
    log.info("clickhouse available nodes：" + helper.nodes.length)
    helper.nodes.map(new CKInputPartition(_, schema, options)).toList.asJava.asInstanceOf[util.List[InputPartition[InternalRow]]]
  }
}
