package cn.ysw.spark.sources

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import java.util.Optional

class ClickHouseDataSourceV2 extends DataSourceV2 with DataSourceRegister with ReadSupport with WriteSupport with StreamWriteSupport {
  /** 声明ClickHouse数据源的简称，使用方式为spark.read.format("clickhouse")... */
  override def shortName(): String = "clickhouse"

  /** 批处理方式下的数据读取 */
  override def createReader(options: DataSourceOptions): DataSourceReader = new CKReader(new CKOptions(options.asMap()))

  /** 批处理方式下的数据写入 */
  override def createWriter(writeUUID: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = Optional.of(new CKWriter(writeUUID, schema, mode, null, new CKOptions(options.asMap())))

  /** 流处理方式下的数据写入 */
  override def createStreamWriter(queryId: String, schema: StructType, mode: OutputMode, options: DataSourceOptions): StreamWriter = new CKWriter(queryId, schema, null, mode, new CKOptions(options.asMap()))
}
