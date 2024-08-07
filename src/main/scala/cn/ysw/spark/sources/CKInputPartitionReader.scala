package cn.ysw.spark.sources

import com.clickhouse.client.ClickHouseNode
import com.clickhouse.jdbc.{ClickHouseConnection, ClickHouseStatement}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory

import java.io.Serializable
import java.sql.{ResultSet, SQLException}

/**
 * 基于批处理方式的ClickHouse分区读取数据实现
 */
class CKInputPartitionReader(node: ClickHouseNode, schema: StructType, options: CKOptions) extends InputPartitionReader[InternalRow] with Serializable {
  final val log = LoggerFactory.getLogger(classOf[CKInputPartitionReader])
  val helper = new CKHelper(options)
  var connection: ClickHouseConnection = _
  var st: ClickHouseStatement = _
  var rs: ResultSet = _

  override def next(): Boolean = {
    if (null == connection || connection.isClosed && null == st || st.isClosed && null == rs || rs.isClosed) {
      try {
        connection = helper.getConnection(node)
        st = connection.createStatement()
        rs = st.executeQuery(helper.getSelectStatement(schema))
      } catch {
        case e: Exception =>
          helper.closeAll(connection, st, null, null)
          if (helper.ignoreErrNode) {
            log.warn(s"节点连接失败：${node.getHost}:${node.getPort}")
          } else {
            throw e
          }
      }
    }
    if (null != rs && !rs.isClosed) rs.next() else false
  }

  override def get(): InternalRow = {
    val fields = schema.fields
    val length = fields.length
    val record = new Array[Any](length)
    for (i <- 0 until length) {
      val field = fields(i)
      val name = field.name
      val dataType = field.dataType
      try {
        dataType match {
          case DataTypes.BooleanType => record(i) = rs.getBoolean(name)
          case DataTypes.DateType => record(i) = DateTimeUtils.fromJavaDate(rs.getDate(name))
          case DataTypes.DoubleType => record(i) = rs.getDouble(name)
          case DataTypes.FloatType => record(i) = rs.getFloat(name)
          case DataTypes.IntegerType => record(i) = rs.getInt(name)
          case DataTypes.LongType => record(i) = rs.getLong(name)
          case DataTypes.ShortType => record(i) = rs.getShort(name)
          case DataTypes.StringType => record(i) = UTF8String.fromString(rs.getString(name))
          case DataTypes.TimestampType => record(i) = DateTimeUtils.fromJavaTimestamp(rs.getTimestamp(name))
          case DataTypes.BinaryType => record(i) = rs.getBytes(name)
          case DataTypes.NullType => record(i) = StringUtils.EMPTY
        }
      } catch {
        case e: SQLException => log.error(e.getStackTrace.mkString("", scala.util.Properties.lineSeparator, scala.util.Properties.lineSeparator))
      }
    }
    new GenericInternalRow(record)
  }

  override def close(): Unit = {
    helper.closeAll(connection, st, null, rs)
  }
}
