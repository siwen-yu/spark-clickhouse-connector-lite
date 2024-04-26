package com.ysw.spark.sources

import com.clickhouse.client.ClickHouseNode
import com.clickhouse.client.config.ClickHouseClientOption
import com.clickhouse.jdbc._
import com.clickhouse.jdbc.internal.ClickHouseJdbcUrlParser
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

import java.io.Serializable
import java.sql._
import java.text.SimpleDateFormat
import java.util
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * ClickHouse的JDBCHelper实现
 */
class CKHelper(options: CKOptions) extends Serializable {
  final val log = LoggerFactory.getLogger(classOf[CKHelper])

  private val opType: String = options.getOpTypeField
  private val id: String = options.getPrimaryKey
  // 目标集群节点个数
  val nodes: scala.Array[ClickHouseNode] = ClickHouseJdbcUrlParser
    .parse(options.getURL, options.asJdbcProperties())
    .getNodes.getNodes.asScala.toArray

  def getConnection(node: ClickHouseNode): ClickHouseConnection = {
    new ClickHouseDataSource(toJdbcUrl(node), options.asJdbcProperties()).getConnection(options.getUser, options.getPassword)
  }

  private def toJdbcUrl(node: ClickHouseNode): String = {
    var db = node.getDatabase.orElse("")
    var url = s"jdbc:clickhouse://${node.getHost}:${node.getPort}/$db"
    if (!node.getOptions.isEmpty) {
      url += "?"
      node.getOptions.asScala.foreach(t => {
        if (ClickHouseClientOption.DATABASE.getKey == t._1) db = t._2
        else url += s"${t._1}=${t._2}&"
      })
      url = url.substring(0, url.length - 1)
    }
    log.info("connect clickhouse : " + url)
    url
  }

  def createTable(table: String, schema: StructType): String = {
    val cols = ArrayBuffer[String]()
    for (field <- schema.fields) {
      val dataType = field.dataType
      val ckColName = field.name
      if (ckColName != opType) {
        var ckColType = getClickhouseSqlType(dataType)
        if (!StringUtils.isEmpty(ckColType)) {
          if (ckColType.toLowerCase == "string") {
            ckColType = "Nullable(" + ckColType + ")"
          }
        }
        cols += ckColName + " " + ckColType
      }
    }
    s"CREATE TABLE IF NOT EXISTS $table(${cols.mkString(",")},sign Int8,version UInt64) ENGINE=VersionedCollapsingMergeTree(sign, version) ORDER BY $id"
  }

  def getSparkTableSchema(customFields: util.LinkedList[String] = null): StructType = {
    val fields = getCKTableSchema(customFields)
      .map(trp => StructField(trp._1, getSparkSqlType(trp._2)))
    StructType(fields)
  }

  private def getFieldValue(fieldName: String, schema: StructType, data: InternalRow): Any = {
    var flag = true
    var fieldValue: String = null
    val fields = schema.fields
    for (i <- fields.indices if flag) {
      val field = fields(i)
      if (fieldName == field.name) {
        fieldValue = field.dataType match {
          case DataTypes.BooleanType => if (data.isNullAt(i)) "NULL" else s"${data.getBoolean(i)}"
          case DataTypes.DoubleType => if (data.isNullAt(i)) "NULL" else s"${data.getDouble(i)}"
          case DataTypes.FloatType => if (data.isNullAt(i)) "NULL" else s"${data.getFloat(i)}"
          case DataTypes.IntegerType => if (data.isNullAt(i)) "NULL" else s"${data.getInt(i)}"
          case DataTypes.LongType => if (data.isNullAt(i)) "NULL" else s"${data.getLong(i)}"
          case DataTypes.ShortType => if (data.isNullAt(i)) "NULL" else s"${data.getShort(i)}"
          case DataTypes.StringType => if (data.isNullAt(i)) "NULL" else s"${data.getUTF8String(i).toString.trim}"
          case DataTypes.DateType => if (data.isNullAt(i)) "NULL" else s"'${new SimpleDateFormat("yyyy-MM-dd").format(new Date(data.get(i, DateType).asInstanceOf[Date].getTime / 1000))}'"
          case DataTypes.TimestampType => if (data.isNullAt(i)) "NULL" else s"${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(data.getLong(i) / 1000))}"
          case DataTypes.BinaryType => if (data.isNullAt(i)) "NULL" else s"${data.getBinary(i)}"
          case DataTypes.NullType => "NULL"
        }
        flag = false
      }
    }
    fieldValue
  }

  def getStatement(table: String, schema: StructType, record: InternalRow): String = {
    val opTypeValue: String = getFieldValue(opType, schema, record).toString
    if (opTypeValue.toLowerCase() == "insert") {
      getInsertStatement(table, schema, record)
    }
    else if (opTypeValue.toLowerCase() == "delete") {
      getUpdateStatement(table, schema, record)
    }
    else if (opTypeValue.toLowerCase() == "update") {
      getDeleteStatement(table, schema, record)
    }
    else {
      ""
    }
  }

  def getSelectStatement(schema: StructType): String = {
    s"SELECT ${schema.fieldNames.mkString(",")} FROM ${options.getTable}"
  }

  def getInsertStatement(table: String, schema: StructType, data: InternalRow): String = {
    val fields = schema.fields
    val names = ArrayBuffer[String]()
    val values = ArrayBuffer[String]()
    // 表示DataFrame中的字段与数据库中的字段相同，拼接SQL语句时使用全量字段拼接
    if (data.numFields == fields.length) {
    } else { // 表示DataFrame中的字段与数据库中的字段不同，拼接SQL时需要仅拼接DataFrame中有的字段到SQL中
    }
    for (i <- fields.indices) {
      val field = fields(i)
      val fieldType = field.dataType
      val fieldName = field.name
      if (fieldName != opType) {
        val fieldValue = fieldType match {
          case DataTypes.BooleanType => if (data.isNullAt(i)) "NULL" else s"${data.getBoolean(i)}"
          case DataTypes.DoubleType => if (data.isNullAt(i)) "NULL" else s"${data.getDouble(i)}"
          case DataTypes.FloatType => if (data.isNullAt(i)) "NULL" else s"${data.getFloat(i)}"
          case DataTypes.IntegerType => if (data.isNullAt(i)) "NULL" else s"${data.getInt(i)}"
          case DataTypes.LongType => if (data.isNullAt(i)) "NULL" else s"${data.getLong(i)}"
          case DataTypes.ShortType => if (data.isNullAt(i)) "NULL" else s"${data.getShort(i)}"
          case DataTypes.StringType => if (data.isNullAt(i)) "NULL" else s"'${data.getUTF8String(i).toString.trim}'"
          case DataTypes.DateType => if (data.isNullAt(i)) "NULL" else s"'${new SimpleDateFormat("yyyy-MM-dd").format(new Date(data.get(i, DateType).asInstanceOf[Date].getTime / 1000))}'"
          case DataTypes.TimestampType => if (data.isNullAt(i)) "NULL" else s"'${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(data.getLong(i) / 1000))}'"
          case DataTypes.BinaryType => if (data.isNullAt(i)) "NULL" else s"${data.getBinary(i)}"
          case DataTypes.NullType => "NULL"
        }
        names += fieldName
        values += fieldValue
      }
    }
    if (names.nonEmpty && values.nonEmpty) {
      names += ("sign", "version")
      values += ("1", System.currentTimeMillis().toString)
    }
    s"INSERT INTO $table(${names.mkString(",")}) VALUES(${values.mkString(",")})"
  }

  def getDeleteStatement(table: String, schema: StructType, data: InternalRow): String = {
    val fields = schema.fields
    val primaryKeyFields = if (options.getPrimaryKey.isEmpty) {
      fields.filter(field => field.name == "id")
    } else {
      fields.filter(field => field.name == options.getPrimaryKey)
    }
    if (primaryKeyFields.length > 0) {
      val primaryKeyField = primaryKeyFields(0)
      val primaryKeyValue = getFieldValue(primaryKeyField.name, schema, data)
      s"ALTER TABLE $table DELETE WHERE ${primaryKeyField.name} = $primaryKeyValue"
    } else {
      log.error("==== 找不到主键，无法生成删除SQL！")
      ""
    }
  }

  def getUpdateStatement(table: String, schema: StructType, data: InternalRow): String = {
    val fields = schema.fields
    val primaryKeyFields = if (options.getPrimaryKey.isEmpty) {
      fields.filter(field => field.name == "id")
    } else {
      fields.filter(field => field.name == options.getPrimaryKey)
    }
    if (primaryKeyFields.length > 0) {
      val primaryKeyField = primaryKeyFields(0)
      val primaryKeyValue = getFieldValue(primaryKeyField.name, schema, data)
      val noPrimaryKeyFields = fields.filter(field => field.name != primaryKeyField.name)
      var sets = ArrayBuffer[String]()
      for (i <- 0 until noPrimaryKeyFields.length) {
        val noPrimaryKeyField = noPrimaryKeyFields(i)
        val set = noPrimaryKeyField.name + "=" + getFieldValue(noPrimaryKeyField.name, schema, data).toString
        sets += set
      }
      sets.remove(sets.length - 1)
      s"ALTER TABLE $table UPDATE ${sets.mkString(" AND ")} WHERE ${primaryKeyField.name}=$primaryKeyValue"
    } else {
      log.error("==== 找不到主键，无法生成修改SQL！")
      ""
    }
  }

  def getCKTableSchema(customFields: util.LinkedList[String] = null): mutable.MutableList[(String, String)] = {
    val fields = new mutable.MutableList[(String, String)]
    var connection: ClickHouseConnection = null
    var st: ClickHouseStatement = null
    var rs: ClickHouseResultSet = null
    var metaData: ClickHouseResultSetMetaData = null
    try {
      connection = getConnection(nodes.head)
      st = connection.createStatement
      val sql = s"SELECT * FROM ${options.getTable} WHERE 1=0"
      rs = st.executeQuery(sql).asInstanceOf[ClickHouseResultSet]
      metaData = rs.getMetaData.asInstanceOf[ClickHouseResultSetMetaData]
      val columnCount = metaData.getColumnCount
      for (i <- 1 to columnCount) {
        val columnName = metaData.getColumnName(i)
        val sqlTypeName = metaData.getColumnTypeName(i)
        if (null != customFields && customFields.size > 0) {
          if (customFields.contains(columnName)) fields.+=((columnName, sqlTypeName))
        } else {
          fields.+=((columnName, sqlTypeName))
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      closeAll(connection, st, null, rs)
    }
    fields
  }

  def executeAddBatch(node: ClickHouseNode, sqls: ArrayBuffer[String]): Unit = {
    // 拼接Batch SQL：VALUES()()...
    val batchSQL = new StringBuilder()
    for (i <- 0 until sqls.length) {
      val line = sqls(i)
      var offset: Int = 0
      if (!StringUtils.isEmpty(line) && line.contains("VALUES")) {
        val offset = line.indexOf("VALUES")
        if (i == 0) {
          val prefix = line.substring(0, offset + 6)
          batchSQL.append(prefix)
        }
        val suffix = line.substring(offset + 6)
        batchSQL.append(suffix)
      }
    }
    var connection: ClickHouseConnection = null
    var st: ClickHouseStatement = null
    try {
      connection = getConnection(node)
      st = connection createStatement()
      st.executeUpdate(batchSQL.toString())
    } catch {
      case e: Exception => log.error(s"执行异常：$sqls\n${e.getMessage}")
    } finally {
      closeAll(connection, st, null, null)
    }
  }

  def executeUpdate(sql: String): Int = {
    var state = 0;
    nodes.foreach(node => {
      var connection: ClickHouseConnection = null
      var st: ClickHouseStatement = null
      try {
        connection = getConnection(node)
        st = connection createStatement()
        state += st.executeUpdate(sql)
      } catch {
        case e: Exception => log.error(s"执行异常：$sql\n${e.getMessage}")
      } finally {
        closeAll(connection, st, null, null)
      }
    })
    state
  }

  def close(connection: Connection): Unit = closeAll(connection)

  def close(st: Statement): Unit = closeAll(null, st, null, null)

  def close(ps: PreparedStatement): Unit = closeAll(null, null, ps, null)

  def close(rs: ResultSet): Unit = closeAll(null, null, null, rs)

  def closeAll(connection: Connection = null, st: Statement = null, ps: PreparedStatement = null, rs: ResultSet = null): Unit = {
    try {
      if (rs != null && !rs.isClosed) rs.close()
      if (ps != null && !ps.isClosed) ps.close()
      if (st != null && !st.isClosed) st.close()
      if (connection != null && !connection.isClosed) connection.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
   * IntervalYear      (Types.INTEGER,   Integer.class,    true,  19,  0),
   * IntervalQuarter   (Types.INTEGER,   Integer.class,    true,  19,  0),
   * IntervalMonth     (Types.INTEGER,   Integer.class,    true,  19,  0),
   * IntervalWeek      (Types.INTEGER,   Integer.class,    true,  19,  0),
   * IntervalDay       (Types.INTEGER,   Integer.class,    true,  19,  0),
   * IntervalHour      (Types.INTEGER,   Integer.class,    true,  19,  0),
   * IntervalMinute    (Types.INTEGER,   Integer.class,    true,  19,  0),
   * IntervalSecond    (Types.INTEGER,   Integer.class,    true,  19,  0),
   * UInt64            (Types.BIGINT,    BigInteger.class, false, 19,  0),
   * UInt32            (Types.INTEGER,   Long.class,       false, 10,  0),
   * UInt16            (Types.SMALLINT,  Integer.class,    false,  5,  0),
   * UInt8             (Types.TINYINT,   Integer.class,    false,  3,  0),
   * Int64             (Types.BIGINT,    Long.class,       true,  20,  0, "BIGINT"),
   * Int32             (Types.INTEGER,   Integer.class,    true,  11,  0, "INTEGER", "INT"),
   * Int16             (Types.SMALLINT,  Integer.class,    true,   6,  0, "SMALLINT"),
   * Int8              (Types.TINYINT,   Integer.class,    true,   4,  0, "TINYINT"),
   * Date              (Types.DATE,      Date.class,       false, 10,  0),
   * DateTime          (Types.TIMESTAMP, Timestamp.class,  false, 19,  0, "TIMESTAMP"),
   * Enum8             (Types.VARCHAR,   String.class,     false,  0,  0),
   * Enum16            (Types.VARCHAR,   String.class,     false,  0,  0),
   * Float32           (Types.FLOAT,     Float.class,      true,   8,  8, "FLOAT"),
   * Float64           (Types.DOUBLE,    Double.class,     true,  17, 17, "DOUBLE"),
   * Decimal32         (Types.DECIMAL,   BigDecimal.class, true,   9,  9),
   * Decimal64         (Types.DECIMAL,   BigDecimal.class, true,  18, 18),
   * Decimal128        (Types.DECIMAL,   BigDecimal.class, true,  38, 38),
   * Decimal           (Types.DECIMAL,   BigDecimal.class, true,   0,  0, "DEC"),
   * UUID              (Types.OTHER,     UUID.class,       false, 36,  0),
   * String            (Types.VARCHAR,   String.class,     false,  0,  0, "LONGBLOB", "MEDIUMBLOB", "TINYBLOB", "MEDIUMTEXT", "CHAR", "VARCHAR", "TEXT", "TINYTEXT", "LONGTEXT", "BLOB"),
   * FixedString       (Types.CHAR,      String.class,     false, -1,  0, "BINARY"),
   * Nothing           (Types.NULL,      Object.class,     false,  0,  0),
   * Nested            (Types.STRUCT,    String.class,     false,  0,  0),
   * Tuple             (Types.OTHER,     String.class,     false,  0,  0),
   * Array             (Types.ARRAY,     Array.class,      false,  0,  0),
   * AggregateFunction (Types.OTHER,     String.class,     false,  0,  0),
   * Unknown           (Types.OTHER,     String.class,     false,  0,  0);
   *
   * @param clickhouseDataType
   * @return
   */
  private def getSparkSqlType(clickhouseDataType: String) = clickhouseDataType match {
    case "IntervalYear" => DataTypes.IntegerType
    case "IntervalQuarter" => DataTypes.IntegerType
    case "IntervalMonth" => DataTypes.IntegerType
    case "IntervalWeek" => DataTypes.IntegerType
    case "IntervalDay" => DataTypes.IntegerType
    case "IntervalHour" => DataTypes.IntegerType
    case "IntervalMinute" => DataTypes.IntegerType
    case "IntervalSecond" => DataTypes.IntegerType
    case "UInt64" => DataTypes.LongType //DataTypes.IntegerType;
    case "UInt32" => DataTypes.LongType
    case "UInt16" => DataTypes.IntegerType
    case "UInt8" => DataTypes.IntegerType
    case "Int64" => DataTypes.LongType
    case "Int32" => DataTypes.IntegerType
    case "Int16" => DataTypes.IntegerType
    case "Int8" => DataTypes.IntegerType
    case "Date" => DataTypes.DateType
    case "DateTime" => DataTypes.TimestampType
    case "Enum8" => DataTypes.StringType
    case "Enum16" => DataTypes.StringType
    case "Float32" => DataTypes.FloatType
    case "Float64" => DataTypes.DoubleType
    case "Decimal32" => DataTypes.createDecimalType
    case "Decimal64" => DataTypes.createDecimalType
    case "Decimal128" => DataTypes.createDecimalType
    case "Decimal" => DataTypes.createDecimalType
    case "UUID" => DataTypes.StringType
    case "String" => DataTypes.StringType
    case "FixedString" => DataTypes.StringType
    case "Nothing" => DataTypes.NullType
    case "Nested" => DataTypes.StringType
    case "Tuple" => DataTypes.StringType
    case "Array" => DataTypes.StringType
    case "AggregateFunction" => DataTypes.StringType
    case "Unknown" => DataTypes.StringType
    case _ => DataTypes.NullType
  }

  private def getClickhouseSqlType(sparkDataType: DataType) = sparkDataType match {
    case DataTypes.ByteType => "Int8"
    case DataTypes.ShortType => "Int16"
    case DataTypes.IntegerType => "Int32"
    case DataTypes.FloatType => "Float32"
    case DataTypes.DoubleType => "Float64"
    case DataTypes.LongType => "Int64"
    case DataTypes.DateType => "DateTime"
    case DataTypes.TimestampType => "DateTime"
    case DataTypes.StringType => "String"
    case DataTypes.NullType => "String"
  }

}
