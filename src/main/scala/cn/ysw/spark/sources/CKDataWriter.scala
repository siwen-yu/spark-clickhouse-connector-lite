package cn.ysw.spark.sources

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.io.Serializable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * ClickHouse的数据写实现
 */
class CKDataWriter(writeUUID: String, schema: StructType, batchMode: SaveMode, streamMode: OutputMode, options: CKOptions) extends DataWriter[InternalRow] with Serializable {
  final val log = LoggerFactory.getLogger(classOf[CKDataWriter])
  val helper = new CKHelper(options)
  val opType = options.getOpTypeField
  private val sqls = ArrayBuffer[String]()
  private val autoCreateTable: Boolean = options.autoCreateTable
  private val init = if (autoCreateTable) {
    val createSQL = helper.createTable(options.getTable, schema)
    println(/** logInfo* */
      s"==== 初始化表SQL：$createSQL")
    helper.executeUpdate(createSQL)
  }
  val fields = schema.fields

  override def commit(): WriterCommitMessage = {
    // 随机写一个节点
    val randomIndex = new Random().nextInt(helper.nodes.length)
    helper.executeAddBatch(helper.nodes(randomIndex), sqls)
    val batchSQL = sqls.mkString("\n")
    new WriterCommitMessage {
      override def toString: String = s"批量插入SQL: $batchSQL"
    }
  }

  override def write(record: InternalRow): Unit = {
    if (StringUtils.isEmpty(opType)) {
      throw new RuntimeException("未传入opTypeField字段名称，无法确定数据持久化类型！")
    }
    var sqlStr: String = helper.getStatement(options.getTable, schema, record)
    log.debug(s"==== 拼接完成的INSERT SQL语句为：$sqlStr")
    try {
      if (StringUtils.isEmpty(sqlStr)) {
        val msg = "==== 拼接INSERT SQL语句失败，因为该语句为NULL或EMPTY！"
        log.error(msg)
        throw new RuntimeException(msg)
      }
      Thread.sleep(options.getInterval())
      // 在流处理模式下操作
      if (null == batchMode) {
        if (streamMode == OutputMode.Append) {
          sqls += sqlStr
          // val state = helper.executeUpdate(sqlStr)
          // println(s"==== 在OutputMode.Append模式下执行：$sqlStr\n状态：$state")
        }
        else if (streamMode == OutputMode.Complete) {
          log.error("==== 未实现OutputMode.Complete模式下的写入操作，请在CKDataWriter.write方法中添加相关实现！")
        }
        else if (streamMode == OutputMode.Update) {
          log.error("==== 未实现OutputMode.Update模式下的写入操作，请在CKDataWriter.write方法中添加相关实现！")
        }
        else {
          log.error(s"==== 未知模式下的写入操作，请在CKDataWriter.write方法中添加相关实现！")
        }
        // 在批处理模式下操作
      } else {
        if (batchMode == SaveMode.Append) {
          sqls += sqlStr
          //val state = helper.executeUpdate(sqlStr)
          //println(s"==== 在SaveMode.Append模式下执行：$sqlStr\n状态：$state")
        }
        else if (batchMode == SaveMode.Overwrite) {
          log.error("==== 未实现SaveMode.Overwrite模式下的写入操作，请在CKDataWriter.write方法中添加相关实现！")
        }
        else if (batchMode == SaveMode.ErrorIfExists) {
          log.error("==== 未实现SaveMode.ErrorIfExists模式下的写入操作，请在CKDataWriter.write方法中添加相关实现！")
        }
        else if (batchMode == SaveMode.Ignore) {
          log.error("==== 未实现SaveMode.Ignore模式下的写入操作，请在CKDataWriter.write方法中添加相关实现！")
        }
        else {
          log.error(s"==== 未知模式下的写入操作，请在CKDataWriter.write方法中添加相关实现！")
        }
      }
    } catch {
      case e: Exception => log.error(e.getMessage)
    }
  }

  override def abort(): Unit = {}
}
