package cn.ysw.spark.sources

import org.apache.commons.lang3.StringUtils

import java.io.Serializable
import java.util
import java.util.Properties

/**
 * 从SparkSQL中DataSourceOptions中提取适用于ClickHouse的参数（spark.[read/write].options参数）
 */
class CKOptions(var originalMap: util.Map[String, String]) extends Serializable {
  val DRIVER_KEY: String = "driver"
  val URL_KEY: String = "url"
  val USER_KEY: String = "user"
  val PASSWORD_KEY: String = "password"
  val DATABASE_KEY: String = "database"
  val TABLE_KEY: String = "table"
  val AUTO_CREATE_TABLE = "autoCreateTable".toLowerCase
  val INTERVAL = "interval"
  val CUSTOM_SCHEMA_KEY: String = "customSchema".toLowerCase
  val WHERE_KEY: String = "where"
  val OP_TYPE_FIELD = "opTypeField".toLowerCase
  val PRIMARY_KEY = "primaryKey".toLowerCase
  val IGNORE_ERR_NODE = "ignoreErrNode".toLowerCase

  def getValue[T](key: String, `type`: T): T = (if (originalMap.containsKey(key)) originalMap.get(key) else null).asInstanceOf[T]

  def getDriver: String = getValue(DRIVER_KEY, new String)

  def getURL: String = getValue(URL_KEY, new String)

  def getUser: String = getValue(USER_KEY, new String)

  def getPassword: String = getValue(PASSWORD_KEY, new String)

  def getDatabase: String = getValue(DATABASE_KEY, new String)

  def getTable: String = getValue(TABLE_KEY, new String)

  def getIgnoreErrNode: Boolean = {
    originalMap.getOrDefault(IGNORE_ERR_NODE, "false").toLowerCase match {
      case "true" => true
      case "false" => false
      case _ => false
    }
  }

  def autoCreateTable: Boolean = {
    originalMap.getOrDefault(AUTO_CREATE_TABLE, "false").toLowerCase match {
      case "true" => true
      case "false" => false
      case _ => false
    }
  }

  def getInterval(): Long = {
    originalMap.getOrDefault(INTERVAL, "200").toLong
  }

  def getWhere: String = getValue(WHERE_KEY, new String)

  def getCustomSchema: String = getValue(CUSTOM_SCHEMA_KEY, new String)

  def getOpTypeField: String = getValue(OP_TYPE_FIELD, new String)

  def getPrimaryKey: String = getValue(PRIMARY_KEY, new String)

  def getFullTable: String = {
    val database = getDatabase
    val table = getTable
    if (StringUtils.isEmpty(database) && !StringUtils.isEmpty(table)) table else if (!StringUtils.isEmpty(database) && !StringUtils.isEmpty(table)) database + "." + table else table
  }

  def asMap(): util.Map[String, String] = this.originalMap
  def asJdbcProperties(): Properties = {
    val properties = new Properties()
    properties.putAll(this.originalMap)
    properties.remove(URL_KEY)
    properties.remove(USER_KEY)
    properties.remove(PASSWORD_KEY)
    properties.remove(TABLE_KEY)
    properties
  }

  override def toString: String = originalMap.toString
}
