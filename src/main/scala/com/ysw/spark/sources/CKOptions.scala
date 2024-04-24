package com.ysw.spark.sources

import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging

import java.io.Serializable
import java.util

/**
 * 从SparkSQL中DataSourceOptions中提取适用于ClickHouse的参数（spark.[read/write].options参数）
 */
class CKOptions(var originalMap: util.Map[String, String]) extends Logging with Serializable {
  val DRIVER_KEY: String = "driver"
  val URL_KEY: String = "url"
  val USER_KEY: String = "user"
  val PASSWORD_KEY: String = "password"
  val DATABASE_KEY: String = "database"
  val TABLE_KEY: String = "table"
  val AUTO_CREATE_TABLE = "autoCreateTable".toLowerCase
  val PATH_KEY = "path"
  val INTERVAL = "interval"
  val CUSTOM_SCHEMA_KEY: String = "customSchema".toLowerCase
  val WHERE_KEY: String = "where"
  val OP_TYPE_FIELD = "opTypeField".toLowerCase
  val PRIMARY_KEY = "primaryKey".toLowerCase

  def getValue[T](key: String, `type`: T): T = (if (originalMap.containsKey(key)) originalMap.get(key) else null).asInstanceOf[T]

  def getDriver: String = getValue(DRIVER_KEY, new String)

  def getURL: String = getValue(URL_KEY, new String)

  def getUser: String = getValue(USER_KEY, new String)

  def getPassword: String = getValue(PASSWORD_KEY, new String)

  def getDatabase: String = getValue(DATABASE_KEY, new String)

  def getTable: String = getValue(TABLE_KEY, new String)

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

  def getPath: String = if (StringUtils.isEmpty(getValue(PATH_KEY, new String))) getTable else getValue(PATH_KEY, new String)

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

  override def toString: String = originalMap.toString
}
