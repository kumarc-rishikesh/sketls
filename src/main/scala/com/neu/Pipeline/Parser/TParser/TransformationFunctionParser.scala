package com.neu.Pipeline.Parser.TParser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scala.reflect.runtime.{universe => ru}
import scala.tools.reflect.ToolBox

object TransformationFunctionParser {
  def compileFunction(sourceCode: String) = {
    val toolbox = ru.runtimeMirror(getClass.getClassLoader).mkToolBox()
    toolbox.compile(toolbox.parse(sourceCode))()
  }

  def getCompiledMethodInputType(functions: Any, methodName: String): (DataFrame, String) => DataFrame = {
    val method = functions.getClass.getMethod(methodName,
      classOf[org.apache.spark.sql.DataFrame],
      classOf[String]
    )

    (df: DataFrame, ipFieldInfo: String) =>
      method.invoke(functions, df, ipFieldInfo).asInstanceOf[DataFrame]
  }

  def getCompiledMethodDerivedType(functions: Any, methodName: String): (DataFrame, Seq[String], (String, DataType)) => DataFrame = {
    val method = functions.getClass.getMethod(methodName,
      classOf[org.apache.spark.sql.DataFrame],
      classOf[Seq[String]],
      classOf[(String, DataType)]
    )

    (df: DataFrame, ipFieldInfo: Seq[String], opFieldInfo:(String,DataType)) =>
      method.invoke(functions, df, ipFieldInfo, opFieldInfo).asInstanceOf[DataFrame]
  }
}
