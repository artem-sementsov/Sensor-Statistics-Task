package com

import java.io._
import java.lang.Math._

package object asementsov {

  def getAbsoluteResourcePath(resource: String): String =
    new File(getClass.getClassLoader.getResource(resource).getFile).getCanonicalPath

  def getResourceInputStream(resource: String): InputStream =
    new FileInputStream(new File(getAbsoluteResourcePath(resource)))

  def generateTestFile(file: File, header: String, content: String = "", times: Int = 0): Unit = {
    val printWriter = new PrintWriter(new FileOutputStream(file), true)

    printWriter.println(header)
    (0 until max(1, times)).foreach(_ => printWriter.println(content))
  }
}