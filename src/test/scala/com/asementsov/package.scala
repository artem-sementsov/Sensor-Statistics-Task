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

//  def repeatRowAsInputStream(sample: String, times: Int): InputStream = {
//    val data: Array[Byte] = sample.getBytes
//    var arrayIndex = 0
//    var counter = 0
//    new InputStream() {
//      override def read(): Int = {
//        if (counter >= times) {
//          -1
//        } else {
//          val result: Byte = data(arrayIndex)
//          arrayIndex += 1
//          if (arrayIndex >= data.length) {
//            arrayIndex = 0
//            counter += 1
//          }
//          result
//        }
//      }
//    }
//  }
}
