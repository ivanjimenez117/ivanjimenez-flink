package mx.cic

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.{File, PrintWriter}
import java.util.regex.Pattern

import org.apache.flink.api.scala._

/**
 * Realizar el WordCount de "n" cantidad de documentos en un directorio y escribir el resultado en un archivo
 * con la estructura siguiente:
 *    file1, file2, file3, #Files(3)
 *    *************** (linea de separación)
 *    #totalWordsOfFile1, #totalWordsOfFile2, #totalWordsOfFile3
 *    *************** (linea de separación)
 *    #TotalWordsRead(totalWordsOfFile1+totalWordsOfFile2+totalWordsOfFile3)
 *    *************** (linea de separación)
 *    nameFile1
 *    word, value
 *    .....
 *    .....
 *    nameFile2
 *    word, value
 *    .....
 *    .....
 *    nameFile3
 *    word, value
 *
 *
 */
object WordCount {
  // patron regex para procesar solo caracteres alfabeticos
  val pattern = Pattern.compile("[a-zA-Z]+")

  /**
   * Se pasan como argumentos el directorio de entrada y el nombre del archivo de salida, que se guardará en el directorio
   * out
   * Se crea una lista de tipo File con los archivos que contiene el directorio de entrada.
   * Se itera sobre esta lista para realizar el conteo total de palabras de cada archivo, los totales parciales se suman
   * para obtener el total de palabras en todos los archivos.
   * Se vuelve a iterar sobre los archivos para obtener su conteo por palabra.
   *
   * @param args Argumentos que se pasan en la línea de comandos: "<input_directory>" "<output_file>"
   *
   */
  def main(args: Array[String]) {
    val files = new File(args(0)).listFiles().toList

    val writeFile = new File(args(1))
    val print_Writer = new PrintWriter(writeFile)

    for (file <- files) {
      print_Writer.write(file.getName + ", ")
    }
    print_Writer.write(files.length + "\n")
    print_Writer.write("*****************************\n")

    var count = 0
    for (file <- files){
      val currTotal = CountTotal(file)
      count += currTotal
      print_Writer.write(currTotal + ", ")
    }
    print_Writer.write("\n*****************************\n")
    print_Writer.write(count.toString + "\n")

    print_Writer.write("\n*****************************\n")

    for (file <- files) {
      val currWordCount = WordCount(file)
      print_Writer.write(file.getName + "\n")
      for ((k,v) <- currWordCount.collect())  {
        print_Writer.write(s"$k : $v \n")
      }
    }
    print_Writer.close()

  }

  /**
   * Esta función genera el WordCount de cada archivo pasando el texto a minusculas, separandolo por caracteres que no
   * sean de palabra, Se les aplica un filtro para obtener solo Strings con caracteres alfabeticos.
   * @param file Es el archivo al que se le aplicara el wordcount
   * @return Devuelve un AggregateDataSet de tuplas de tipo (palabra, conteo)
   */
  def WordCount(file: File): DataSet[(String, Int)] = {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(file.getPath)

    val counts = text.flatMap {
      _.toLowerCase.split("\\W+")
    }
      .filter(word => pattern.matcher(word).matches())
      .map {
        (_, 1)
      }
      .groupBy(0)
      .sum(1)

    counts
  }

  /**
   * Esta función obtiene el total de palabras en cada archivo.
   * @param file Es el archivo a procesar.
   * @return Devuelve un entero, que es el total de palabras que contiene el archivo.
   */

  def CountTotal(file: File): Int ={

    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(file.getPath)

    val counts = text.flatMap { _.toLowerCase.split("\\W+") }
      .filter(word => pattern.matcher(word).matches())
      .map { (_, 1) }
      .sum(1)

    counts.collect()(0)._2
  }
}

