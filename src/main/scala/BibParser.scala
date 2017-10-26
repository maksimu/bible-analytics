import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._ //for count()
import org.apache.spark.sql._


case class ESVVerse(bookID: Int, chapterID: Int, verseID: Int, verseText: String)
case class Verse(verseID: Int, bookID: Int, chapter: Int, verseNum: Int, osisRef: String, verseText: String)
case class Book(bookID: Int, bookName: String, numOfChapters: Int, bookDiv: String, shortName: String, osisName: String)

class BibParser {

  val sparkSession: SparkSession = SparkSession.builder
    .master("local")
    .appName("example")
    .getOrCreate()


  private def readCSVToDF[T <: Product: TypeTag](tempViewName: String,  hasHeader: Boolean, filePath: String): DataFrame = {
    val df = sparkSession
      .read
      .option("header",  hasHeader.toString.toLowerCase)
      .schema(Encoders.product[T].schema)
      .csv(filePath)

    df.createOrReplaceTempView(tempViewName)
    df.cache()
  }





  val esvVersesDF: DataFrame = readCSVToDF[ESVVerse]("esvVerses", hasHeader = false, "src/main/resources/ESV_fixed.csv")
  val versesDF:    DataFrame = readCSVToDF[Verse]   ("verses", hasHeader = true, "src/main/resources/KJV-bible-database-with-metadata-MetaV--master/CSV/Verses.csv")
  val booksDF:     DataFrame = readCSVToDF[Book]    ("books", hasHeader = true, "src/main/resources/KJV-bible-database-with-metadata-MetaV--master/CSV/Books.csv")






  val catalog: Catalog = sparkSession.catalog



}


object Obj{
  def main(args: Array[String]): Unit = {
    println("Hi")

    val bibParser = new BibParser

//    sqlQuery(bibParser)

    dataFrameQuery(bibParser)

  }


  def dataFrameQuery(bibParser: BibParser) = {


    import bibParser.sparkSession.implicits._

    val esvVersesDF = bibParser.esvVersesDF
    val bibleBooksDF = bibParser.booksDF

    val esvBibleVersesJoinedDF = esvVersesDF.join(bibleBooksDF, esvVersesDF("bookID") === bibleBooksDF("bookID"))

    esvBibleVersesJoinedDF

      .where(esvBibleVersesJoinedDF("verseText").contains("Jesus").or(esvBibleVersesJoinedDF("verseText").contains("Joshua")))
      .groupBy(esvVersesDF("bookID"), bibleBooksDF("bookName")).agg(count("*").as("jcount"))
      .orderBy($"jcount".desc)
      .show()

//    esvBibleVersesJoinedDF.select("*")
//      .where(esvBibleVersesJoinedDF("verseText").contains("Lord"))
//        .groupBy("bookID")
//        .agg(count("*").as("jcount"))
//        .orderBy($"jcount".desc)
//      .show()
//
//      .joinWith(bibleBooksDF.as("books"), esvVersesDF.col("bookID") === bibleBooksDF.col("bookID") )
////      .select(
////        //          "bookID",
////        "books.osisName",
////        "esvVerses.chapterID",
////        "esvVerses.verseID",
////        "esvVerses.verseText")
//
//
//
//      .select(esvVersesDF("verseText"))
//      .filter(esvVersesDF("verseText").contains("Jesus"))
//
//      .show()
  }



  def sqlQuery(bibParser: BibParser) = {
    val catalog = bibParser.catalog

    //print the databases
    catalog.listDatabases().select("name").show()

    // print all the tables
    catalog.listTables().select("name").show()


    val ss = bibParser.sparkSession

    val allDF = ss.sql(
      """
        SELECT v.osisRef, b.bookName, v.verseText
        FROM verses AS v JOIN books AS b ON v.bookID = b.bookID
        WHERE verseText LIKE '%God%'
        LIMIT 10
        """.stripMargin)


    //    allDF.filter("Dad").filter("verseText".contains("bar"))

    allDF.explain()
    allDF.foreach(f => println(f))

  }
}