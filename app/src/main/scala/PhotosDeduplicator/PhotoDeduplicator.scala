package PhotosDeduplicator

import net.semanticmetadata.lire.aggregators.BOVW
import net.semanticmetadata.lire.builders.DocumentBuilder
import net.semanticmetadata.lire.indexers.parallel.ImagePreprocessor
import net.semanticmetadata.lire.indexers.parallel.ParallelIndexer
import net.semanticmetadata.lire.searchers.GenericFastImageSearcher
import net.semanticmetadata.lire.utils.ImageUtils
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.store.FSDirectory
import net.semanticmetadata.lire.imageanalysis.features.global.AutoColorCorrelogram
import net.semanticmetadata.lire.imageanalysis.features.global.CEDD
import net.semanticmetadata.lire.imageanalysis.features.global.FCTH
import javax.imageio.ImageIO
import java.awt.image.BufferedImage
import java.io.IOException
import java.nio.file.Paths
import scala.util.Using, java.io.{PrintWriter, File}
import scala.collection.mutable


object PhotoDeduplicator {
  def main(args: Array[String]): Unit = {

    val folder = "Path to your dir"
    index(folder)

    val duplicates = new scala.collection.mutable.Queue[String]
    val toDelete = new scala.collection.mutable.Queue[String]

    val images = getAllImages(folder, descendIntoSubDirectories = true)

    for (i <- images) {
      if (!duplicates.contains(i)) {
        val duplicate = search(i)
        // When we find a dupe we remove it from processing as it is marked for delete so we dont later on reverse the result of search
        duplicate match {
          case None => null
          case Some(duplicate) => {
            duplicates += duplicate;


            if(getFilename(i) != getFilename(duplicate))toDelete += getFilename(i)+ ",";println(s"Image $i is duplicated by $duplicate");

          }
        }
      }
    }


    Using(new PrintWriter(new File("toDelete.csv"))) {
      writer => toDelete.foreach(writer.println)
    }


  }

  def getFilename (fullPath: String):String  = {
    fullPath.split("/").last
  }

  @throws[IOException]
  def getAllImages(dirname: String, descendIntoSubDirectories: Boolean): mutable.Queue[String] = {
    val dir = new File(dirname)
    val resultList = new scala.collection.mutable.Queue[String]
    val f = dir.listFiles()
    for (file <- f) {
      if (file != null && file.getName.toLowerCase.endsWith(".jpg")) resultList += file.getCanonicalPath
      if (descendIntoSubDirectories && file.isDirectory) {
        val tmp = getAllImages(file.getAbsolutePath, true)
        if (tmp != null) resultList ++= tmp
      }
    }
    if (resultList.nonEmpty) resultList
    else null
  }

  @throws[IOException]
  def search(image: String): Option[String] = {
    val indexPath = "index"
    val reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))


    val imgSearcher = new GenericFastImageSearcher(3, classOf[CEDD])

    val hits = imgSearcher.search(ImageIO.read(new File(image)), reader)

    //System.out.printf("%.2f: (%d) %s\n", hits.score(0), hits.documentID(0), reader.document(hits.documentID(0)).getValues(DocumentBuilder.FIELD_NAME_IDENTIFIER)(0))
    if (hits.score(0) == 0.0 && hits.score(1) == 0.0) {
      val name1 = reader.document(hits.documentID(0)).getValues(DocumentBuilder.FIELD_NAME_IDENTIFIER)(0);
      val name2 = reader.document(hits.documentID(1)).getValues(DocumentBuilder.FIELD_NAME_IDENTIFIER)(0);
      // When there is a hit it will always include source and the duplicate. We dont want to delete the source
      if (image == name1)
        Some(name2)
      else
        Some(name1)
    }
    else None


  }

  def index(dirName: String): Unit = {
    val aggregator = classOf[BOVW]

    val indexer = new ParallelIndexer(50, "index", dirName, 128, 500, aggregator)
    indexer.setImagePreprocessor(new ImagePreprocessor() {
      override def process(image: BufferedImage): BufferedImage = ImageUtils.createWorkingCopy(image)

    })

    indexer.addExtractor(classOf[CEDD])
    indexer.addExtractor(classOf[FCTH])
    indexer.addExtractor(classOf[AutoColorCorrelogram])

    indexer.run();

  }
}
