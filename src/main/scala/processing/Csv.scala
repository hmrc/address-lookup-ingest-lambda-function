package processing

import java.io.{BufferedReader, File, FileInputStream, FileOutputStream, FileReader, PrintWriter}
import java.nio.file.{Files, Paths}
import java.util.zip.ZipInputStream

class Csv(private val root: String) {
  def process(): Unit = {
    val typeToWriterMap: Map[String, PrintWriter] =
      Csv.fileNameToHeadingsMap
        .map { case (rid, m) =>
          (
            rid,
            m("file_name").toString,
            m("headers").asInstanceOf[Seq[String]]
          )
        }
        .map { case (rid, fn, hs) => rid -> createWriterAndAddHeader(fn, hs) }
        .toMap

    Paths
      .get(root)
      .toFile
      .listFiles
      .filter(_.getName.endsWith(".zip"))
      .flatMap(unpackZipFile)
      .foreach(f => processFile(typeToWriterMap)(f))

    typeToWriterMap.foreach{case (_, out) => out.flush; out.close}
  }

  private def createWriterAndAddHeader(name: String, headers: Seq[String]) = {
    val f = Paths.get(root, name).toFile
    val w = new PrintWriter(f)
    w.println(headers.mkString(","))
    w.flush()
    w
  }

  private def unpackZipFile(f: File) = {
    val zin = new ZipInputStream(new FileInputStream(f))
    Stream.continually(zin.getNextEntry).takeWhile(_ != null).map { file =>
      val foutf = File.createTempFile(f.getName, "")
      val fout = new FileOutputStream(foutf)
      val buffer = new Array[Byte](1024)
      Stream
        .continually(zin.read(buffer))
        .takeWhile(_ != -1)
        .foreach(fout.write(buffer, 0, _))
      foutf
    }
  }

  private def processFile(typeToWriterMap: Map[String, PrintWriter])(f: File): Unit = {
    val fr = new BufferedReader(new FileReader(f))

    Stream.continually(fr.readLine()).takeWhile(_ != null).foreach { l =>
      val rowId = l.substring(0, l.indexOf(","))
      val writerForRow = typeToWriterMap(rowId)
      writerForRow.println(l)
    }
  }
}

object Csv {
  private val ID10_Header_Records = "ID10_Header_Records.csv"
  private val ID11_Street_Records = "ID11_Street_Records.csv"
  private val ID15_StreetDesc_Records = "ID15_StreetDesc_Records.csv"
  private val ID21_BLPU_Records = "ID21_BLPU_Records.csv"
  private val ID23_XREF_Records = "ID23_XREF_Records.csv"
  private val ID24_LPI_Records = "ID24_LPI_Records.csv"
  private val ID28_DPA_Records = "ID28_DPA_Records.csv"
  private val ID29_Metadata_Records = "ID29_Metadata_Records.csv"
  private val ID30_Successor_Records = "ID30_Successor_Records.csv"
  private val ID31_Org_Records = "ID31_Org_Records.csv"
  private val ID32_Class_Records = "ID32_Class_Records.csv"
  private val ID99_Trailer_Records = "ID99_Trailer_Records.csv"

  val tableToFileNames = Map(
    "abp_blpu" -> ID21_BLPU_Records,
    "abp_delivery_point" -> ID28_DPA_Records,
    "abp_lpi" -> ID24_LPI_Records,
    "abp_crossref" -> ID23_XREF_Records,
    "abp_classification" -> ID32_Class_Records,
    "abp_street" -> ID11_Street_Records,
    "abp_street_descriptor" -> ID15_StreetDesc_Records,
    "abp_organisation" -> ID31_Org_Records,
    "abp_successor" -> ID30_Successor_Records
  )


  val fileNameToHeadingsMap: Map[String, Map[String, Object]] = Map(
    "10" -> Map(
      "file_name" -> ID10_Header_Records,
      "headers" -> Seq(
        "RECORD_IDENTIFIER",
        "CUSTODIAN_NAME",
        "LOCAL_CUSTODIAN_NAME",
        "PROCESS_DATE",
        "VOLUME_NUMBER",
        "ENTRY_DATE",
        "TIME_STAMP",
        "VERSION",
        "FILE_TYPE"
      )
    ),
    "11" -> Map(
      "file_name" -> ID11_Street_Records,
      "headers" -> Seq(
        "RECORD_IDENTIFIER",
        "CHANGE_TYPE",
        "PRO_ORDER",
        "USRN",
        "RECORD_TYPE",
        "SWA_ORG_REF_NAMING",
        "STATE",
        "STATE_DATE",
        "STREET_SURFACE",
        "STREET_CLASSIFICATION",
        "VERSION",
        "STREET_START_DATE",
        "STREET_END_DATE",
        "LAST_UPDATE_DATE",
        "RECORD_ENTRY_DATE",
        "STREET_START_X",
        "STREET_START_Y",
        "STREET_START_LAT",
        "STREET_START_LONG",
        "STREET_END_X",
        "STREET_END_Y",
        "STREET_END_LAT",
        "STREET_END_LONG",
        "STREET_TOLERANCE"
      )
    ),
    "15" -> Map(
      "file_name" -> ID15_StreetDesc_Records,
      "headers" -> Seq(
        "RECORD_IDENTIFIER",
        "CHANGE_TYPE",
        "PRO_ORDER",
        "USRN",
        "STREET_DESCRIPTION",
        "LOCALITY_NAME",
        "TOWN_NAME",
        "ADMINSTRATIVE_AREA",
        "LANGUAGE",
        "START_DATE",
        "END_DATE",
        "LAST_UPDATE_DATE",
        "ENTRY_DATE"
      )
    ),
    "21" -> Map(
      "file_name" -> ID21_BLPU_Records,
      "headers" -> Seq(
        "RECORD_IDENTIFIER",
        "CHANGE_TYPE",
        "PRO_ORDER",
        "UPRN",
        "LOGICAL_STATUS",
        "BLPU_STATE",
        "BLPU_STATE_DATE",
        "PARENT_UPRN",
        "X_COORDINATE",
        "Y_COORDINATE",
        "LATITUDE",
        "LONGITUDE",
        "RPC",
        "LOCAL_CUSTODIAN_CODE",
        "COUNTRY",
        "START_DATE",
        "END_DATE",
        "LAST_UPDATE_DATE",
        "ENTRY_DATE",
        "ADDRESSBASE_POSTAL",
        "POSTCODE_LOCATOR",
        "MULTI_OCC_COUNT"
      )
    ),
    "23" -> Map(
      "file_name" -> ID23_XREF_Records,
      "headers" -> Seq(
        "RECORD_IDENTIFIER",
        "CHANGE_TYPE",
        "PRO_ORDER",
        "UPRN",
        "XREF_KEY",
        "CROSS_REFERENCE",
        "VERSION",
        "SOURCE",
        "START_DATE",
        "END_DATE",
        "LAST_UPDATE_DATE",
        "ENTRY_DATE"
      )
    ),
    "24" -> Map(
      "file_name" -> ID24_LPI_Records,
      "headers" -> Seq(
        "RECORD_IDENTIFIER",
        "CHANGE_TYPE",
        "PRO_ORDER",
        "UPRN",
        "LPI_KEY",
        "LANGUAGE",
        "LOGICAL_STATUS",
        "START_DATE",
        "END_DATE",
        "LAST_UPDATE_DATE",
        "ENTRY_DATE",
        "SAO_START_NUMBER",
        "SAO_START_SUFFIX",
        "SAO_END_NUMBER",
        "SAO_END_SUFFIX",
        "SAO_TEXT",
        "PAO_START_NUMBER",
        "PAO_START_SUFFIX",
        "PAO_END_NUMBER",
        "PAO_END_SUFFIX",
        "PAO_TEXT",
        "USRN",
        "USRN_MATCH_INDICATOR",
        "AREA_NAME",
        "LEVEL",
        "OFFICIAL_FLAG"
      )
    ),
    "28" -> Map(
      "file_name" -> ID28_DPA_Records,
      "headers" -> Seq(
        "RECORD_IDENTIFIER",
        "CHANGE_TYPE",
        "PRO_ORDER",
        "UPRN",
        "UDPRN",
        "ORGANISATION_NAME",
        "DEPARTMENT_NAME",
        "SUB_BUILDING_NAME",
        "BUILDING_NAME",
        "BUILDING_NUMBER",
        "DEPENDENT_THOROUGHFARE",
        "THOROUGHFARE",
        "DOUBLE_DEPENDENT_LOCALITY",
        "DEPENDENT_LOCALITY",
        "POST_TOWN",
        "POSTCODE",
        "POSTCODE_TYPE",
        "DELIVERY_POINT_SUFFIX",
        "WELSH_DEPENDENT_THOROUGHFARE",
        "WELSH_THOROUGHFARE",
        "WELSH_DOUBLE_DEPENDENT_LOCALITY",
        "WELSH_DEPENDENT_LOCALITY",
        "WELSH_POST_TOWN",
        "PO_BOX_NUMBER",
        "PROCESS_DATE",
        "START_DATE",
        "END_DATE",
        "LAST_UPDATE_DATE",
        "ENTRY_DATE"
      )
    ),
    "29" -> Map(
      "file_name" -> ID29_Metadata_Records,
      "headers" -> Seq(
        "RECORD_IDENTIFIER",
        "GAZ_NAME",
        "GAZ_SCOPE",
        "TER_OF_USE",
        "LINKED_DATA",
        "GAZ_OWNER",
        "NGAZ_FREQ",
        "CUSTODIAN_NAME",
        "CUSTODIAN_UPRN",
        "LOCAL_CUSTODIAN_CODE",
        "CO_ORD_SYSTEM",
        "CO_ORD_UNIT",
        "META_DATE",
        "CLASS_SCHEME",
        "GAZ_DATE",
        "LANGUAGE",
        "CHARACTER_SET"
      )
    ),
    "30" -> Map(
      "file_name" -> ID30_Successor_Records,
      "headers" -> Seq(
        "RECORD_IDENTIFIER",
        "CHANGE_TYPE",
        "PRO_ORDER",
        "UPRN",
        "SUCC_KEY",
        "START_DATE",
        "END_DATE",
        "LAST_UPDATE_DATE",
        "ENTRY_DATE",
        "SUCCESSOR"
      )
    ),
    "31" -> Map(
      "file_name" -> ID31_Org_Records,
      "headers" -> Seq(
        "RECORD_IDENTIFIER",
        "CHANGE_TYPE",
        "PRO_ORDER",
        "UPRN",
        "ORG_KEY",
        "ORGANISATION",
        "LEGAL_NAME",
        "START_DATE",
        "END_DATE",
        "LAST_UPDATE_DATE",
        "ENTRY_DATE"
      )
    ),
    "32" -> Map(
      "file_name" -> ID32_Class_Records,
      "headers" -> Seq(
        "RECORD_IDENTIFIER",
        "CHANGE_TYPE",
        "PRO_ORDER",
        "UPRN",
        "CLASS_KEY",
        "CLASSIFICATION_CODE",
        "CLASS_SCHEME",
        "SCHEME_VERSION",
        "START_DATE",
        "END_DATE",
        "LAST_UPDATE_DATE",
        "ENTRY_DATE"
      )
    ),
    "99" -> Map(
      "file_name" -> ID99_Trailer_Records,
      "headers" -> Seq(
        "RECORD_IDENTIFIER",
        "NEXT_VOLUME_NUMBER",
        "RECORD_COUNT",
        "ENTRY_DATE",
        "TIME_STAMP"
      )
    )
  )
}
