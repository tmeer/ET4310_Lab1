import org.apache.log4j.{Level, Logger};
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql._;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions._;
import java.sql.Timestamp;

case class GKGSchemaHeaders
(
	GKGRECORDID:				String,
	DATE:						Timestamp,
	SourceCollectionIdentifier:	Integer,
	SourceCommonName:			String,
	DocumentIdentifier:			String,
	Counts:						String,
	V2Counts:					String,
	Themes:						String,
	V2Themes:					String,
	Locations:					String,
	V2Locations:				String,
	Persons:					String,
	V2Persons:					String,
	Organizations:				String,
	V2Organizations:			String,
	V2Tone:						String,
	Dates:						String,
	GCAM:						String,
	SharingImage:				String,
	RelatedImages:				String,
	SocialImageEmbeds:			String,
	SocialVideoEmbeds:			String,
	Quotations:					String,
	AllNames:					String,
	Amounts:					String,
	TranslationInfo:			String,
	Extras:						String	
)

object Lab1
{
	def main(args: Array[String])
	{
		//	Initialize Spark
		println("Starting lab 1!");
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
		val spark = SparkSession.builder.appName("ET4310 Lab 1").getOrCreate();
		val sc = spark.sparkContext;
		import spark.implicits._;

		// Define the GKG schema
		val GKGSchema = StructType(
			Seq(
					StructField("GKGRECORDID",					StringType,		true),
					StructField("DATE",							TimestampType,	true),
					StructField("SourceCollectionIdentifier",	IntegerType,	true),
					StructField("SourceCommonName",				StringType,		true),
					StructField("DocumentIdentifier",			StringType,		true),
					StructField("Counts",						StringType,		true),
					StructField("V2Counts",						StringType,		true),
					StructField("Themes",						StringType,		true),
					StructField("V2Themes",						StringType,		true),
					StructField("Locations",					StringType,		true),
					StructField("V2Locations",					StringType,		true),
					StructField("Persons",						StringType,		true),
					StructField("V2Persons",					StringType,		true),
					StructField("Organizations",				StringType,		true),
					StructField("V2Organizations",				StringType,		true),
					StructField("V2Tone",						StringType,		true),
					StructField("Dates",						StringType,		true),
					StructField("GCAM",							StringType,		true),
					StructField("SharingImage",					StringType,		true),
					StructField("RelatedImages",				StringType,		true),
					StructField("SocialImageEmbeds",			StringType,		true),
					StructField("SocialVideoEmbeds",			StringType,		true),
					StructField("Quotations",					StringType,		true),
					StructField("AllNames",						StringType,		true),
					StructField("Amounts",						StringType,		true),
					StructField("TranslationInfo",				StringType,		true),
					StructField("Extras",						StringType,		true)	
			)
		);

		// Load the data from the segment files
		val segment1 = spark.read.schema(GKGSchema).
			option("delimiter", "\t").
			option("timestampFormat", "YYYYMMDDHHMMSS").
			csv("data/segment/20150218230000.gkg.csv").
			as[GKGSchemaHeaders];
		val segment2 = spark.read.schema(GKGSchema).
			option("delimiter", "\t").
			option("timestampFormat", "YYYYMMDDHHMMSS").
			csv("data/segment/20150218231500.gkg.csv").
			as[GKGSchemaHeaders];
		val segment3 = spark.read.schema(GKGSchema).
			option("delimiter", "\t").
			option("timestampFormat", "YYYYMMDDHHMMSS").
			csv("data/segment/20150218233000.gkg.csv").
			as[GKGSchemaHeaders];
		val segment4 = spark.read.schema(GKGSchema).
			option("delimiter", "\t").
			option("timestampFormat", "YYYYMMDDHHMMSS").
			csv("data/segment/20150218234500.gkg.csv").
			as[GKGSchemaHeaders];

		// Take the union of the data frames
		val allSegments = segment1.unionAll(segment2).unionAll(segment3).unionAll(segment4);

		// Find the topics
		val allSegmentsNames = allSegments.
			//limit(100).					// limit the size of the data frame for testing purposes
			select("AllNames").				// select only the relevant column
			flatMap(_.mkString.split(";")). // split the rows so that each row contains one topic
			filter("value != 'null'").		// filter out rows only containing 'null' (for some reason column changed names)
			map(line => line.substring(0, line.lastIndexOf(','))).	// remove the char index for each row
			toDF("AllNames");				// change back to data frame of rows instead of strings

		// Group by topic and count
		val allSegmentsGrouped = allSegmentsNames.groupBy("AllNames").count();
		// Sort them in descending order
		val allSegmentsSorted = allSegmentsGrouped.orderBy(desc("count"));

		// And finally display the top 10 topics
		allSegmentsSorted.take(50).foreach(println);

		spark.stop();
	}
}
