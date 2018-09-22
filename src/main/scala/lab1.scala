import org.apache.log4j.{Level, Logger};
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql._;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions._;
import java.sql.Timestamp;
import scala.collection.mutable.WrappedArray;

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
		
		// Read the files
		val segments = spark.read.schema(GKGSchema).
			option("delimiter", "\t").
			option("timestampFormat", "YYYYMMDDHHMMSS").
			csv("data/segment/*.csv").
			as[GKGSchemaHeaders];

		// User-defined function for removing the character offset number from each occurrence of a topic in a document
		val removeCharOffset = udf((data : String) => data.substring(0, data.lastIndexOf(',')));
		val removeTimeComponent = udf((date : Timestamp) => date);
//		val mergeColumns = udf((col1 : String, col2 : String) => col1 + "," + col2);
		val mergeColumns = udf((col1 : String, col2 : String) => (col1, col2));
		val shortenArray = udf((wrappedArray : WrappedArray[(String,String)]) => wrappedArray.take(10));

		// Find the topics
		val countedTopicsPerDay = segments.//limit(50).
			select("DATE", "AllNames").									//select the two relevant columns
			withColumn("DATE", removeTimeComponent($"DATE")).
			withColumn("AllNames", explode(split($"AllNames", ";"))).	//generate a seperate row for each date-topic pair
			filter("AllNames != 'null'").								//remove the null entries
			withColumn("AllNames", removeCharOffset($"AllNames")).		//remove the character offset numbers from each row
			groupBy("DATE", "AllNames").count().						//group and count the occurrences of each topic
			orderBy(desc("count")).										//order them in a descending order by count
			withColumn("AllNames", mergeColumns($"AllNames", $"count")).//merge the AllNames and count column
			select("DATE", "AllNames").									//effectively delete the count column
			groupBy("DATE").agg(collect_list("AllNames").as("AllNames")).
			withColumn("AllNames", shortenArray($"AllNames"));

		countedTopicsPerDay.printSchema();

		countedTopicsPerDay.collect.foreach(println);
/*		
			
		// Group by topic and count
		val allSegmentsGrouped = allSegmentsNames.groupBy("AllNames").count();
		// Sort them in descending order
		val allSegmentsSorted = allSegmentsGrouped.orderBy(desc("count"));

		// And finally display the top 10 topics
		allSegmentsSorted.take(10).foreach(println);
*/
		spark.stop();
	}
}
