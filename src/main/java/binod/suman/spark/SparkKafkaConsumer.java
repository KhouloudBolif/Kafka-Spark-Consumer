package binod.suman.spark;






import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import com.mongodb.client.MongoClients;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.bson.Document;


import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoClient;

import kafka.serializer.StringDecoder;

public class SparkKafkaConsumer {

	public static void main(String[] args) throws IOException {

		System.out.println("Spark Streaming started now .....");

		SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// batchDuration - The time interval at which streaming data will be divided into batches
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(20000));

		// Configuration MongoDB (replace with your connection details)
		String mongoURI = "mongodb://localhost:27017";
		String databaseName = "ma_base_de_donnees";
		String collectionName = "ma_collection";
		MongoClient mongoClient = MongoClients.create(mongoURI); // Utilisez MongoClients.create() pour obtenir un MongoClient
		MongoDatabase database = mongoClient.getDatabase(databaseName);
		MongoCollection<Document> collection = database.getCollection(collectionName);

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
		Set<String> topics = Collections.singleton("test10");

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		List<String> allRecord = new ArrayList<String>();
		final String COMMA = ",";

		directKafkaStream.foreachRDD(rdd -> {

			System.out.println("New data arrived  " + rdd.partitions().size() +" Partitions and " + rdd.count() + " Records");
			if(rdd.count() > 0) {
				rdd.collect().forEach(rawRecord -> {

					System.out.println(rawRecord);
					System.out.println("***************************************");
					System.out.println(rawRecord._2);
					String record = rawRecord._2();
					StringTokenizer st = new StringTokenizer(record,",");
					allRecord.add(record);
					StringBuilder sb = new StringBuilder();

					// Conversion des données en Document MongoDB
					while (st.hasMoreTokens()) {
						// Lire chaque champ du CSV
						String state = st.nextToken();
						int accountLength = Integer.parseInt(st.nextToken());
						int areaCode = Integer.parseInt(st.nextToken());
						String internationalPlan = st.nextToken();
						String voiceMailPlan = st.nextToken();
						int numVoiceMailMessages = Integer.parseInt(st.nextToken());
						double totalDayMinutes = Double.parseDouble(st.nextToken());
						int totalDayCalls = Integer.parseInt(st.nextToken());
						double totalDayCharge = Double.parseDouble(st.nextToken());
						double totalEveMinutes = Double.parseDouble(st.nextToken());
						int totalEveCalls = Integer.parseInt(st.nextToken());
						double totalEveCharge = Double.parseDouble(st.nextToken());
						double totalNightMinutes = Double.parseDouble(st.nextToken());
						int totalNightCalls = Integer.parseInt(st.nextToken());
						double totalNightCharge = Double.parseDouble(st.nextToken());
						double totalIntlMinutes = Double.parseDouble(st.nextToken());
						int totalIntlCalls = Integer.parseInt(st.nextToken());
						double totalIntlCharge = Double.parseDouble(st.nextToken());
						int customerServiceCalls = Integer.parseInt(st.nextToken());
						String churn = st.nextToken();

						// Créer un document MongoDB pour chaque ligne
						Document doc = new Document()
								.append("state", state)
								.append("accountLength", accountLength)
								.append("areaCode", areaCode)
								.append("internationalPlan", internationalPlan)
								.append("voiceMailPlan", voiceMailPlan)
								.append("numVoiceMailMessages", numVoiceMailMessages)
								.append("totalDayMinutes", totalDayMinutes)
								.append("totalDayCalls", totalDayCalls)
								.append("totalDayCharge", totalDayCharge)
								.append("totalEveMinutes", totalEveMinutes)
								.append("totalEveCalls", totalEveCalls)
								.append("totalEveCharge", totalEveCharge)
								.append("totalNightMinutes", totalNightMinutes)
								.append("totalNightCalls", totalNightCalls)
								.append("totalNightCharge", totalNightCharge)
								.append("totalIntlMinutes", totalIntlMinutes)
								.append("totalIntlCalls", totalIntlCalls)
								.append("totalIntlCharge", totalIntlCharge)
								.append("customerServiceCalls", customerServiceCalls)
								.append("churn", churn);

						collection.insertOne(doc);
					}

					// Enregistrement dans la collection MongoDB


				});
				System.out.println("All records OUTER MOST :"+allRecord.size());
				FileWriter writer = new FileWriter("Master_dataset.csv");
				for(String s : allRecord) {
					writer.write(s);
					writer.write("\n");
				}
				System.out.println("Master dataset has been created : ");
				writer.close();
			}
		});

		ssc.start();
		ssc.awaitTermination();
		mongoClient.close(); // Fermeture de la connexion MongoDB
	}


}


