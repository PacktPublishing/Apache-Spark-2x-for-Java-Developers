package com.packt.sfjd.ch9;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

//{"flightId":"tz302","timestamp":1494423926816,"temperature":21.12,"landed":false}
public class StateFulProcessingExample {
	public static void main(String[] args) throws InterruptedException {

		System.setProperty("hadoop.home.dir", "C:\\softwares\\Winutils");

		SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("Stateful Streaming Example")
				.config("spark.sql.warehouse.dir", "file:////C:/Users/sgulati/spark-warehouse").getOrCreate();

		JavaStreamingContext jssc= new JavaStreamingContext(new JavaSparkContext(sparkSession.sparkContext()),
				Durations.milliseconds(1000));
		JavaReceiverInputDStream<String> inStream = jssc.socketTextStream("10.204.136.223", 9999);
		jssc.checkpoint("C:\\Users\\sgulati\\spark-checkpoint");

		JavaDStream<FlightDetails> flightDetailsStream = inStream.map(x -> {
			ObjectMapper mapper = new ObjectMapper();
			return mapper.readValue(x, FlightDetails.class);
		});
		
		

		JavaPairDStream<String, FlightDetails> flightDetailsPairStream = flightDetailsStream
				.mapToPair(f -> new Tuple2<String, FlightDetails>(f.getFlightId(), f));

		Function3<String, Optional<FlightDetails>, State<List<FlightDetails>>, Tuple2<String, Double>> mappingFunc = (
				flightId, curFlightDetail, state) -> {
			List<FlightDetails> details = state.exists() ? state.get() : new ArrayList<>();

			boolean isLanded = false;

			if (curFlightDetail.isPresent()) {
				details.add(curFlightDetail.get());
				if (curFlightDetail.get().isLanded()) {
					isLanded = true;
				}
			}
			Double avgSpeed = details.stream().mapToDouble(f -> f.getTemperature()).average().orElse(0.0);

			if (isLanded) {
				state.remove();
			} else {
				state.update(details);
			}
			return new Tuple2<String, Double>(flightId, avgSpeed);
		};

		JavaMapWithStateDStream<String, FlightDetails, List<FlightDetails>, Tuple2<String, Double>> streamWithState = flightDetailsPairStream
				.mapWithState(StateSpec.function(mappingFunc).timeout(Durations.minutes(5)));
		
		streamWithState.print();
		jssc.start();
		jssc.awaitTermination();
	}
}
