package bdt.cs523;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaStreamProducer {
	
	public static void main(String[] args) throws Exception {
		
		final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);

		String consumerKey = "flsfOlKjS9KR4xWwM8pztn4Um";
		String consumerSecret = "eAnP1TF9CF0obix4XndikAbAYPBmTEwW358ueQeY6Mkav5NYZp"; 
		String accessToken = "968452682775257088-kNLEtvuGX5MYGxo7oV99IyQtW0gDQ99"; 
		String accessTokenSecret = "QT8MvNaS4rDQwuM2b1gpRkEz7GEO2Vxrn4cY9p8i3ybgu"; 

		
		String topicName = "warInNorthEthiopia";

		String[] keyWords = {"#nomore"}; 

		ConfigurationBuilder configBuilder = new ConfigurationBuilder();
		configBuilder.setDebugEnabled(true)
				.setOAuthConsumerKey(consumerKey)
				.setOAuthConsumerSecret(consumerSecret)
				.setOAuthAccessToken(accessToken)
				.setOAuthAccessTokenSecret(accessTokenSecret);

		TwitterStream twitterStream = new TwitterStreamFactory(configBuilder.build()).getInstance();
		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("Got scrub_geo event userId:" + userId + "upToStatusId:" + upToStatusId);
			}

			@Override
			public void onStallWarning(StallWarning warning) {
				System.out.println("Got stall warning:" + warning);
			}

			@Override
			public void onException(Exception ex) {
				ex.printStackTrace();
			}
		};
		
		twitterStream.addListener(listener);

		FilterQuery query = new FilterQuery().track(keyWords);
		twitterStream.filter(query);

		Properties properties = new Properties();
		properties.put("metadata.broker.list", "localhost:9092");
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("acks", "all");
		properties.put("retries", 0);
		properties.put("batch.size", 16384);
		properties.put("linger.ms", 1);
		properties.put("buffer.memory", 33554432);

		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		@SuppressWarnings("resource")
		Producer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		int j = 0;

		while (true) {
			
			Status tweets = queue.poll();

			if (tweets == null) {
				Thread.sleep(100);
			} else {
					System.out.println("Tweet:" + tweets);
					String msg = new String (tweets.getCreatedAt() + ", " + 
							tweets.getUser().getFollowersCount()+ ", " + 
							tweets.getUser().getFavouritesCount() + ", " + 
							getLocation(tweets.getUser().getLocation())  + ", " + 
							tweets.getRetweetCount() + ", " + 
							tweets.getFavoriteCount() + ", " + 
							tweets.getLang());
					
					producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(j++), msg));

			}
		}
	}
	
	private static String getLocation(String loc){
		
		if (loc == null) 
			return "null" ;
		else return loc.split(",")[0];
	}
	
}