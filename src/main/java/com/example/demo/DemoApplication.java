package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dataexchange.DataExchangeClient;
import software.amazon.awssdk.services.dataexchange.model.ListDataSetsRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;


@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);

		/*Region region = Region.US_WEST_2;
		S3Client s3 = S3Client.builder()
				.region(region)
				.build();

		ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder().build();
		ListBucketsResponse listBucketsResponse = s3.listBuckets(listBucketsRequest);
		listBucketsResponse.buckets().stream().forEach(x -> System.out.println(x.name()));*/
	}
}
