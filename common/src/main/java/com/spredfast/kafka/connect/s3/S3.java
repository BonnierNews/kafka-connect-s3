package com.spredfast.kafka.connect.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.util.Map;

public class S3 {

	public static AmazonS3 s3client(Map<String, String> config) {
		AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard();
		ClientConfiguration clientConfig = new ClientConfiguration();

		Boolean useS3PathStyle = Boolean.parseBoolean(config.get("s3.path_style"));
		s3ClientBuilder.withPathStyleAccessEnabled(useS3PathStyle);

		String endpoint = config.get("s3.endpoint");
		if (endpoint != null && endpoint.trim().length() > 0) {
			s3ClientBuilder.withEndpointConfiguration(
				new EndpointConfiguration(endpoint, Regions.EU_WEST_1.getName())
			);
		}

		String signer = config.getOrDefault("s3.signer", "S3SignerType");
		if (signer != null && signer.trim().length() > 0) {
			clientConfig.setSignerOverride(signer);

		}

		return s3ClientBuilder.withClientConfiguration(clientConfig).build();
	}

}
