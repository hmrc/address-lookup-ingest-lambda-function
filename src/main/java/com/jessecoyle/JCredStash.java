package com.jessecoyle;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.AwsRegionProvider;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.model.DecryptRequest;
import com.amazonaws.services.kms.model.DecryptResult;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.jessecoyle.WildcardHelper.credentialNamesMatchingWildcard;

public class JCredStash implements AutoCloseable {

    private static final String DEFAULT_TABLE = "credential-store";

    private String tableName;
    private AmazonDynamoDBClient amazonDynamoDBClient;
    private AWSKMSClient awskmsClient;
    private CredStashCrypto credStashCrypto;

    public JCredStash() {
        this(new CredStashBouncyCastleCrypto());
    }

    public JCredStash(String tableName) {
        this(tableName, new CredStashBouncyCastleCrypto());
    }

    public JCredStash(CredStashCrypto crypto) {
        this(DEFAULT_TABLE, crypto);
    }

    public JCredStash(String tableName, CredStashCrypto crypto) {
        this(tableName, new DefaultAWSCredentialsProviderChain(), new DefaultAwsRegionProviderChain(), crypto);
    }

    public JCredStash(String tableName, AWSCredentialsProvider awsCredentialsProvider, AwsRegionProvider awsRegionProvider, CredStashCrypto crypto) {
        this(tableName, createAmazonDynamoDBClient(awsCredentialsProvider, awsRegionProvider), createAwsKmsClient(awsCredentialsProvider, awsRegionProvider), crypto);
    }

    private static AWSKMSClient createAwsKmsClient(AWSCredentialsProvider awsCredentialsProvider, AwsRegionProvider awsRegionProvider) {
        AWSKMSClient awsKmsClient = new AWSKMSClient(awsCredentialsProvider);
        awsKmsClient.setRegion(RegionUtils.getRegion(awsRegionProvider.getRegion()));
        return awsKmsClient;
    }

    private static AmazonDynamoDBClient createAmazonDynamoDBClient(AWSCredentialsProvider awsCredentialsProvider, AwsRegionProvider awsRegionProvider) {
        AmazonDynamoDBClient amazonDynamoDBClient = new AmazonDynamoDBClient(awsCredentialsProvider);
        amazonDynamoDBClient.setRegion(RegionUtils.getRegion(awsRegionProvider.getRegion()));
        return amazonDynamoDBClient;
    }

    public JCredStash(String tableName, AmazonDynamoDBClient amazonDynamoDBClient, AWSKMSClient awskmsClient, CredStashCrypto crypto) {
        this.tableName = tableName;
        this.amazonDynamoDBClient = amazonDynamoDBClient;
        this.awskmsClient = awskmsClient;
        this.credStashCrypto = crypto;
    }

    private StoredSecret readDynamoItem(String tableName, String credential) {
        QueryResult queryResult = amazonDynamoDBClient.query(new QueryRequest(tableName)
                .withLimit(1)
                .withScanIndexForward(false)
                .withConsistentRead(true)
                .addKeyConditionsEntry("name", new Condition()
                        .withComparisonOperator(ComparisonOperator.EQ)
                        .withAttributeValueList(new AttributeValue(credential)))
        );
        if (queryResult.getCount() == 0) {
            throw new RuntimeException("Secret " + credential + " could not be found");
        }
        Map<String, AttributeValue> item = queryResult.getItems().get(0);

        return new StoredSecret(item);
    }

    private ByteBuffer decryptKeyWithKMS(byte[] encryptedKeyBytes, Map<String, String> context) {
        ByteBuffer blob = ByteBuffer.wrap(encryptedKeyBytes);

        DecryptResult decryptResult = awskmsClient.decrypt(new DecryptRequest().withCiphertextBlob(blob).withEncryptionContext(context));

        return decryptResult.getPlaintext();
    }

    public String getSecret(String credential, Map<String, String> context) {

        // The secret was encrypted using AES, then the key for that encryption was encrypted with AWS KMS
        // Then both the encrypted secret and the encrypted key are stored in dynamo

        // First find the relevant rows from the credstash table
        StoredSecret encrypted = readDynamoItem(tableName, credential);

        // First obtain that original key again using KMS
        ByteBuffer plainText = decryptKeyWithKMS(encrypted.getKey(), context);

        // The key is just the first 32 bits, the remaining are for HMAC signature checking
        byte[] keyBytes = new byte[32];
        plainText.get(keyBytes);

        byte[] hmacKeyBytes = new byte[plainText.remaining()];
        plainText.get(hmacKeyBytes);
        byte[] digest = credStashCrypto.digest(hmacKeyBytes, encrypted.getContents());
        if (!Arrays.equals(digest, encrypted.getHmac())) {
            throw new RuntimeException("HMAC integrety check failed"); //TODO custom exception type
        }

        // now use AES to finally decrypt the actual secret
        byte[] decryptedBytes = credStashCrypto.decrypt(keyBytes, encrypted.getContents());
        return new String(decryptedBytes);
    }

    public List<CredentialVersion> listSecrets() {
        ScanResult queryResult = amazonDynamoDBClient.scan(new ScanRequest(tableName)
                .withProjectionExpression("#N, version")
                .withExpressionAttributeNames(new HashMap<String, String>() {{
                    put("#N", "name");
                }})
        );
        return queryResult.getItems()
                .stream()
                .map(item -> new CredentialVersion(item.get("name").getS(), item.get("version").getS()))
                .collect(Collectors.toList());
    }

    public Map<String, String> findSecrets(String credential, Map<String, String> context) {
        return credentialNamesMatchingWildcard(listSecrets(), credential)
                .collect(Collectors.toMap(x -> x, x -> getSecret(x, context)));
    }

    @Override
    public void close() {
        amazonDynamoDBClient.shutdown();
        awskmsClient.shutdown();
    }
}
