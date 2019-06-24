package org.elasticsearch.snapshots;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class S3Repository implements Repository {

    private final Terminal terminal;
    private final AmazonS3 client;
    private final String bucket;
    private final String basePath;

    S3Repository(Terminal terminal, String endpoint, String region, String accessKey, String secretKey, String bucket, String basePath) {
        this.terminal = terminal;
        this.client = buildS3Client(endpoint, region, accessKey, secretKey);
        this.basePath = basePath;
        this.bucket = bucket;
    }

    private static AmazonS3 buildS3Client(String endpoint, String region, String accessKey, String secretKey) {
        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));
        if (Strings.hasLength(endpoint)) {
            builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, null))
                    .enablePathStyleAccess();
        } else {
            builder.withRegion(region);
        }

        return builder.build();
    }

    private final String fullPath(String path) {
        return basePath + "/" + path;
    }

    @Override
    public Long readLatestIndexId() throws IOException {
        try (InputStream blob = client.getObject(bucket, fullPath(BlobStoreRepository.INDEX_LATEST_BLOB)).getObjectContent()) {
            BytesStreamOutput out = new BytesStreamOutput();
            Streams.copy(blob, out);
            return Numbers.bytesToLong(out.bytes().toBytesRef());
        } catch (IOException e) {
            terminal.println("Failed to read index.latest blob");
            throw e;
        }
    }

    @Override
    public RepositoryData getRepositoryData(Long indexFileGeneration) throws IOException {
        final String snapshotsIndexBlobName = BlobStoreRepository.INDEX_FILE_PREFIX + indexFileGeneration;
        try (InputStream blob = client.getObject(bucket, fullPath(snapshotsIndexBlobName)).getObjectContent()) {
            BytesStreamOutput out = new BytesStreamOutput();
            Streams.copy(blob, out);
            // EMPTY is safe here because RepositoryData#fromXContent calls namedObject
            try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE, out.bytes(), XContentType.JSON)) {
                return RepositoryData.snapshotsFromXContent(parser, indexFileGeneration);
            }
        } catch (IOException e) {
            terminal.println("Failed to read " + snapshotsIndexBlobName + " file");
            throw e;
        }
    }

    @Override
    public Set<String> getAllIndexIds() {
        try {
            List<String> prefixes = new ArrayList<>();
            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(bucket);
            request.setPrefix(fullPath("indices/"));
            request.setDelimiter("/");
            ObjectListing object_listing = client.listObjects(request);
            prefixes.addAll(object_listing.getCommonPrefixes());

            while (object_listing.isTruncated()) ;
            {
                object_listing = client.listNextBatchOfObjects(object_listing);
                prefixes.addAll(object_listing.getCommonPrefixes());
            }
            int indicesPrefixLength = fullPath("indices/").length();
            assert prefixes.stream().allMatch(prefix -> prefix.startsWith(fullPath("indices/")));
            return prefixes.stream().map(prefix -> prefix.substring(indicesPrefixLength, prefix.length()-1)).collect(Collectors.toSet());
        } catch (AmazonServiceException e) {
            terminal.println("Failed to list indices");
            throw e;
        }
    }

    @Override
    public Date getIndexNTimestamp(Long indexFileGeneration) {
        final String snapshotsIndexBlobName = BlobStoreRepository.INDEX_FILE_PREFIX + indexFileGeneration;
        ObjectListing listing = client.listObjects(bucket, fullPath(snapshotsIndexBlobName));
        List<S3ObjectSummary> summaries = listing.getObjectSummaries();
        if (summaries.size() != 1) {
            terminal.println("Unexpected size");
            return null;
        } else {
            S3ObjectSummary any = summaries.get(0);
            return any.getLastModified();
        }
    }

    @Override
    public Date getIndexTimestamp(String indexId) {
        ObjectListing listing = client.listObjects(bucket, fullPath("indices/" + indexId + "/"));
        List<S3ObjectSummary> summaries = listing.getObjectSummaries();
        while (summaries.isEmpty() && listing.isTruncated()) {
            summaries = listing.getObjectSummaries();
        }
        if (summaries.isEmpty()) {
            terminal.println("Failed to find single file in index directory");
            return null;
        } else {
            S3ObjectSummary any = summaries.get(0);
            return any.getLastModified();
        }
    }

    private List<String> listAllFiles(String prefix) {
        List<String> files = new ArrayList<>();

        ObjectListing listing = client.listObjects(bucket, prefix);
        while (true) {
            List<S3ObjectSummary> summaries = listing.getObjectSummaries();
            for (S3ObjectSummary obj : summaries) {
                files.add(obj.getKey());
            }

            if (listing.isTruncated()) {
                listing = client.listNextBatchOfObjects(listing);
            } else {
                return files;
            }
        }
    }

    private void deleteFiles(List<String> files) {
        List<List<String>> deletePartitions = new ArrayList<>();
        List<String> currentDeletePartition = new ArrayList<>();
        for (String file : files) {
            if (currentDeletePartition.size() == 1000) {
                currentDeletePartition = new ArrayList<>();
                deletePartitions.add(currentDeletePartition);
            }
            currentDeletePartition.add(file);
        }
        if (currentDeletePartition.isEmpty() == false) {
            deletePartitions.add(currentDeletePartition);
        }
        for (List<String> partition: deletePartitions) {
            client.deleteObjects(new DeleteObjectsRequest(bucket).withKeys(Strings.toStringArray(partition)));
        }
    }

    @Override
    public void deleteIndices(Set<String> leakedIndexIds) {
        for (String indexId : leakedIndexIds) {
            List<String> files = listAllFiles(fullPath("indices/" + indexId));
            deleteFiles(files);
        }
    }
}
