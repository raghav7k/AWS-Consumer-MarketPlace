package com.example.demo.dao;

import com.example.demo.model.Share;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.ParseException;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.auth.credentials.*;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.codepipeline.model.AWSSessionCredentials;

import software.amazon.awssdk.services.dataexchange.DataExchangeClient;
import software.amazon.awssdk.services.dataexchange.model.*;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.util.*;

@Repository("Dao")
@JsonSerialize
public class FakeShareDataService implements ShareDao{

    private static ArrayList<Share> Shares = new ArrayList<>();

    @Override
    public int insertShare(UUID id, Share share) {
        Shares.add(new Share(id, share.getShareDataSets()));
        return 1;
    }

    @Override
    public List<Share> getShares() {

        AwsBasicCredentials credentials = AwsBasicCredentials.create("AKIA4JCDUSJZI7HDUYG4", "YJYl8QaNtf7ZUMxR3CnZ00h2NJnKcBM7vzN+tvCT");
        Region region = Region.US_WEST_2;
        S3Client s3 = S3Client.builder().credentialsProvider(StaticCredentialsProvider.create(credentials))
                .region(region)
                .build();

        DataExchangeClient dataExchangeClient = DataExchangeClient.create();
        ListDataSetsRequest dataSetsRequest = ListDataSetsRequest.builder().origin("ENTITLED").maxResults(1).build();
        ListDataSetsResponse listDataSetsResponse= dataExchangeClient.listDataSets(dataSetsRequest);

        listDataSetsResponse.dataSets().stream().forEach(x -> System.out.println(x.id()));

        return Shares;
    }

    @Override
    public Optional<Share> getShareById(UUID id) {
        return Shares.stream().filter(share -> share.getShare_id().equals(id)).findFirst();
    }

    public int deleteShareById(UUID share_id) {
        Optional<Share> share = getShareById(share_id);
        if (share.isEmpty()) {
            return 0;
        }
        Shares.remove(share.get());
        return 1;
    }

    @Override
    public ArrayList<DataSetEntry> getDataSets(String product_id) throws ParseException {
        DataExchangeClient dataExchangeClient = DataExchangeClient.create();
        ListDataSetsRequest dataSetsRequest = ListDataSetsRequest.builder().origin("ENTITLED").build();
        ListDataSetsResponse listDataSetsResponse= dataExchangeClient.listDataSets(dataSetsRequest).toBuilder().build();

        listDataSetsResponse.dataSets().stream().forEach(x -> System.out.println(x));
        // return listDataSetsResponse.dataSets();

        ArrayList<DataSetEntry> datasets = new ArrayList();

        listDataSetsResponse.dataSets().stream().forEach(x ->{
            if (x.originDetails().productId().compareTo(product_id) == 0)
                datasets.add(x);
        });
        return datasets;
    }

    @Override
    public GetDataSetResponse getDatasetInfo(String datasetId) {
        DataExchangeClient dataExchangeClient = DataExchangeClient.create();
        GetDataSetRequest dataSetRequest = GetDataSetRequest.builder().dataSetId(datasetId).build();
        GetDataSetResponse dataSetResponse = dataExchangeClient.getDataSet(dataSetRequest);
        System.out.println(dataSetResponse);
        return dataSetResponse;
    }

    @Override
    public ListDataSetRevisionsResponse getDataSetRevisions(String id) {
        DataExchangeClient dataExchangeClient = DataExchangeClient.create();
        ListDataSetRevisionsRequest dataSetRevisionsRequest = ListDataSetRevisionsRequest.builder().dataSetId(id).build();
        ListDataSetRevisionsResponse listDataSetRevisionsResponse = dataExchangeClient.listDataSetRevisions(dataSetRevisionsRequest);
        listDataSetRevisionsResponse.revisions().stream().forEach(x -> System.out.println(x));
        return listDataSetRevisionsResponse;
    }

    public JSONArray exportAllRevisions(String id, String revision_id, String bucket_name) throws InterruptedException {
        JSONArray jsonArray = new JSONArray();
        DataExchangeClient dataExchangeClient = DataExchangeClient.create();
        ListDataSetRevisionsRequest dataSetRevisionsRequest = ListDataSetRevisionsRequest.builder().dataSetId(id).build();
        ListDataSetRevisionsResponse listDataSetRevisionsResponse = dataExchangeClient.listDataSetRevisions(dataSetRevisionsRequest);

        HashMap<String, String> map = new HashMap<>();
        listDataSetRevisionsResponse.revisions().stream().forEach(x -> map.put(x.createdAt().toString() , x.id()));
        System.out.println(listDataSetRevisionsResponse.nextToken());
        while(listDataSetRevisionsResponse.nextToken() != null) {
            dataSetRevisionsRequest = ListDataSetRevisionsRequest.builder().dataSetId(id).nextToken(listDataSetRevisionsResponse.nextToken()).build();
            listDataSetRevisionsResponse = dataExchangeClient.listDataSetRevisions(dataSetRevisionsRequest);
            listDataSetRevisionsResponse.revisions().stream().forEach(x -> map.put(x.createdAt().toString() , x.id()));
        }

        TreeMap<String, String> Revisions = new TreeMap<>();

        Revisions.putAll(map);
        System.out.println(Revisions.size());

        boolean flag = false;

        for ( Map.Entry<String, String> x: Revisions.entrySet()) {

            if ( x.getValue().compareTo(revision_id) == 0) {
                flag = true;
            }
            if ( !flag ) continue;;

            String keyPattern = id+"/${Asset.Name}";

            System.out.println(x.getKey());
            System.out.println((x.getValue()));
            RevisionDestinationEntry revisionDestinationEntry = RevisionDestinationEntry.builder().revisionId(x.getValue()).
                    bucket(bucket_name).keyPattern(keyPattern).build();
            ExportRevisionsToS3RequestDetails exportRevisionsToS3RequestDetails = ExportRevisionsToS3RequestDetails.builder().
                    dataSetId(id).revisionDestinations(revisionDestinationEntry).build();
            RequestDetails requestDetails = RequestDetails.builder().exportRevisionsToS3(exportRevisionsToS3RequestDetails).build();
            CreateJobRequest jobRequest = CreateJobRequest.builder().type("EXPORT_REVISIONS_TO_S3").details(requestDetails).build();
            CreateJobResponse createJobResponse = dataExchangeClient.createJob(jobRequest);
            String jobId = createJobResponse.id();
            String jobState = createJobResponse.state().toString();
            StartJobRequest startJobRequest = StartJobRequest.builder().jobId(jobId).build();
            StartJobResponse startJobResponse = dataExchangeClient.startJob(startJobRequest);

            while ( jobState != "COMPLETED") {
                Thread.sleep(2000);
                GetJobRequest getJobRequest = GetJobRequest.builder().jobId(jobId).build();
                GetJobResponse jobResponse = dataExchangeClient.getJob(getJobRequest);
                jobState = jobResponse.state().toString();
            }
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("RevisionId", x.getValue());
            jsonObject.put("CreatedAt", x.getKey());
            jsonObject.put("jobId", jobId);

            jsonArray.add(jsonObject);

        }
    return jsonArray;
    }


    @Override
    public List<AssetEntry> getAssets(String d_id, String r_id) {

        DataExchangeClient dataExchangeClient = DataExchangeClient.create();
        ListRevisionAssetsRequest listRevisionAssetsRequest = ListRevisionAssetsRequest.builder().dataSetId(d_id).revisionId(r_id).build();
        ListRevisionAssetsResponse listDataSetRevisionsResponse = dataExchangeClient.listRevisionAssets(listRevisionAssetsRequest);
        listDataSetRevisionsResponse.assets().stream().forEach(x-> System.out.println(x));
        return listDataSetRevisionsResponse.assets();
    }

    @Override
    public String exportAssetToS3(String d_id, String r_id, String a_id, String bucket_id) {


        String keyPattern = "${Asset.Name}";

        AssetDestinationEntry assetDestinationEntry = AssetDestinationEntry.builder().assetId(a_id)
                .bucket(bucket_id).build();
        ExportAssetsToS3RequestDetails exportAssetsToS3RequestDetails = ExportAssetsToS3RequestDetails.builder().
                dataSetId(d_id).assetDestinations(assetDestinationEntry).revisionId(r_id).build();
        DataExchangeClient dataExchangeClient = DataExchangeClient.create();

        RequestDetails requestDetails = RequestDetails.builder().exportAssetsToS3(exportAssetsToS3RequestDetails).build();
        CreateJobRequest jobRequest = CreateJobRequest.builder().type("EXPORT_ASSETS_TO_S3").details(requestDetails).build();


        CreateJobResponse createJobResponse = dataExchangeClient.createJob(jobRequest);
        String jobId = createJobResponse.id();

        StartJobRequest startJobRequest = StartJobRequest.builder().jobId(jobId).build();
        StartJobResponse startJobResponse = dataExchangeClient.startJob(startJobRequest);
        System.out.println(jobId);

        GetJobRequest getJobRequest = GetJobRequest.builder().jobId(jobId).build();
        GetJobResponse jobResponse = dataExchangeClient.getJob(getJobRequest);
        System.out.println(jobResponse.state());
        return jobId;

    }

    @Override
    public State getJobState(String id) {
        DataExchangeClient dataExchangeClient = DataExchangeClient.create();
        GetJobRequest getJobRequest = GetJobRequest.builder().jobId(id).build();
        GetJobResponse jobResponse = dataExchangeClient.getJob(getJobRequest);
        System.out.println(jobResponse.state());
        return  jobResponse.state();
    }

    @Override
    public List<String> listBuckets() {

        // Creating bucket of VCm accounts
        S3Client s3Client = S3Client.create();
        ListBucketsResponse listBucketsResponse = s3Client.listBuckets();

        System.out.println(listBucketsResponse.buckets());
        ArrayList<String> buckets = new ArrayList<>();
        listBucketsResponse.buckets().stream().forEach(x -> buckets.add(x.name()));

        // Assuming role of DV
        StsClient stsClient = StsClient.create();
        AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder().roleArn("arn:aws:iam::844102931058:role/test_AssumeRole_VCM").roleSessionName("DVSession").durationSeconds(900)
                        .build();
        AssumeRoleResponse assumeRoleResponse =  stsClient.assumeRole(assumeRoleRequest);
        Credentials credentials = assumeRoleResponse.credentials();

        // First approach
        AwsCredentials awsCredentials = new AwsCredentials() {

            @Override
            public String accessKeyId() {
                return credentials.accessKeyId();
            }

            @Override
            public String secretAccessKey() {
                return credentials.secretAccessKey();
            }
        };

        AwsCredentialsProvider aws = new AwsCredentialsProvider() {
            @Override
            public AwsCredentials resolveCredentials() {
                return awsCredentials;
            }
        };
        System.out.println(aws.resolveCredentials().accessKeyId() + " "+ credentials.accessKeyId());
        S3Client s3Client2 = S3Client.builder().credentialsProvider(aws).build();
        // System.out.println(s3Client2.listBuckets());

        // Different approach
        AWSSessionCredentials awsSessionCredentials = AWSSessionCredentials.builder().accessKeyId(credentials.accessKeyId())
                .sessionToken(credentials.sessionToken()).secretAccessKey(credentials.secretAccessKey()).build();

        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(
                credentials.accessKeyId(),
                credentials.secretAccessKey());

        S3Client s3 = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsBasicCredentials))
                .build();

        System.out.println(s3.listBuckets());
        return buckets;
    }

    @Override
    public String autoExportToS3(String bucket, String id) {

        String keyPattern = id+"/${Asset.Name}";
        DataExchangeClient dataExchangeClient = DataExchangeClient.create();
        // Creating the auto export request
        AutoExportRevisionDestinationEntry autoExportRevisionDestinationEntry = AutoExportRevisionDestinationEntry.builder().bucket(bucket).keyPattern(keyPattern).build();
        AutoExportRevisionToS3RequestDetails autoExportRevisionToS3RequestDetails = AutoExportRevisionToS3RequestDetails.builder().
                revisionDestination(autoExportRevisionDestinationEntry).build();
        Action action = Action.builder().exportRevisionToS3(autoExportRevisionToS3RequestDetails).build();

        RevisionPublished revisionPublished = RevisionPublished.builder().dataSetId(id).build();
        Event event = Event.builder().revisionPublished(revisionPublished).build();
        CreateEventActionRequest createEventActionRequest = CreateEventActionRequest.builder().action(action).event(event).build();

        // calling the auto export call
        CreateEventActionResponse createEventActionResponse =  dataExchangeClient.createEventAction(createEventActionRequest);

        System.out.println(createEventActionResponse.id());
        return createEventActionResponse.id();
    }

    @Override
    public JSONArray getAssetsFromBucket(String datasetId, String bucket) {
        System.out.println(bucket);
        JSONArray assets = new JSONArray();

        S3Client s3Client = S3Client.builder().region(Region.US_WEST_2).build();
        ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder().bucket(bucket).prefix(datasetId).build();
        ListObjectsResponse listObjectsResponse = s3Client.listObjects(listObjectsRequest);
        listObjectsResponse.contents().forEach((S3Object s3) -> {
            JSONObject s3object = new JSONObject();
            String[] arrayList = s3.key().split("/");
            String x = arrayList[arrayList.length -1];
            System.out.println(x);
            s3object.put("name", x);
            s3object.put("key", s3.key());
            s3object.put("lastModified", s3.lastModified());
            s3object.put("size", s3.size());

            assets.add(s3object);
        });
        System.out.println(listObjectsResponse.contents());
        return assets;
    }

    @Override
    public void removeAutoExport(String event_id) {
        DataExchangeClient dataExchangeClient = DataExchangeClient.create();

        DeleteEventActionRequest removeAutoExport = DeleteEventActionRequest.builder().eventActionId(event_id).build();

        DeleteEventActionResponse response = dataExchangeClient.deleteEventAction(removeAutoExport);

    }



}
