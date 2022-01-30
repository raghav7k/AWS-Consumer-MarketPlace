package com.example.demo.dao;

import com.example.demo.model.Share;
import net.minidev.json.JSONArray;
import net.minidev.json.parser.ParseException;
import org.json.JSONException;
import software.amazon.awssdk.services.dataexchange.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ShareDao {

    int insertShare(UUID id, Share share) throws JSONException;

    default int insertShare(Share share) throws JSONException {
        UUID id = UUID.randomUUID();

        return insertShare(id, share);
    }

    List<Share> getShares();

    Optional<Share> getShareById(UUID id);

    int deleteShareById(UUID id);

    ArrayList<DataSetEntry> getDataSets(String product_id) throws ParseException;

    ListDataSetRevisionsResponse getDataSetRevisions(String id);

    List<AssetEntry> getAssets(String d_id, String r_id);

    String exportAssetToS3(String d_id, String r_id, String a_id, String bucket_id);

    State getJobState(String id);

    List<String> listBuckets();

    String autoExportToS3(String bucket, String id);

    JSONArray exportAllRevisions(String id, String revisionId, String bucket_name) throws InterruptedException;

    JSONArray getAssetsFromBucket(String datasetId, String bucket);

    public GetDataSetResponse getDatasetInfo(String datasetId);

    void removeAutoExport(String event_id);
}
