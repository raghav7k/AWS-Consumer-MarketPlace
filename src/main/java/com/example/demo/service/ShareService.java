package com.example.demo.service;

import com.amazonaws.services.marketplacediscovery.model.GetSearchFacetsResult;
import com.amazonaws.services.marketplacediscovery.model.SearchListingsResult;
import com.example.demo.dao.DiscoveryAPI;
import com.example.demo.dao.InPlaceWork;
import com.example.demo.dao.JSONWork;
import com.example.demo.dao.ShareDao;
import com.example.demo.model.Share;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dataexchange.model.*;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class ShareService {

    private final ShareDao shareDao;
    public final JSONWork jsonWork;
    public final DiscoveryAPI discoveryAPI;
    public final InPlaceWork inPlaceWork;
    public final InplaceShareService inplaceShareService;
    private final MarketplaceShareService marketplaceShareService;

    @Autowired
    public ShareService(@Qualifier("Dao") ShareDao shareDao, @Qualifier("JSON") JSONWork jsonWork,
                        @Qualifier("Discovery") DiscoveryAPI discoveryAPI, @Qualifier("InPlace") InPlaceWork inPlaceWork,
                        InplaceShareService inplaceShareService, MarketplaceShareService marketplaceShareService) {
        this.shareDao = shareDao;
        this.jsonWork = jsonWork;
        this.discoveryAPI = discoveryAPI;
        this.inPlaceWork = inPlaceWork;
        this.inplaceShareService = inplaceShareService;
        this.marketplaceShareService = marketplaceShareService;
    }
    public int addShare(Share share) throws JSONException {
        return shareDao.insertShare(share);
    }

    public JSONArray getDataSets(String product_id) throws ParseException {
        ArrayList<DataSetEntry> dataSets =  shareDao.getDataSets(product_id);
        System.out.println(dataSets);

        JSONArray jsonArray = new JSONArray();

        for( DataSetEntry dataSetEntry: dataSets) {
            JSONObject jsonObject = new JSONObject();

            jsonObject.put("ARN", dataSetEntry.arn());
            jsonObject.put("AssetType", dataSetEntry.assetTypeAsString());
            jsonObject.put("CreatedAt", dataSetEntry.createdAt());
            jsonObject.put("Description", dataSetEntry.description());
            jsonObject.put("DatasetId", dataSetEntry.id());
            jsonObject.put("Name", dataSetEntry.name());
            JSONObject origindetails = new JSONObject();
            origindetails.put("ProductId", dataSetEntry.originDetails().productId());
            jsonObject.put("Origin", dataSetEntry.origin());
            jsonObject.put("OriginDetails", origindetails);
            jsonObject.put("UpdatedAt", dataSetEntry.updatedAt());
            jsonArray.add(jsonObject);
        }
        return jsonArray;
    }

    public JSONObject getDatasetInfo(String datasetId) {
        GetDataSetResponse dataset = shareDao.getDatasetInfo(datasetId);
        JSONObject response = new JSONObject();
        response.put("id", dataset.id());
        response.put("arn", dataset.arn());
        response.put("assetType", dataset.assetType());
        response.put("description", dataset.description());
        response.put("name", dataset.name());
        response.put("productId", dataset.originDetails().productId());
        return response;
    }

    public JSONArray getDataSetRevision(String id) {
        ListDataSetRevisionsResponse listDataSetRevisionsResponse = shareDao.getDataSetRevisions(id);
        JSONArray jsonArray = new JSONArray();

        for (RevisionEntry revisionEntry: listDataSetRevisionsResponse.revisions()) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("ARN", revisionEntry.arn());
            jsonObject.put("Comment", revisionEntry.comment());
            jsonObject.put("RevisionId", revisionEntry.id());
            jsonObject.put("DatasetId", revisionEntry.dataSetId());
            jsonObject.put("Finalized", revisionEntry.finalized());
            jsonObject.put("CreatedAt", revisionEntry.createdAt());
            jsonObject.put("UpdatedAt", revisionEntry.updatedAt());

            jsonArray.add(jsonObject);
        }

        return jsonArray;
    }

    public JSONArray getAssets(String d_id, String r_id) {
        List<AssetEntry> Assets = shareDao.getAssets(d_id, r_id);
        JSONArray jsonArray = new JSONArray();
        for (AssetEntry assetEntry: Assets) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("Arn", assetEntry.arn());
            jsonObject.put("Name", assetEntry.name());
            jsonObject.put("AssetType", assetEntry.assetTypeAsString());
            jsonObject.put("DatasetId", assetEntry.dataSetId());
            jsonObject.put("RevisionId", assetEntry.revisionId());
            jsonObject.put("AssetId", assetEntry.id());
            jsonObject.put("CreatedAt", assetEntry.createdAt());
            jsonObject.put("UpdatedAt", assetEntry.updatedAt());
            jsonArray.add(jsonObject);
        }

        return jsonArray;
    }

    public JSONArray getShareAssets(String datasetId, String shareId) {
        /*JSONArray shares = jsonWork.getShares(shareId, "marketplace");
        JSONObject share = (JSONObject) shares.get(0);
        JSONArray datasets = (JSONArray) share.get("datasets");
        for (Object o: datasets) {
            JSONObject dataset = (JSONObject) o;
            if ( dataset.get("id").toString().compareTo(datasetId) == 0) {
                return shareDao.getAssetsFromBucket(datasetId, dataset.get("bucket_name").toString());
            }
        }*/
        JSONObject datasetDetails = marketplaceShareService.getDatasetDetails(datasetId, shareId);
        if (!datasetDetails.isEmpty()) {
            return shareDao.getAssetsFromBucket(datasetId, datasetDetails.get("bucket").toString());
        }
        return new JSONArray();
    }

    public String exportAssetToS3(String d_id, String r_id, String a_id, String bucket_id) {
        return shareDao.exportAssetToS3(d_id, r_id, a_id, bucket_id);
    }

    public State getJobState(String id) {
        return shareDao.getJobState(id);
    }

    public List<String> listBuckets() {
        return shareDao.listBuckets();
    }

    public String autoExportToS3(String bucket, String id) {
        return shareDao.autoExportToS3(bucket, id);
    }

    public JSONArray exportAllRevisions(String id, String revisions_id, String bucket_name) throws InterruptedException {
        return shareDao.exportAllRevisions(id, revisions_id, bucket_name);
    }
    public UUID insertShares(String requestBody) throws JSONException{
        return jsonWork.insertShares(requestBody);
    }

    public Object inPlaceShares(String id) throws ParseException {
        return inplaceShareService.insertShares(id);
        //return inPlaceWork.inPlaceShare(id);
    }
    public JSONArray getShareDatasets(String id, String model) throws FileNotFoundException {
        if ( model.compareToIgnoreCase("inplace") == 0)
            return inplaceShareService.getShareDatasets(id);
        else {
            return marketplaceShareService.getShareDatasets(id);
        }
    }

    public void insertDatasetInShare(String shareId, String datasetId, String bucketName, String event_id) {
        // jsonWork.insertDatasetInShare(shareId, datasetId, bucketName);
        marketplaceShareService.insertDatasetsInShare(shareId, datasetId, bucketName, event_id);
    }


    public JSONArray getShares(String id, String shareType) {
        // JSONArray jsonArray = jsonWork.getShares(id, shareType);
        if ( shareType != null && shareType.compareToIgnoreCase("inplace") == 0) {
            return inplaceShareService.getShares(id);
        }
        return marketplaceShareService.getShares(id);
    }

    public GetSearchFacetsResult getResultByFacet(String facet, String searchText) {
        return discoveryAPI.getResultByFacet(facet, searchText);
    }

    public SearchListingsResult searchListings(String searchText, String facetType, String facetVal) {
        return discoveryAPI.searchListings(searchText, facetType, facetVal);
    }


    public Object updateShare(String shareId, String requestBody) throws ParseException, IOException {
        inplaceShareService.updateShare(shareId, requestBody);
        return "Share Updated";
        // return inPlaceWork.updateShare(shareId, requestBody);
    }
    public String addSubscriber(String shareId, String requestBody) throws IOException, ParseException {
        // return inPlaceWork.addSubscribers(shareId, requestBody);
        JSONObject subscriber = (JSONObject) new JSONParser().parse(requestBody);
        inplaceShareService.insertSubscribers(shareId, subscriber);
        return "Subscriber added successfully";
    }

    public String deleteSubscriber(String shareId, String subscriberName) throws IOException {
        // return inPlaceWork.deleteSubscriber(shareId, subscriberName);
        return inplaceShareService.deleteSubscriber(shareId, subscriberName);
    }
    public void sendInvitation(String name, JSONObject share) {
        inPlaceWork.sendInvitation(name, share);
    }

    public void removeAutoExport(String event_id) {
        shareDao.removeAutoExport(event_id);
    }
}
