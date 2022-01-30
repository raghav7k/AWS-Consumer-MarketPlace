package com.example.demo.api;

import com.amazonaws.services.marketplacediscovery.model.GetSearchFacetsResult;
import com.amazonaws.services.marketplacediscovery.model.ListingFacet;
import com.amazonaws.services.marketplacediscovery.model.ListingSummary;
import com.amazonaws.services.marketplacediscovery.model.SearchListingsResult;
import com.example.demo.dao.JSONWork;
import com.example.demo.service.MarketplaceShareService;
import com.example.demo.service.MyJDBC;
import com.example.demo.service.ShareService;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.web.bind.annotation.*;
import software.amazon.awssdk.services.dataexchange.model.State;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.sql.SQLException;
import java.util.Base64;
import java.util.List;
import java.util.Map;

@RequestMapping("api/v1")
@RestController
public class ShareController {

    private final ShareService shareService;
    private final MyJDBC myJDBC;
    private final JSONWork jsonWork;
    private final MarketplaceShareService marketplaceShareService;

    @Autowired
    public ShareController(ShareService shareService, MyJDBC myJDBC,
                           JSONWork jsonWork, MarketplaceShareService marketplaceShareService) {
        this.shareService = shareService;
        this.myJDBC = myJDBC;
        this.jsonWork = jsonWork;
        this.marketplaceShareService = marketplaceShareService;
    }

    @GetMapping(path = "signature")
    public String getHMAC256() {
        String signature = null;
        String signStr = "ragstorage1"+"\n"+"rwdlacupitfx"+"\n"+"bfqt"+"\n"+"sc"+"\n"+"2021-12-27T12:12:12Z"+"\n"+"2021-12-27T20:12:12Z"+ "\n"+ "\n" + "https"+"\n"+"2020-08-04"+"\n";
        try {
            SecretKeySpec secretKey = new SecretKeySpec(Base64.getDecoder().decode("Sr2FF8wH4vLWOzw8iUVqG4VsGz2jDvOVQ4EqKaYCJwPgTsRWnkaDtXaKtuXHVo/jo9bk2/oi1JoozAfFbSd7xw=="), "HmacSHA256");
            Mac sha256HMAC = Mac.getInstance("HmacSHA256");
            sha256HMAC.init(secretKey);
            signature = Base64.getEncoder().encodeToString(sha256HMAC.doFinal(signStr.getBytes("UTF-8")));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return signature;
    }

    @PostMapping(path = "share")
    public Object insertShares(@RequestBody String requestBody) throws JSONException, ParseException, CharacterCodingException {

        JSONParser jsonParser = new JSONParser();
        JSONObject jsonObject = (JSONObject) jsonParser.parse(requestBody);
        String share_type = (String) jsonObject.get("model");
        if ( share_type.compareToIgnoreCase("inplace") == 0) {
            return shareService.inPlaceShares(requestBody);
        }
        //return shareService.insertShares(requestBody);
        return marketplaceShareService.insertShares(jsonObject);
    }

    @GetMapping(path = "share")
    public JSONArray getShares(@RequestParam(value = "share_id", required = false) String id,
                               @RequestParam(value = "model", required = false) String shareType) {
        return shareService.getShares(id, shareType);
    }

    @GetMapping(path = "product/{productId}/datasets")
    public JSONArray getProductDataSets(@PathVariable("productId") String productId) throws ParseException, FileNotFoundException {
        JSONArray dataSets = shareService.getDataSets(productId);
        return dataSets;
    }

    @GetMapping(path = "share/{shareId}/datasets")
    public JSONArray getShareDataSets(@PathVariable("shareId") String shareId,
                                 @RequestParam( value = "model", required = false) String shareType) throws ParseException, FileNotFoundException {
        if ( shareType != null && shareType.compareToIgnoreCase("inplace") == 0) {
            return shareService.getShareDatasets(shareId, "inplace");
        }
        /*JSONArray jsonArray = shareService.getShares(shareId, null);
        if (!jsonArray.isEmpty()) {
            JSONObject jsonObject = (JSONObject) jsonArray.get(0);
            return (JSONArray) jsonObject.get("datasets");
        }*/
        JSONArray jsonArray = shareService.getShareDatasets(shareId, "marketplace");
        return jsonArray;
    }

    @GetMapping(path = "share/{shareId}/datasets/{datasetId}")
    public JSONObject getDatasetInfo(@PathVariable("datasetId") String datasetId)   {
        return shareService.getDatasetInfo(datasetId);
    }


    @GetMapping(path = "product/{productId}/datasets/{datasetId}/revisions")
    public JSONArray getDataSetRevision(@PathVariable("datasetId") String id) {
        return shareService.getDataSetRevision(id);
    }

    @GetMapping(path = "product/{productId}/datasets/{datasetId}/revisions/{revisionId}/assets")
    public JSONArray getAssets(@PathVariable("datasetId") String d_id, @PathVariable("revisionId") String r_id) {
        return shareService.getAssets(d_id, r_id);
    }

    @PostMapping(path = "share/{shareId}/datasets/{datasetId}/synchronization/run")
    public Object exportAllRevisions(@PathVariable("datasetId") String id,
                                     @PathVariable("shareId") String shareId,
                                     @RequestBody String requestBody,
                                     @RequestParam(value = "autoExport", required = false, defaultValue = "false")  Boolean autoExport) throws InterruptedException, ParseException {
        JSONParser jsonParser = new JSONParser();
        JSONObject body = (JSONObject) jsonParser.parse(requestBody);
        String revisionId = body.get("revisionId").toString();
        String bucket_name = body.get("bucket").toString();

        JSONArray response;

        if ( autoExport ) {
            response = shareService.exportAllRevisions(id, revisionId ,bucket_name);
            String jobId = shareService.autoExportToS3(bucket_name, id);
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("autoExportId", jobId);
            response.add((jsonObject));
            shareService.insertDatasetInShare(shareId, id, bucket_name, jobId);
        } else {
            response = shareService.exportAllRevisions(id, revisionId, bucket_name);
            shareService.insertDatasetInShare(shareId, id, bucket_name, null);
        }
        return response;
    }

    @GetMapping(path = "share/{shareId}/datasets/{datasetId}/synchronization/run/{runId}")
    public State getJobState(@PathVariable("runId") String id) {
        return shareService.getJobState(id);
    }

    @PutMapping(path = "share/{shareId}/datasets/{datasetId}/synchronization")
    public String autoExportToS3(@RequestBody String requestBody, @PathVariable("datasetId") String datasetId,
                                 @PathVariable("shareId") String shareId) throws ParseException, SQLException {
        JSONObject body = (JSONObject) new JSONParser().parse(requestBody);
        String eventId;
        if ((Boolean) body.get("autoUpdate")) {
            eventId = shareService.autoExportToS3(body.get("bucket").toString(), datasetId);
            //jsonWork.insertEventInDataset(eventId, datasetId, shareId, requestBody);
            marketplaceShareService.insertEventInDataset(eventId, datasetId, shareId, body.get("bucket").toString());
        } else {
            JSONObject datasetDetails = marketplaceShareService.getDatasetDetails(datasetId, shareId);
            eventId = datasetDetails.get("event_id").toString();
            if ( !eventId.isEmpty()) {
                shareService.removeAutoExport(eventId);
                marketplaceShareService.removeEventInDataset(datasetId, shareId);
            }
        }
        return eventId;
    }


    @GetMapping(path = "share/{shareId}/datasets/{datasetId}/assets")
    public JSONArray getShareAssets(@PathVariable("datasetId") String datasetId,
                                    @PathVariable("shareId") String shareId) {
        //we just need to give the list of downloaded assets of the dataset
        return shareService.getShareAssets(datasetId, shareId);
    }


    @GetMapping(path = "buckets")
    public List<String> listBuckets(){
        return shareService.listBuckets();
    }

    @RequestMapping(value = {"/product","/product/"}, method = RequestMethod.GET)
    public Object getCatalogListing(@RequestParam(required = false) String searchText
            , @RequestParam(required = false) String facetType
            , @RequestParam(required = false) String facetValue
            , @RequestParam(required = false) String format)
    {
        SearchListingsResult result = shareService.searchListings(searchText, facetType, facetValue);
        if (format == null)
        {
            return result.getListingSummaries();
        }

        JSONArray jsonArray = new JSONArray();
        for (ListingSummary ls : result.getListingSummaries())
        {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("title ", ls.getDisplayAttributes().getTitle());
            jsonObject.put("Short Description ", ls.getDisplayAttributes().getShortDescription());
            jsonArray.add(jsonObject);
        }

        return jsonArray;
    }

    @RequestMapping(value = "/facet", method = RequestMethod.GET)
    public Map<String, List<ListingFacet>> getCatalogListingFacet(@RequestParam(required = false) String facet, @RequestParam(required = false) String searchText)
    {
        GetSearchFacetsResult res = shareService.getResultByFacet(facet, searchText);
        System.out.println(res.getListingFacets());
        return res.getListingFacets();
    }

    @PostMapping(path = "share/{shareId}/datasets/{datasetId}/revisions/{revisionId}/assets/{assetId}/export")
    public String exportAssetToS3(@PathVariable("datasetId") String d_id, @PathVariable("revisionId") String r_id,
                                  @PathVariable("assetId") String a_id, @RequestParam(value = "bucket", required = false, defaultValue = "rag-test1") String bucket_id) {
        return shareService.exportAssetToS3(d_id, r_id, a_id, bucket_id);
    }


    // The APIS for In place share

    @PatchMapping(path = "share/{shareId}")
    public Object updateShare(@PathVariable("shareId") String shareId,
                                  @RequestBody String requestBody) throws ParseException, IOException {
        return shareService.updateShare(shareId, requestBody);
    }

    @PostMapping(path = "share/{shareId}/subscriber")
    public String addSubscriber(@PathVariable("shareId") String shareId,
                                    @RequestBody String requestBody) throws IOException, ParseException {
        return shareService.addSubscriber(shareId, requestBody);
    }

    @DeleteMapping(path = "share/{shareId}/subscriber/{subscriberName}")
    public String deleteSubscriber(@PathVariable("shareId") String shareId,
                                       @PathVariable String subscriberName) throws IOException {
        return shareService.deleteSubscriber(shareId, subscriberName);
    }

    @PostMapping(path = "share/{shareId}/subscriber/{subscriberName}/invitation")
    public JSONObject sendInvitation(@PathVariable("shareId") String shareId,
                                     @PathVariable String subscriberName) {
        JSONArray share = shareService.getShares(shareId, "inplace");
        shareService.sendInvitation(subscriberName, (JSONObject) share.get(0));
        return (JSONObject) share.get(0);
    }

    @GetMapping(path = "share/{shareId}/datasets/{datasetName}/schema")
    public JSONObject getDatasetSchema(@PathVariable("shareId") String shareId,
                                       @PathVariable("datasetName") String datasetName
    ) throws SQLException {
        // return myJDBC.getDatasetSchema(datasetName);
        return shareService.inplaceShareService.getDatasetSchema(shareId, datasetName);
    }

    @GetMapping(path = "share/{shareId}/datasets/{datasetName}/data")
    public JSONObject getDatasetData(@RequestBody String requestBody,
                                     @PathVariable("datasetName") String datasetName,
                                     @PathVariable("shareId") String shareId
    ) throws SQLException, ParseException {
        String datasetSQL = myJDBC.getDatasetSQl(shareId, datasetName);
        String sql = jsonWork.getSqlFromBody(requestBody, datasetSQL);
        return myJDBC.getDatasetData(sql);
    }
}
