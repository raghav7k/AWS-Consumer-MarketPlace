package com.example.demo.dao;
// package com.marketplace.discovery.service;

import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.marketplacediscovery.AWSMarketplaceDiscovery;
import com.amazonaws.services.marketplacediscovery.AWSMarketplaceDiscoveryClientBuilder;
import com.amazonaws.services.marketplacediscovery.model.SearchListingsResult;

import org.springframework.stereotype.Repository;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.globalaccelerator.model.EndpointConfiguration;

import com.amazonaws.services.marketplacediscovery.model.Filter;
import com.amazonaws.services.marketplacediscovery.model.GetSearchFacetsRequest;
import com.amazonaws.services.marketplacediscovery.model.GetSearchFacetsResult;
import com.amazonaws.services.marketplacediscovery.model.ListingFacetSort;
import com.amazonaws.services.marketplacediscovery.model.ListingFacetSortBy;
import com.amazonaws.services.marketplacediscovery.model.ListingFacetSortOrder;
import com.amazonaws.services.marketplacediscovery.model.RequestContext;
import com.amazonaws.services.marketplacediscovery.model.SearchListingsRequest;

@Repository("Discovery")
public class DiscoveryAPI {
    private final Region region = Region.US_EAST_1;
    private final String integrationId = "integ-bfhnnm52mrvuk";

    public SearchListingsResult searchListings(String searchText, String facetType, String facetVal) {

        AwsClientBuilder.EndpointConfiguration endpointConfig = new AwsClientBuilder.EndpointConfiguration(
                "https://discovery.marketplace.us-east-1.amazonaws.com/",
                "us-east-1"
        );
        AWSMarketplaceDiscovery client = AWSMarketplaceDiscoveryClientBuilder.standard()
                .withEndpointConfiguration(endpointConfig)
                .build();

        SearchListingsRequest searchListingsRequest;
        if( facetType == null || facetVal == null) {
            searchListingsRequest = new SearchListingsRequest()
                    .withRequestContext(new RequestContext().withIntegrationId(integrationId))
                    .withSearchText(searchText);
        } else {
            Filter filter = new Filter().withType(facetType).withValues(facetVal);
            searchListingsRequest = new SearchListingsRequest().
                    withRequestContext(new RequestContext().withIntegrationId(integrationId))
                    .withSearchText(searchText)
                    .withFilters(filter);
        }

        SearchListingsResult result = client.searchListings(searchListingsRequest);

        System.out.println(result);
        return result;
    }

    public GetSearchFacetsResult getResultByFacet(String facet, String searchText) {
        AwsClientBuilder.EndpointConfiguration endpointConfig = new AwsClientBuilder.EndpointConfiguration(
                "https://discovery.marketplace.us-east-1.amazonaws.com/",
                "us-east-1"
        );

        EndpointConfiguration endpointConfiguration = EndpointConfiguration.builder().endpointId("https://discovery.marketplace.us-east-1.amazonaws.com/").build();


        GetSearchFacetsRequest getSearchFacetsRequest;
        if ( facet != null && !facet.isEmpty()) {
            getSearchFacetsRequest = new GetSearchFacetsRequest()
                    .withRequestContext(new RequestContext().withIntegrationId(integrationId))
                    .withSearchText(searchText)
                    .withSortPerFacet(
                            new ListingFacetSort()
                                    .withSortBy(ListingFacetSortBy.TOP_RESULTS)
                                    .withSortOrder(ListingFacetSortOrder.DESCENDING))
                    .withFacetTypes(facet);
        } else {
            getSearchFacetsRequest = new GetSearchFacetsRequest()
                    .withRequestContext(new RequestContext().withIntegrationId(integrationId))
                    .withSearchText(searchText);
        }
        // System.out.println(SDKGlobalConfiguration.class.getProtectionDomain().getCodeSource().getLocation());

        GetSearchFacetsResult getSearchFacetsResult = AWSMarketplaceDiscoveryClientBuilder.standard().withEndpointConfiguration(endpointConfig).build().getSearchFacets(getSearchFacetsRequest);
        System.out.println(getSearchFacetsResult);
        return getSearchFacetsResult;
    }


}
