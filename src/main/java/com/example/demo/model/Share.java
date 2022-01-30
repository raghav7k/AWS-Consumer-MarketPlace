package com.example.demo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.UUID;

public class Share {
    private final UUID share_id;
    private final List<String> dataSets;

    public Share(@JsonProperty("share_id") UUID share_id,
                 @JsonProperty("datasets") List datasets) {
        this.share_id = share_id;
        this.dataSets = datasets;
    }

    public List<String> getShareDataSets() {
        return dataSets;
    }

    public UUID getShare_id() {
        return share_id;
    }
}
