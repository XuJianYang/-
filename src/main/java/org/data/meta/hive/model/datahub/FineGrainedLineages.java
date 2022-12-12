package org.data.meta.hive.model.datahub;

import lombok.Data;

import java.util.List;

/**
 * @description:
 * @author: jianyang xu
 * @createDate: 2022/11/23
 * @version: 1.0
 */
@Data
public class FineGrainedLineages {
    private String upstreamType = "FIELD_SET";
    private List<String> upstreams ;
    private String downstreamType = "FIELD";
    private List<String> downstreams ;
    private int confidenceScore = 1;
}
