package org.data.meta.hive.model.datahub;

import lombok.Data;

import java.util.List;

/**
 * @description: 上游表
 * @author: jianyang xu
 * @createDate: 2022/11/23
 * @version: 1.0
 */
@Data
public class Upstreams {
    private AuditStamp AuditStamp;
    private String dataset;
    final private String type =  "TRANSFORMED";
}
