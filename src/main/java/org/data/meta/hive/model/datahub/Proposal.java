package org.data.meta.hive.model.datahub;

import lombok.Data;

/**
 * @description:
 * @author: jianyang xu
 * @createDate: 2022/11/23
 * @version: 1.0
 */
@Data
public class Proposal {
    private String entityType;
    private String entityUrn;
    private String changeType;
    private String aspectName;
    private Aspect aspect;
}
