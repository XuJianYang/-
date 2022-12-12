package org.data.meta.hive.model.datahub;

import lombok.Data;
import org.joda.time.DateTime;

import java.sql.Date;

/**
 * @description:
 * @author: jianyang xu
 * @createDate: 2022/11/23
 * @version: 1.0
 */
@Data
public class MysqlEntity {
    private String entityUrn;
    final private String aspectName = "upstreamLineage";

    //报文信息
    private String msgInfo;
    private String createdDate ;
    private String createdTime;
    final private String createdBy = "kevin.xu";

}
