package org.data.meta.hive.model.datahub;

import lombok.Data;
import org.joda.time.DateTime;

/**
 * @description:
 * @author: jianyang xu
 * @createDate: 2022/11/23
 * @version: 1.0
 */
@Data
public class AuditStamp {
    final private String actor = "urn:li:corpuser:kevin.xu";
    private long time = System.currentTimeMillis();
}
