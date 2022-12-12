package org.data.meta.hive.model.datahub;

import lombok.Data;


/**
 * @description:
 * @author: jianyang xu
 * @createDate: 2022/11/23
 * @version: 1.0
 */

@Data
public class Aspect {
    private String value;
    final private String contentType = "application/json";
}
