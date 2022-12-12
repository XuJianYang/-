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
public class Value {
    private List<Upstreams> upstreams;
    private List<FineGrainedLineages> fineGrainedLineages;
}
