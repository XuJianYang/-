package org.data.meta.hive.model.mapper;

import org.data.meta.hive.model.datahub.MysqlEntity;

import java.util.List;

/**
 * @description:
 * @author: jianyang xu
 * @createDate: 2022/11/24
 * @version: 1.0
 */
public interface MysqlEntityDao {
    int insertMysqlEntity(MysqlEntity mysqlEntity);

    List<MysqlEntity> getMysqlEntity();
}
