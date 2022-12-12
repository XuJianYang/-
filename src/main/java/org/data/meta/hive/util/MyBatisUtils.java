package org.data.meta.hive.util;



import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;

/**
 * @description:
 * @author: jianyang xu
 * @createDate: 2022/11/24
 * @version: 1.0
 */
public class MyBatisUtils {
    private static SqlSessionFactory sqlSessionFactory;

    static {
        try {
            String resource = "mybatis-config.xml";
            InputStream inputStream = Resources.getResourceAsStream(resource);
            sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param
     * @return
     * @description 获取 SqlSession 连接
     * @date 2020/7/14 11:46
     * @author cunyu1943
     * @version 1.0
     */
    public static SqlSession getSession() {
        return sqlSessionFactory.openSession();
    }

}
