import com.alibaba.fastjson.JSONObject;
import org.apache.ibatis.session.SqlSession;
import org.data.meta.hive.model.datahub.*;
import org.data.meta.hive.model.mapper.MysqlEntityDao;
import org.data.meta.hive.util.MyBatisUtils;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @description:
 * @author: jianyang xu
 * @createDate: 2022/11/24
 * @version: 1.0
 */
public class MysqlEntityTest {
//    @Test
//    public void test(){
//        // 获取 SqlSession 对象
//        SqlSession sqlSession = MybatisUtils.getSqlSession();
//
//        // 执行 SQL 语句
//        UserDao mapper = sqlSession.getMapper(UserDao.class);
//        List<User> userList = mapper.getUser();
//
//        for (User user:userList
//        ) {
//            System.out.println(user);
//        }
//
//        // 关闭 SqlSession
//        sqlSession.close();
//    }

    @Test
    public void test(){
        // 获取 SqlSession 对象
        SqlSession sqlSession = MyBatisUtils.getSession();

        MysqlEntity mysqlEntity = new MysqlEntity();
        mysqlEntity.setEntityUrn("urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD)");
        mysqlEntity.setMsgInfo("{\"proposal\":{\"aspectName\":\"upstreamLineage\",\"entityUrn\":\"urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD)\",\"entityType\":\"dataset\",\"changeType\":\"UPSERT\",\"aspect\":{\"value\":\"{\\\"upstreams\\\":[{\\\"auditStamp\\\":{\\\"actor\\\":\\\"urn:li:corpuser:unknown\\\",\\\"time\\\":0},\\\"type\\\":\\\"TRANSFORMED\\\",\\\"dataset\\\":\\\"urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD)\\\"}],\\\"fineGrainedLineages\\\":[{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),id)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),id)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),project_no)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),project_no)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),product_no)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),product_no)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),apply_no)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),apply_no)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),user_id)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),user_id)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),user_name)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),user_name)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),loan_amount)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),loan_amount)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),total_term)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),total_term)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),apply_status)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),apply_status)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),capital_plan_no)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),capital_plan_no)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),apply_date)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),apply_date)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),end_date)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),end_date)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),repayment_day)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),repayment_day)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),loan_mouth_interest_rate)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),loan_mouth_interest_rate)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),loan_usage)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),loan_usage)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),guarantee_method)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),guarantee_method)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),repay_method)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),repay_method)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),risk_level)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),risk_level)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),version)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),version)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),created_date)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),created_date)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),last_modified_date)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),last_modified_date)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),history_flag)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),history_flag)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),apply_time)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),apply_time)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),loan_year_interest_rate)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),loan_year_interest_rate)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),remark)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),remark)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),total_fee_rate)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),total_fee_rate)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),apply_city)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),apply_city)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),pre_apply_no)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),pre_apply_no)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"},{\\\"downstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,stage.loan_apply,PROD),discount_amount)\\\"],\\\"confidenceScore\\\":1,\\\"upstreams\\\":[\\\"urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:mysql,acc_loan.loan_apply,PROD),discount_amount)\\\"],\\\"downstreamType\\\":\\\"FIELD\\\",\\\"upstreamType\\\":\\\"FIELD_SET\\\"}]}\",\"contentType\":\"application/json\"}}}");
        mysqlEntity.setCreatedTime("2022-11-24 10:35:08");
        mysqlEntity.setCreatedDate("2022-11-24");

        // 执行 SQL 语句
        MysqlEntityDao mapper = sqlSession.getMapper(MysqlEntityDao.class);
        int i = mapper.insertMysqlEntity(mysqlEntity);
        System.out.println(i);

        //提交事务到数据库
        sqlSession.commit();
        // 关闭 SqlSession
        sqlSession.close();
    }



    @Test
    public void test1(){

        AuditStamp auditStamp = new AuditStamp();
        Upstreams upstreams = new Upstreams();
        Value value = new Value();


        upstreams.setAuditStamp(auditStamp);
        upstreams.setDataset("urn:li:dataset:(urn:li:dataPlatform:hive,stage.receipt_detail,PROD)");

        List<Upstreams> upstreamsArrayList = new ArrayList<>();
        upstreamsArrayList.add(upstreams);

        value.setUpstreams(upstreamsArrayList);

        FineGrainedLineages fineGrainedLineages = new FineGrainedLineages();
        ArrayList<String> upstreams1 = new ArrayList<>();
        upstreams1.add("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,ods.loan_info,PROD),due_bill_no)");

        ArrayList<String> downstreams = new ArrayList<>();

        downstreams.add("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,default.tmp,PROD),due_bill_no)");

        fineGrainedLineages.setUpstreams(upstreams1);
        fineGrainedLineages.setDownstreams(downstreams);

        ArrayList<FineGrainedLineages> fineGrainedLineagesList = new ArrayList<>();
        fineGrainedLineagesList.add(fineGrainedLineages);

        value.setFineGrainedLineages(fineGrainedLineagesList);


        final Aspect aspect = new Aspect();
        aspect.setValue(JSONObject.toJSONString(value));


        final Proposal proposal = new Proposal();
        proposal.setEntityType("dataset");
        proposal.setChangeType("UPSERT");
        proposal.setAspectName("upstreamLineage");
        proposal.setEntityUrn("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,ods.loan_info,PROD)");
        proposal.setAspect(aspect);



        MsgInfo msgInfo = new MsgInfo();
        msgInfo.setProposal(proposal);

        final MysqlEntity mysqlEntity = new MysqlEntity();
        mysqlEntity.setEntityUrn("urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,ods.loan_info,PROD)");
        mysqlEntity.setMsgInfo(JSONObject.toJSONString(msgInfo));

        final Date date = new Date();
        SimpleDateFormat DateFormat = new SimpleDateFormat("YYYY-MM-dd");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        mysqlEntity.setCreatedDate(DateFormat.format(date));
        mysqlEntity.setCreatedTime(simpleDateFormat.format(date));

        final SqlSession sqlSession = MyBatisUtils.getSession();
        MysqlEntityDao mapper = sqlSession.getMapper(MysqlEntityDao.class);
        mapper.insertMysqlEntity(mysqlEntity);
        //提交事务到数据库
        sqlSession.commit();
        // 关闭 SqlSession
        sqlSession.close();

        System.out.println(mysqlEntity.toString());

    }


    @Test
    public void test2(){

        ArrayList<AuditStamp> auditStamps = new ArrayList<>();
        AuditStamp auditStamp = new AuditStamp();
        auditStamps.add(auditStamp);
        auditStamps.add(auditStamp);

        System.out.println(JSONObject.toJSONString(auditStamps));
    }

}
