package com.zkdesign.zookeeper;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Before;
import org.junit.Test;


/*** @author 邱润泽 bulloc*/
@Slf4j
public class ZkclientTest {

    private ZkClient zkClient;

    @Before
    public void init() {
        zkClient = new ZkClient("127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183", 5000, 5000);
    }

    /**
     * 先执行该方法创建节点，然后执行 DataTest 中的测试方法
     *
     * @throws InterruptedException
     */
    @Test
    public void createTest() throws InterruptedException {

        zkClient.subscribeDataChanges("/qiurunze", new IZkDataListener() {
            @Override
            public void handleDataChange(String s, Object o) throws Exception {
                log.info(s);
                log.info(JSON.toJSONString(o));
            }

            @Override
            public void handleDataDeleted(String s) throws Exception {
                log.info(s);
            }
        });

        Thread.sleep(100000);
    }
}
