package com.zkdesign.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import static com.asuka.cons.Constants.CONN;

/**
 * 描述：fluent(流式风格)创建连接
 *
 * @author chenpeng
 * @date 2021-07-07 2:09 PM
 */
public class CreateConnnectFulent {
    public static void main(String[] args) throws InterruptedException {

        ExponentialBackoffRetry retryPolixy = new ExponentialBackoffRetry(1000, 2);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(CONN)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolixy)
                .namespace("base") // 命名空间，该连接下的操作都将是对该节点及其子节点的操作
                .build();
        client.start();

        Thread.sleep(2000);
    }
}
