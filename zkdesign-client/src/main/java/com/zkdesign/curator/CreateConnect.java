package com.zkdesign.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import static com.asuka.cons.Constants.CONN;

/**
 * 描述：使用Curator和zookeeper服务器建立连接
 *
 * @author chenpeng
 * @date 2021-07-07 11:58 AM
 */
public class CreateConnect {

    public static void main(String[] args) throws InterruptedException {
        // 先定义一个重试策略
        // 计算当前需要 sleep 的时间：sleepTime = baseSleepTimeMs * Math.max(1, Math.random(1 << (retryCount + 1)))
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 2);

        CuratorFramework client = CuratorFrameworkFactory.newClient(CONN, retryPolicy); // 只是初始化
        client.start(); // 这一步才建立连接

        Thread.sleep(2000);
    }
}
