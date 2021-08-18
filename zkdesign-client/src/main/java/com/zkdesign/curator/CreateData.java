package com.zkdesign.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import static com.asuka.cons.Constants.CONN;

/**
 * 描述：创建节点
 *
 * @author chenpeng
 * @date 2021-07-07 2:28 PM
 */
public class CreateData {
    public static void main(String[] args) throws Exception {
        ExponentialBackoffRetry retryPolxy = new ExponentialBackoffRetry(1000, 2);
        CuratorFramework curator = CuratorFrameworkFactory.builder()
                .connectString(CONN)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolxy)
               // .namespace("curator")
                .build();

        curator.start();

        String path = "/zk-book/zk1";

        // 创建节点
        curator.create()
                .creatingParentsIfNeeded() // 如果父节点不存在，会创建，并且为持久节点
                .withMode(CreateMode.EPHEMERAL) // 子节点可以是临时节点
                .forPath(path, "init".getBytes());

        // 读取数据
        Stat stat = new Stat();
        curator.getData().storingStatIn(stat).forPath(path);
        System.out.println(stat);

        // 删除节点
        curator.delete()
                .deletingChildrenIfNeeded()
                .withVersion(stat.getAversion()) // 强制指定版本进行删除
                .forPath(path);

        Thread.sleep(Integer.MAX_VALUE);
    }
}
