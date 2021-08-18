package com.zkdesign.zkclient;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;

import java.util.List;

import static com.asuka.cons.Constants.CONN;

/**
 * 描述：使用ZkClient的Listener
 *
 * @author chenpeng
 * @date 2021-07-07 11:19 AM
 */
public class ZkClientListener {

    public static void main(String[] args) throws InterruptedException {
        ZkClient zkClient = new ZkClient(CONN, 5000);
        String path = "/zkClient-listener";
        // 注册一个监听器，可以一直有效，不像zookeeper原生的watcher是一次性的，可以对不存在的节点进行监听
        zkClient.subscribeChildChanges(path, new IZkChildListener() {
            @Override
            public void handleChildChange(String parentPath, List<String> childsPath) throws Exception {
                System.out.println(parentPath + "'s child changed, current childsPath: " + childsPath);
            }
        });

        zkClient.createPersistent(path);
        Thread.sleep(100);

        // 创建子节点
        zkClient.createEphemeral(path + "/c1");
        Thread.sleep(100);

        zkClient.delete(path + "/c1");
        Thread.sleep(100);

        zkClient.delete(path);
        Thread.sleep(Integer.MAX_VALUE);
    }
}
