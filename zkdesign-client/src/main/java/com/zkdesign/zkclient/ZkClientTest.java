package com.zkdesign.zkclient;

import org.I0Itec.zkclient.ZkClient;

import static com.asuka.cons.Constants.CONN;

/**
 * 描述：使用 ZkClient 来操作 zookeeper
 *
 * @author chenpeng
 * @date 2021-07-07 11:01 AM
 */
public class ZkClientTest {
    public static void main(String[] args) {

        ZkClient zkClient = new ZkClient(CONN, 5000);
        System.out.println("连接已经建立");

        // 支持递归的形式创建父节点
        String path = "/zk_client-test";
        String childPath = path + "/zk";
        zkClient.createPersistent(childPath, true);
        Object data = zkClient.readData(childPath);
        System.out.println(data);

        // 支持递归删除节点，也就是节点如果包含子节点，会递归删除完所有子节点后删除自己
        boolean deleted = zkClient.deleteRecursive(path);
        System.out.println("删除节点，包含子节点是否成功：" + deleted);
    }
}
