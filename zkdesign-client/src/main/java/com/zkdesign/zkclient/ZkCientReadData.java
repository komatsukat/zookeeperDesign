package com.zkdesign.zkclient;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import static com.asuka.cons.Constants.CONN;

/**
 * 描述：读取节点数据，添加监听器监听节点数据的变更
 *
 * @author chenpeng
 * @date 2021-07-07 11:39 AM
 */
public class ZkCientReadData {

    public static void main(String[] args) throws InterruptedException {
        ZkClient zkClient = new ZkClient(CONN, 500);

        String path = "/zkClient-readData";
        zkClient.subscribeDataChanges(path, new IZkDataListener() {
            @Override
            public void handleDataChange(String s, Object o) throws Exception {
                System.out.println(s + "节点数据或者版本发生了改变，改变后的值：" + o);
            }

            @Override
            public void handleDataDeleted(String s) throws Exception {
                System.out.println(s + "节点被删除");
            }
        });

        zkClient.createEphemeral(path, "你好啊");
        Thread.sleep(100);

        zkClient.writeData(path, "我不好");
        Thread.sleep(100);

        zkClient.delete(path);
        Thread.sleep(100);
    }
}
