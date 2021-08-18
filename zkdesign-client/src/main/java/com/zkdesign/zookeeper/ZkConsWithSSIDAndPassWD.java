package com.zkdesign.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static com.asuka.cons.Constants.CONN;

/**
 * 描述：通过 sessionId 和 passwd 来创建一个连接，目的是复用会话
 *
 * @author chenpeng
 * @date 2021-06-30 3:25 PM
 */
public class ZkConsWithSSIDAndPassWD implements Watcher {

    private static CountDownLatch cdl = new CountDownLatch(1);

    public static void main(String[] args) {
        try {
            // WatchedEvent state:SyncConnected type:None path:null
            ZooKeeper client = new ZooKeeper(CONN, 5000, new ZkConsWithSSIDAndPassWD());
            cdl.await();

            long sessionId = client.getSessionId();
            byte[] sessionPasswd = client.getSessionPasswd();

            cdl = new CountDownLatch(1);
            // WatchedEvent state:Expired type:None path:null
            client = new ZooKeeper(CONN, 5000, new ZkConsWithSSIDAndPassWD(), 1L, "hh".getBytes());
            cdl.await();

            cdl = new CountDownLatch(1);
            // WatchedEvent state:SyncConnected type:None path:null
            client = new ZooKeeper(CONN, 5000, new ZkConsWithSSIDAndPassWD(), sessionId, sessionPasswd);
            cdl.await();

            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("接收到 watched Event" + event);
        cdl.countDown(); // 这里必须放在 if 外面，因为使用错误的 ssid 和 passwd 来创建连接时，状态并不是 syncConnected。
        if (Event.KeeperState.SyncConnected == event.getState()) {
            System.out.println("连接已经建立");
        }
    }
}
