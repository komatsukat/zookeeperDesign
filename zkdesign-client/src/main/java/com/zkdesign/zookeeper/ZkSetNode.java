package com.zkdesign.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.asuka.cons.Constants.CONN;

/**
 * 描述：节点更新操作
 *
 * @author chenpeng
 * @date 2021-06-30 5:04 PM
 */
public class ZkSetNode implements Watcher {
    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private static ZooKeeper zooKeeper;
    private static String path = "/test";

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        zooKeeper = new ZooKeeper(CONN, 5000, new ZkSetNode());
        countDownLatch.await();

        System.out.println("==========================");
        zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create("/test/t1", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        // true 表示注册了一个watcher，一旦 /test 的子节点发生改变时，就是调用 watcher，返回一个通知，
        // 注意：仅仅是通知事件发生了，不包含事件内容，需要我们再自己获取发生的事件内容
        List<String> children = zooKeeper.getChildren(path, true);
        System.out.println(children);

        zooKeeper.create("/test/t2", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        Thread.sleep(Integer.MAX_VALUE);

    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
        if (Event.KeeperState.SyncConnected == event.getState()) {
            if (Event.EventType.None == event.getType() && null == event.getPath()) {
                System.out.println("连接已经建立。");
                countDownLatch.countDown(); // 和服务端建立连接时的回调
            } else if (Event.EventType.NodeChildrenChanged == event.getType()) {
                System.out.println("子节点变更通知。");
                try {
                    System.out.println("child node changed: " + zooKeeper.getChildren(path, true));
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
