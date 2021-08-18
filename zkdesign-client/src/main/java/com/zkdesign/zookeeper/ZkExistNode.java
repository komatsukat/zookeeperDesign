package com.zkdesign.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static com.asuka.cons.Constants.CONN;

/**
 * 描述：判断节点是否存在，并注册watcher
 *
 * @author chenpeng
 * @date 2021-07-05 2:35 PM
 */
public class ZkExistNode implements Watcher {

    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private static ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        String path = "/zk-book";
        zooKeeper = new ZooKeeper(CONN, 5000, new ZkExistNode());
        countDownLatch.await();

        // 使用默认的监视器，如果节点被创建、更新、删除，都会得到通知，但如果是添加子节点或者对子节点操作，怎么不会通知
        Stat stat = zooKeeper.exists(path, true);
        System.out.println(stat);

        zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        zooKeeper.setData(path, "a".getBytes(), -1);

        zooKeeper.create(path + "/zk", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        zooKeeper.delete(path + "/zk", -1);

        zooKeeper.delete(path, -1);

        Thread.sleep(Integer.MAX_VALUE);
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
        try {
            if (Event.KeeperState.SyncConnected == event.getState()) {
                if (Event.EventType.None == event.getType() && event.getPath() == null) {
                    System.out.println("已建立连接");
                    countDownLatch.countDown();
                } else if (Event.EventType.NodeCreated == event.getType()) {
                    System.out.println("节点 " + event.getPath() + " 被创建");
                    // 然后继续添加监听事件，继续监听，因为通知是一次性的
                    zooKeeper.exists(event.getPath(), true);
                } else if (Event.EventType.NodeDataChanged == event.getType()) {
                    System.out.println("节点 " + event.getPath() + " 值被改变了");
                    zooKeeper.exists(event.getPath(), true);
                } else if (Event.EventType.NodeDeleted == event.getType()) {
                    System.out.println("节点 " + event.getPath() + " 被删除了");
                }
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
