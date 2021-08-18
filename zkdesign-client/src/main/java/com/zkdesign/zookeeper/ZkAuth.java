package com.zkdesign.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static com.asuka.cons.Constants.CONN;

/**
 * 描述：节点权限操作
 *
 * @author chenpeng
 * @date 2021-07-07 9:40 AM
 */
public class ZkAuth implements Watcher {
    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zooKeeper = new ZooKeeper(CONN, 5000, new ZkAuth());
        countDownLatch.await();

        String path = "/zk-book_auto_digest";
        String childPath = path + "/ch";

        // 这种权限方式类似 username:password
        zooKeeper.addAuthInfo("digest", "foo:true".getBytes());

        zooKeeper.create(path, "很好".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
        zooKeeper.create(childPath, "".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.EPHEMERAL);

        byte[] data = zooKeeper.getData(path, false, null);
        System.out.println(new String(data));

        // 再创建一个连接，来获取path的值
        countDownLatch = new CountDownLatch(1);
        ZooKeeper zooKeeper1 = new ZooKeeper(CONN, 6000, new ZkAuth());
        countDownLatch.await();

        try {
            // 没有权限，获取节点数据，失败
            byte[] data1 = zooKeeper1.getData(path, false, null); // KeeperErrorCode = NoAuth for /zk-book_auto_digest
            System.out.println(new String(data1));
        } catch (Exception e) {
            System.out.println("无权限获取不到节点数据。" + e.getMessage());
        }

        countDownLatch = new CountDownLatch(1);
        ZooKeeper zooKeeper2 = new ZooKeeper(CONN, 7000, new ZkAuth());
        countDownLatch.await();

        // 添加和上面一样的权限，然后获取数据
        zooKeeper2.addAuthInfo("digest", "foo:true".getBytes());
        byte[] data2 = zooKeeper2.getData(path, false, null);
        System.out.println("有权限后可以获取到节点数据：" + new String(data2));


        // 权限删除有些特殊，对加了权限对节点，没有权限的客户端依然可以删除该节点，但是不可以删除子节点。
        countDownLatch = new CountDownLatch(1);
        ZooKeeper zooKeeper3 = new ZooKeeper(CONN, 5000, new ZkAuth());
        countDownLatch.await();

        try {
            // 没权限，不能删除子节点
            zooKeeper3.delete(childPath, -1);
        } catch (Exception e) {
            System.out.println("删除节点失败，没权限删除节点的子节点：" + e.getMessage());
        }

        countDownLatch = new CountDownLatch(1);
        ZooKeeper zooKeeper4 = new ZooKeeper(CONN, 5000, new ZkAuth());
        countDownLatch.await();

        zooKeeper4.addAuthInfo("digest", "foo:true".getBytes());
        // 添加权限信息后，才能删除成功
        zooKeeper4.delete(childPath, -1);
        System.out.println("添加权限后，删除子节点成功。");

        countDownLatch = new CountDownLatch(1);
        ZooKeeper zooKeeper5 = new ZooKeeper(CONN, 5000, new ZkAuth());
        countDownLatch.await();
        // 没有添加权限，直接删除该节点，能删除成功
        zooKeeper5.delete(path, -1);
        System.out.println("不需要权限，就能删除节点");
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
        if (Event.KeeperState.SyncConnected == event.getState()) {
            if (Event.EventType.None == event.getType() && event.getPath() == null) {
                System.out.println("连接成功");
                countDownLatch.countDown();
            }
        }
    }
}
