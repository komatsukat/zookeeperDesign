package com.zkdesign.zookeeper;

import org.apache.zookeeper.AsyncCallback;
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
 * 描述：异步更新数据
 *
 * @author chenpeng
 * @date 2021-07-01 11:45 AM
 */
public class ZkSetDataCallBack implements Watcher {
    private static CountDownLatch countDownLatch = new CountDownLatch(1);
    private static ZooKeeper zooKeeper;
    private static String path = "/zkSet_callback";

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        zooKeeper = new ZooKeeper(CONN, 500000, new ZkSetDataCallBack());
        countDownLatch.await();

        String dd = zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(dd);

        zooKeeper.setData(path, "hha".getBytes(), -1, new IDataCallback(), "我是透传数据");

        Thread.sleep(Integer.MAX_VALUE);
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
        if (Event.KeeperState.SyncConnected == event.getState()) {
            if (Event.EventType.None == event.getType() && event.getPath() == null) {
                System.out.println("连接已建立。。。");
                countDownLatch.countDown();
            }
        }
    }

    static class IDataCallback implements AsyncCallback.StatCallback {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            System.out.println(rc + "," + path + "," + ctx);
            System.out.println(stat.getCtime() + "," + stat.getMtime() + "," + stat.getVersion());
        }
    }

}
