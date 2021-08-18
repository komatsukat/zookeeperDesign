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
 * 描述：异步回调的形式返回数据改变通知
 *
 * @author chenpeng
 * @date 2021-07-01 10:39 AM
 */
public class ZkGetDateCallBack implements Watcher {
    private static CountDownLatch countDownLatch = new CountDownLatch(1);
    private static ZooKeeper zooKeeper;
    private static Stat stat = new Stat();
    private static String path = "/zk_callback";
    private static String childPath = path + "/zk1";

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        zooKeeper = new ZooKeeper(CONN, 50000, new ZkGetDateCallBack());
        countDownLatch.await();

        zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create(childPath, "zk1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        // 异步的方式获取节点值，通过异步接口返回
        zooKeeper.getData(childPath, true, new IDataCallBack(), "我是透传的数据哦哦哦哦哦");

        /**
         * -1 表示告诉服务端，客户端需要基于数据最新的version更新数据。
         * zk是基于CAS更新数据的，也就是每次都会那一个期望值与实际去比较，如果一样就更新，不一样就不会更新
         */
        Stat stat = zooKeeper.setData(childPath, "haha".getBytes(), -1);
        System.out.println(stat.getCtime() + "," + stat.getMtime() + "," + stat.getVersion());

        // 注意：zk更新节点值后变更两样东西：数据内容和版本，如果更新的值一样，其版本还是会变化的。
        zooKeeper.setData(childPath, "haha".getBytes(), stat.getVersion()); // 版本已变

        // 用旧的版本更新一下数据试试
        try {
            zooKeeper.setData(childPath, "haha".getBytes(), stat.getVersion());
        } catch (Exception e) {
            e.printStackTrace();
        }


        Thread.sleep(Integer.MAX_VALUE);
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
        if (Event.KeeperState.SyncConnected == event.getState()) {
            if (Event.EventType.None == event.getType() && event.getPath() == null) {
                System.out.println("连接建立成功");
                countDownLatch.countDown();
            } else if (Event.EventType.NodeDataChanged == event.getType()) {
                System.out.println("节点数据变更了。。");
                try {
                    System.out.println(new String(zooKeeper.getData(childPath, true, stat)));
                    System.out.println(stat.getCtime() + ',' + stat.getMtime() + "," + stat.getVersion());
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * 异步方式返回获取的结果
     */
    static class IDataCallBack implements AsyncCallback.DataCallback {

        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            System.out.println(rc + "," + path + "," + new String(data) + "," + ctx);
            System.out.println(stat.getCtime() + "," + stat.getMtime() + "," + stat.getVersion());
        }
    }
}
