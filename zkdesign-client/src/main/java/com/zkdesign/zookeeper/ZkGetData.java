package com.zkdesign.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

import static com.asuka.cons.Constants.CONN;

/**
 * 描述：获取节点值
 *
 * @author chenpeng
 * @date 2021-07-01 9:53 AM
 */
public class ZkGetData implements Watcher {
    private static CountDownLatch countDownLatch = new CountDownLatch(1);
    private static ZooKeeper zooKeeper;
    private static String path = "/zk_book";
    private static String childPath = path + "/zk1";
    private static Stat stat = new Stat();

    public static void main(String[] args) throws Exception {
        zooKeeper = new ZooKeeper(CONN, 5000, new ZkGetData());
        countDownLatch.await();

        // 创建根节点以及根节点的子节点。。
        zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create(childPath, "我是zk1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        /**
         * 设置 watch = true 表示启用默认的 watcher 监听该节点数据的变化，当该节点的值被改变时，就会通过该 watcher 通知。
         * 将 stat 透传，最终会被 zk 服务端返回的 stat 覆盖掉，所以 stat 就能拿到服务端节点的各种属性状态
         */
        byte[] data = zooKeeper.getData(childPath, true, stat);
        System.out.println("获取到子节点数据：" + new String(data));
        System.out.println(stat.getCtime() + "," + stat.getMtime() + "," + stat.getVersion());

        // 这里改变了节点的值，会通过默认的 watcher 通知节点值改变了。
        zooKeeper.setData(childPath, "我变成a了".getBytes(), -1);

        // 注意：测试的时候，这里一定要添加这句阻塞，不然还没等 process() 执行完，主程序就结束了，process 剩余的代码没执行完
        Thread.sleep(Integer.MAX_VALUE);
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
        if (Event.KeeperState.SyncConnected == event.getState()) {
            System.out.println("-----------");
            if (Event.EventType.None == event.getType() && null == event.getPath()) {
                System.out.println("连接已经建立了。。");
                countDownLatch.countDown();
            } else if (Event.EventType.NodeDataChanged == event.getType()) {
                System.out.println("---=========");
                try {
                    // 服务端只会调用之前注册的 watcher 发送通知，而且只发送一次，具体的内容，需要我们自己获取
                    System.out.println(new String(zooKeeper.getData(event.getPath(), true, stat)));
                    System.out.println(stat.getCtime() + "," + stat.getMtime() + "," + stat.getVersion());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
