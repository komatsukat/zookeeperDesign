package com.zkdesign.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static com.asuka.cons.Constants.CONN;

/**
 * 描述：zookeeper建立连接时的情况
 *
 * @author chenpeng
 * @date 2021-06-30 3:10 PM
 */
public class ZkConstructor implements Watcher {

    private static CountDownLatch connecteSemaphore = new CountDownLatch(1);

    /**
     * 如果在 connectString 加一个根节点名称，那么这个名称就被称为 Chroot，即客户端隔离命名空间。如：127.0.0.1:2183/qiurunze，qiurunze就被称为chroot。
     * sessionTimeOut：会话超时时间，在这个时间范围内，如果没有有效的心跳监测，就认为这次会话失效。
     * <p>
     * 会话的建立是异步，在初始化完客户端后，就立马返回，这时，连接可能并没有建立好，状态是 CONNECTING。
     * 当会话真正建立后，服务端会返回一个通知，客户端只有在获取到这个通知后，表明建立才真正建立好了。
     */
    public static void main(String[] args) {
        try {
            ZooKeeper client = new ZooKeeper(CONN, 5000, new ZkConstructor());
            System.out.println(client.getState()); // CONNECTING

            connecteSemaphore.await(); // 等待 1 -> 0 后，阻塞结束
            System.out.println("end");

            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("接受到 watched event：" + event);
        if (Event.KeeperState.SyncConnected == event.getState()) {
            // 能进入到这里，说明连接已经建立，结束主程序到阻塞
            System.out.println("连接已经建立...");
            connecteSemaphore.countDown();
        }
    }
}
