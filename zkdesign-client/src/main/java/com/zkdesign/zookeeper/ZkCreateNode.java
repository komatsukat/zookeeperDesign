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
 * 描述：创建节点
 *
 * @author chenpeng
 * @date 2021-06-30 3:52 PM
 */
public class ZkCreateNode {
    private static CountDownLatch cdl = new CountDownLatch(1);

    public static void main(String[] args) {

        try {
            ZooKeeper zooKeeper = new ZooKeeper(CONN, 500000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println(event);
                    cdl.countDown();
                }
            });
            cdl.await();

            // 注意：每执行一次create，连接都会被关闭。
            /*cdl = new CountDownLatch(1);
            // 异步方式创建对象，所有的异常会在回调方法中通过 rc(result code) 来体现。
            zooKeeper.create(
                    "/test",  // 要创建的节点
                    "hello world".getBytes(), // 节点的值，是个字节数组，非字符串的值需要手动进行系列化和反序列化
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, // 对该节点不进行权限控制，也就是权限是 world:anyone:crdwa
                    CreateMode.EPHEMERAL, // 节点类型：持久节点，持久顺序节点，临时节点，临时顺序节点
                    new AsyncCallback.Create2Callback() { // 创建完的回调
                        @Override
                        public void processResult(int rc, String path, Object ctx, String name, Stat stat) {
                            System.out.println("=======>> >> rc: " + rc); // 0:接口调用成功，-4：连接已断开 -110：节点已存在 -120：会话已过期
                            System.out.println("=======>> >> path: " + path);
                            System.out.println("=======>> >> ctx: " + ctx);
                            System.out.println("=======>> >> name: " + name); // 实际在服务端创建的节点名称
                            System.out.println("=======>> >> stat: " + stat);
                            cdl.countDown();
                        }
                    },
                    "我是我" // 透传的对象，可以在回调的时候使用，通常是一个上下文 content
            );
            cdl.await();*/
            System.out.println("----------------------------");

            // 创建同步节点，异常抛出。
            cdl = new CountDownLatch(1);
            System.out.println("等待创建临时节点中。。。。");
            String path = zooKeeper.create("/test", "你好".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            //cdl.await();
            System.out.println("临时顺序节点：" + path);

            //zooKeeper.close(); // WatchedEvent state:Closed type:None path:null
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
