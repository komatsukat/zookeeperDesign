package com.zkdesign.zookeeper;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 邱润泽 bullock
 */
@Slf4j
public class DataTest {
    private static final String LOG_PRE = "==============> ";
    private static final String NODE_NAME = "/qiurunze";
    private ZooKeeper zooKeeper;

    @Before
    public void init() throws IOException {
        String conn = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"; // 集群

        zooKeeper = new ZooKeeper(conn, 100000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(LOG_PRE + JSON.toJSONString(watchedEvent));
                System.out.println(LOG_PRE + "zookepper客户端已建立连接");
            }
        });
    }

    /**
     * 超级权限的密码加密
     */
    @Test
    public void generateDigest() {
        try {
            System.out.println(DigestAuthenticationProvider.generateDigest("super:superpw"));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getData() throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData(NODE_NAME, false, null);
        System.out.println(LOG_PRE + new String(data));
    }

    /**
     * 使用zookeeper自带的监视器回调，而不是注册 Watcher，当监听当zNode发生改变时，会打印相关消息.
     * 注意：只会触发一次
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getDataWatch() throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData(NODE_NAME, true, null);
        System.out.println(LOG_PRE + new String(data));
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 一直监听节点的变化
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getDataWatchKeepLive() throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        zooKeeper.getData(NODE_NAME, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    zooKeeper.getData(watchedEvent.getPath(), this, null);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(LOG_PRE + JSON.toJSONString(watchedEvent));
            }
        }, stat);

        // 此时，stat就是包含了 NODE_NAME 节点的所有信息
        System.out.println(LOG_PRE + "stat: " + JSON.toJSONString(stat));
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 获取子节点名
     * 如 /qiurunze/aa /qiurunze/bb
     * 获取 /qiurunze 的子节点返回 aa bb
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getChild() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(NODE_NAME, false);
        children.stream().forEach(System.out::println);
    }

    /**
     * 监听节点的变化，只会回调一次
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getData4() throws KeeperException, InterruptedException {
        zooKeeper.getData("/qiurunze", false, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                log.info("stat:{}", JSON.toJSON(stat));
            }
        }, "");
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 创建节点，并给节点添加一些访问权限
     * 认证模式 scheme ：
     * world：任何人都可以访问，默认。
     * ip：限定客户端IP访问。
     * auth：用户密码访问模式，只有在会话中加了认证才可以访问。
     * digest：和 auth 类似，区别在于 auth 采用明文密码，digest 采用 sha - 1 + base64 加密后的方式。
     * <p>
     * 权限位 permission：
     * c: create 创建节点权限
     * r: read 读取节点权限
     * d: delete 删除节点权限
     * w: write 写节点权限
     * a: admin 可以设置节点访问控制列表权限
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void createData() throws KeeperException, InterruptedException {
        List<ACL> list = new ArrayList<>();
        int perm = ZooDefs.Perms.ADMIN | ZooDefs.Perms.READ;
        ACL acl = new ACL(perm, new Id("world", "anyone"));
//        ACL acl2 = new ACL(perm, new Id("ip", "192.168.0.149"));
//        ACL acl3 = new ACL(perm, new Id("ip", "127.0.0.1"));
        list.add(acl);
//        list.add(acl2);
//        list.add(acl3);
        zooKeeper.create(NODE_NAME + "/gekkq", "hello".getBytes(), list, CreateMode.PERSISTENT);
    }

    /**
     * 读取子节点的值
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getChild2() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren("/qiurunze", event -> {
            System.out.println(event.getPath());
            try {
                zooKeeper.getChildren(event.getPath(), false);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        children.stream().forEach(System.out::println);
        Thread.sleep(Long.MAX_VALUE);
    }

}
