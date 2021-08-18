package com.zkdesign.zookeeper;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;

import java.io.IOException;

/**
 * @author qiurunze
 **/
public class Client {
    private UserService service;

    public static void main(String[] args) throws IOException {
        Client client1 = new Client();
        client1.buildService("");

        // 用户ID
        String cmd;
        while (!(cmd = read()).equals("exit")) {
            UserVo u = client1.service.getUser(Integer.parseInt(cmd));
            System.out.println(u);
        }
    }

    private static String read() throws IOException {
        byte[] b = new byte[1024];
        int size = System.in.read(b);
        return new String(b, 0, size).trim();
    }

    // URL 远程服务的调用地址
    private UserService buildService(String url) {
        ApplicationConfig config = new ApplicationConfig("young-app"); // 消费方服务名
        // 构建一个引用对象
        ReferenceConfig<UserService> referenceConfig = new ReferenceConfig<>();
        referenceConfig.setApplication(config);
        referenceConfig.setInterface(UserService.class); // 调用的远程接口
        // referenceConfig.setUrl(url);
        referenceConfig.setRegistry(new RegistryConfig("zookeeper://127.0.0.1:2181"));
        referenceConfig.setTimeout(5000);

        // 透明化
        this.service = referenceConfig.get();
        return service;
    }
}
