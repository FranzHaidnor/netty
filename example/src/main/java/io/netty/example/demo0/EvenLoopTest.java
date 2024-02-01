package io.netty.example.demo0;

import io.netty.channel.nio.NioEventLoopGroup;

public class EvenLoopTest {
    public static void main(String[] args) {
        NioEventLoopGroup executors = new NioEventLoopGroup();
        executors.execute(new Runnable() {
            @Override
            public void run() {
                System.out.println("step 1");
            }
        });

        System.out.println(executors.executorCount());
        System.out.println("step 2");
    }
}
