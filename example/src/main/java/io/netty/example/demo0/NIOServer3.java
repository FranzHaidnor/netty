package io.netty.example.demo0;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Set;

@Slf4j
public class NIOServer3 {

    @SneakyThrows
    public static void main(String[] args) {
        // 用于处理服务器端请求的选择器
        final Selector serverSelector = Selector.open();
        // 用于处理客户端数据读写的选择器
        final Selector clientSelector = Selector.open();

        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.socket().bind(new InetSocketAddress(8080));
        serverChannel.configureBlocking(false);
        serverChannel.register(serverSelector, SelectionKey.OP_ACCEPT);

        // 此线程处理客户端连接
        new Thread(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                while (true) {
                    if (serverSelector.select(1) > 1) {

                    }
                    serverSelector.select();
                    Set<SelectionKey> selectionKeys = serverSelector.selectedKeys();
                    Iterator<SelectionKey> keyIterator = selectionKeys.iterator();
                    while (keyIterator.hasNext()) {
                        SelectionKey key = keyIterator.next();
                        try {
                            if (key.isAcceptable()) {
                                ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
                                SocketChannel clientChannel = serverChannel.accept();
                                clientChannel.configureBlocking(false);
                                // 当执行到下面这行代码的时候,两个线程都会死锁住了
                                clientChannel.register(clientSelector, SelectionKey.OP_READ);
                                System.out.println("客户端注册");
                            }
                        } finally {
                            keyIterator.remove();
                        }
                    }
                }
            }
        }).start();

        // 此线程处理客户端读写
        new Thread(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                while (true) {
                    clientSelector.select();
                    Set<SelectionKey> selectionKeys = clientSelector.selectedKeys();
                    Iterator<SelectionKey> keyIterator = selectionKeys.iterator();
                    while (keyIterator.hasNext()) {
                        SelectionKey key = keyIterator.next();
                        try {
                            // 客户端 SocketChannel 的读取数据事件
                            if (key.isReadable()) {
                                SocketChannel clientChannel = (SocketChannel) key.channel();
                                ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
                                clientChannel.read(byteBuffer);
                                byteBuffer.flip();
                                log.info("接收到客户端消息: {}", Charset.defaultCharset().newDecoder().decode(byteBuffer));
                            }
                        } finally {
                            keyIterator.remove();
                            key.interestOps(SelectionKey.OP_READ);
                        }
                    }
                }
            }
        }).start();
    }

}
