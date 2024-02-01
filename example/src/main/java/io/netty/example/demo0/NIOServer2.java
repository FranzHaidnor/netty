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
public class NIOServer2 {

    @SneakyThrows
    public static void main(String[] args) {
        final Selector selector = Selector.open();

        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.socket().bind(new InetSocketAddress(8080),1); // 参数 backlog 可以设置等待连接的队列容量
        serverChannel.configureBlocking(false);
        // 将 ServerSocketChannel 注册到选择器上
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        new Thread(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                while (true) {
                    // select() 方法会导致阻塞
                    selector.select();

                    // SelectionKey 代表 channel 通道
                    Set<SelectionKey> selectionKeys = selector.selectedKeys();
                    Iterator<SelectionKey> keyIterator = selectionKeys.iterator();
                    while (keyIterator.hasNext()) {
                        SelectionKey key = keyIterator.next();
                        try {
                            // 服务器 ServerSocketChannel 的注册事件
                            if (key.isAcceptable()) {
                                ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
                                // 使用 ServerSocketChannel 监听到连接事件
                                SocketChannel clientChannel = serverChannel.accept();
                                clientChannel.configureBlocking(false);
                                // 可客户端 SocketChannel 注册到选择器上, 并设置客户端 SocketChannel 的事件为读事件
                                clientChannel.register(selector, SelectionKey.OP_READ);
                                log.info("客户端注册");
                            }
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
                        }
                    }
                }
            }
        }).start();
    }

}
