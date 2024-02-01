package io.netty.example.demo0;

import lombok.SneakyThrows;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class NIOServer {

    @SneakyThrows
    public static void main(String[] args) {
        // 创建选择器
        final Selector selector = Selector.open();

        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.socket().bind(new InetSocketAddress(8080));
        serverChannel.configureBlocking(false);

        // 表示SelectableChannel注册到选择器的标记。 每次向选择器注册通道时，都会创建一个选择键
        final SelectionKey selectionKey = serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        new Thread(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                while (true) {
                    // select() 方法会导致阻塞
                    int select = selector.select();

                    // SelectionKey 代表 channel 通道
                    Set<SelectionKey> selectionKeys = selector.selectedKeys();
                    Iterator<SelectionKey> iterator = selectionKeys.iterator();
                    while (iterator.hasNext()) {
                        SelectionKey key = iterator.next();

                        if (key.isReadable()) {
                            ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
                            SocketChannel clientChannel = serverChannel.accept();
                            clientChannel.configureBlocking(false);
                            // socketChannel.register(selector, SelectionKey.OP_READ);
                            System.out.println("T1 register");
                        }

                        iterator.remove();
                    }
                }
            }
        }).start();
    }

}
