package io.netty.example.demo0.socket;

import lombok.SneakyThrows;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;

public class ServerSocketChannelTest {
    @SneakyThrows
    public static void main(String[] args) {
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(true);
        // 获取 ServerSocketChannel 中的 ServerSocket
        ServerSocket serverSocket = serverChannel.socket();
        serverSocket.bind(new InetSocketAddress(8080));

        // 阻塞,等待获取客户端连接
        Socket clientSocket = serverSocket.accept();
    }
}
