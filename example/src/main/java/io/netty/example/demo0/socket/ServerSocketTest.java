package io.netty.example.demo0.socket;

import lombok.SneakyThrows;

import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class ServerSocketTest {

    public static void main(String[] args) throws Exception {
        ServerSocket serverSocket = new ServerSocket(8080);
        while (true) {
            final Socket clientSocket = serverSocket.accept();

            // 每创建一个客户端连接,都需要创建一个线程去监听客户端 Socket
            new Thread(new Runnable() {
                @SneakyThrows
                @Override
                public void run() {
                    InputStream inputStream = clientSocket.getInputStream();
                    int readLength;
                    byte[] bytes = new byte[1024];
                    while ((readLength = inputStream.read(bytes)) != -1) {
                        String msg = new String(bytes, 0, readLength);
                        System.out.println(msg);
                    }
                }
            }).start();
        }
    }

}