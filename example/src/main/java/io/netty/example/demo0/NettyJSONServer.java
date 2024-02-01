package io.netty.example.demo0;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.json.JsonObjectDecoder;
import io.netty.handler.codec.string.StringEncoder;

public class NettyJSONServer {
    public static void main(String[] args) throws InterruptedException {
        NioEventLoopGroup boosGroup = new NioEventLoopGroup();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(boosGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
//                    .handler(new ChannelInitializer<NioServerSocketChannel>() {
//                        @Override
//                        protected void initChannel(NioServerSocketChannel ch) throws Exception {
//
//                        }
//                    })
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(final SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            // inbound
                            pipeline.addLast("1-JsonObjectDecoder", new JsonObjectDecoder());
                            pipeline.addLast("2-ByteToStringDecoder", new ByteToStringDecoder());
                            pipeline.addLast("3-SimpleChannelInboundHandler", new SimpleChannelInboundHandler<String>() {
                                @Override
                                protected void channelRead0(ChannelHandlerContext ctx, String msg) {
                                    System.out.println(msg);
                                    ctx.channel().write(msg);
                                }
                            });
                            pipeline.addLast("A-StringEncoder", new StringEncoder());
                        }
                    });

            ChannelFuture bindFuture = bootstrap.bind(8080);

            Channel channel = bindFuture.channel();
            ChannelFuture closeFuture = channel.closeFuture();
            closeFuture.sync();
        } finally {
//            boosGroup.shutdownGracefully();
//            workerGroup.shutdownGracefully();
        }
    }

}