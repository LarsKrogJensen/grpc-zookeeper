package com.byhiras.dist.example;

import com.byhiras.dist.common.Health;
import com.byhiras.dist.common.PingPongGrpc;
import com.byhiras.dist.discovery.ServiceDiscovery;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.net.URI;


public class Server {

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        io.grpc.Server server1 =
                ServerBuilder.forPort(port)
                             .addService(new PingPongGrpc.PingPongImplBase() {
                                 @Override
                                 public void pingit(Health.Ping request, StreamObserver<Health.Pong> responseObserver) {
                                     System.out.println("pong at server at port: " + port);
                                     responseObserver.onNext(Health.Pong.newBuilder().setMsg("Pong from server at port: " +port).build());
                                     responseObserver.onCompleted();
                                 }
                             }).build();
        server1.start();

        ServiceDiscovery serviceDiscovery = new ServiceDiscovery("localhost:2181");
        serviceDiscovery.registerService("demo", URI.create("dns://localhost:" + server1.getPort()));

        System.in.read();
        serviceDiscovery.deregister("demo", URI.create("dns://localhost:" + server1.getPort()));
        serviceDiscovery.close();
    }
}
