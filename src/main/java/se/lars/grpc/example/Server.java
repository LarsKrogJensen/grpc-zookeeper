package se.lars.grpc.example;

import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import se.lars.grpc.discovery.ServiceDiscovery;
import se.lars.proto.Health;
import se.lars.proto.PingPongGrpc;

import java.net.URI;


public class Server {

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        io.grpc.Server server1 =
                ServerBuilder.forPort(port)
                             .addService(new PingPongGrpc.PingPongImplBase() {
                                 @Override
                                 public void pingit(Health.Ping request, StreamObserver<Health.Pong> responseObserver) {
                                     responseObserver.onNext(Health.Pong.newBuilder().setMsg("Pong from server at port: " + port).build());
                                     responseObserver.onCompleted();
                                 }
                             }).build();
        server1.start();

        ServiceDiscovery serviceDiscovery = new ServiceDiscovery("localhost:2181");
        String address = "localhost"; //resvoleAdress();
        serviceDiscovery.registerService("demo", URI.create("dns://" + address + ":" + server1.getPort()));

        System.in.read();
        serviceDiscovery.deregister("demo", URI.create("dns://" + address + ":" + server1.getPort()));
        serviceDiscovery.close();
    }


}
