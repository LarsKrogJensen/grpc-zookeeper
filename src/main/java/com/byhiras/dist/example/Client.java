package com.byhiras.dist.example;

import com.byhiras.dist.common.Health;
import com.byhiras.dist.common.PingPongGrpc;
import com.byhiras.dist.discovery.ZookeeperZoneAwareNameResolverProvider;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.util.RoundRobinLoadBalancerFactory;

import java.io.IOException;


public class Client {

    public static void main(String[] args) throws IOException {

        ManagedChannel channel =
                ManagedChannelBuilder.forTarget("zk://demo")
                                     .nameResolverFactory(ZookeeperZoneAwareNameResolverProvider.newBuilder()
                                                                                                .setZookeeperAddress("localhost:2181")
                                                                                                .build())
                                     .usePlaintext(true)
                                     .loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance())
                                     .build();
        PingPongGrpc.PingPongBlockingStub stub = PingPongGrpc.newBlockingStub(channel);

        while (true) {
            for (int i = 0; i < 100; i++) {
                Health.Pong pong = stub.pingit(Health.Ping.newBuilder().build());
                System.out.println(pong);
            }
            System.in.read();
        }
        //channel.shutdown();
    }
}
