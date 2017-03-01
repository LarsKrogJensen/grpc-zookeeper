package se.lars.grpc.example;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import se.lars.grpc.discovery.ZookeeperZoneAwareNameResolverProvider;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.util.RoundRobinLoadBalancerFactory;
import se.lars.grpc.retry.RetryClientInterceptor;
import se.lars.grpc.retry.Retryer;
import se.lars.proto.Health;
import se.lars.proto.PingPongGrpc;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;


public class Client {

    public static void main(String[] args) throws IOException {

        ManagedChannel channel =
//                ManagedChannelBuilder.forAddress("localhost", 8181)
                ManagedChannelBuilder.forTarget("zk://demo")
                                     .nameResolverFactory(ZookeeperZoneAwareNameResolverProvider.newBuilder()
                                                                                                .setZookeeperAddress("localhost:2181")
                                                                                                .build())
                                     .usePlaintext(true)
                                     .loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance())
                                     .build();
       PingPongGrpc.PingPongBlockingStub stub = PingPongGrpc.newBlockingStub(channel);
        //PingPongGrpc.PingPongFutureStub stub = PingPongGrpc.newFutureStub(channel);
        //stub.withInterceptors(new RetryClientInterceptor(Retryer.createDefault().maxRetries(5)));

        while (true) {
            //for (int i = 0; i < 100; i++) {
            try {
                long start = System.nanoTime();
                Health.Pong pong = withRetry(() -> stub.pingit(Health.Ping.newBuilder().build()), 5);
                System.out.print(pong + " completed in: " + (System.nanoTime() - start)/1_000 + " us.");

//                ListenableFuture<Health.Pong> pingit = stub.pingit(Health.Ping.newBuilder().build());
//                pingit.addListener(() -> {
//                    try {
//                        System.out.print(pingit.get());
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    } catch (ExecutionException e) {
//                        e.printStackTrace();
//                    }
//                }, MoreExecutors.directExecutor());
//
//                long start = System.currentTimeMillis();
//                asyncWithRetry(() -> stub.pingit(Health.Ping.newBuilder().build()), 5)
//                        .whenComplete((pong, throwable) -> {
//                            if (throwable == null)
//                                System.out.print(pong + " completed in: " + (System.currentTimeMillis() -start) + " ms.");
//                            else
//                                System.out.println("Failed: " + throwable.getMessage());
//                        });


            } catch (Exception e) {
                System.out.println("Failed, msg: " + e.getMessage());
            }
//            try {
//                Thread.sleep(200);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            }
//            System.in.read();
        }
        //channel.shutdown();
    }

    public static <T> T withRetry(Server.ExceptionalSupplier<T> supplier, int maxAttempts) {
        try {
            return supplier.supply();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.UNAVAILABLE && maxAttempts > 0) {
                System.out.println("Warning, failed to invoke");
                return withRetry(supplier, maxAttempts - 1);
            } else {
                throw e;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> CompletableFuture<T> asyncWithRetry(Supplier<ListenableFuture<T>> supplier, int maxAttempts) {
        CompletableFuture<T> stage = new CompletableFuture<T>();
        runAsyncWithRetry(supplier, maxAttempts, stage);
        return stage;
    }

    private static <T> void runAsyncWithRetry(Supplier<ListenableFuture<T>> supplier, int maxAttempts, CompletableFuture<T> stage) {
        ListenableFuture<T> future = supplier.get();
        future.addListener(() -> {
            try {
                stage.complete(future.get());
            } catch (ExecutionException e) {
                if (e.getCause() instanceof StatusRuntimeException) {
                    StatusRuntimeException se = (StatusRuntimeException) e.getCause();
                    if (se.getStatus().getCode() == Status.Code.UNAVAILABLE && maxAttempts > 0) {
                        System.out.println("Warning, failed to invoke");
                        runAsyncWithRetry(supplier, maxAttempts - 1, stage);
                    } else {
                        stage.completeExceptionally(e.getCause());
                    }
                } else {
                    stage.completeExceptionally(e.getCause());
                }
            } catch (Exception e) {
                stage.completeExceptionally(e);
            }
        }, MoreExecutors.directExecutor());

    }
}
