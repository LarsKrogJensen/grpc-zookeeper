package se.lars.grpc.healthchecking;

import io.grpc.stub.StreamObserver;
import se.lars.proto.Health;
import se.lars.proto.PingPongGrpc;


/**
 * Author stefanofranz
 */
public class PingPongServiceImpl extends PingPongGrpc.PingPongImplBase {


    @Override
    public void pingit(Health.Ping request, StreamObserver<Health.Pong> responseObserver) {
        responseObserver.onNext(Health.Pong.newBuilder().build());
        responseObserver.onCompleted();
    }
}
