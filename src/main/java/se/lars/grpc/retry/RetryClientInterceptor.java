package se.lars.grpc.retry;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.Status.Code;

/**
 * Interceptor for retrying client calls.  Only UNAVAILABLE errors will be retried.  Retry is only
 * supported for method types where the client sends a single request.  For streaming responses
 * the call is not retried if at least one message was already read as this type of result has partial
 * state and should therefore be retried in application code.
 * <p>
 * Usage
 * <pre>
 * {code
 * FooBlockingStub client = FooGrpc.newBlockingStub(channel);
 * client
 * .withInterceptors(new RetryClientInterceptor(Retryer.createDefault().maxRetrys(5)))
 * .sayHello(HelloRequest.newBuilder().setName("foo").build());
 * }
 * </pre>
 */
public class RetryClientInterceptor implements ClientInterceptor {
    private final Retryer retryer;

    public RetryClientInterceptor(Retryer retryer) {
        this.retryer = retryer;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
                                                               final CallOptions callOptions,
                                                               final Channel next) {
        if (!method.getType().clientSendsOneMessage()) {
            return next.newCall(method, callOptions);
        }

        // TODO: Check if the method is immutable and retryable
        return new ReplayingSingleSendClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
            Retryer instance = retryer;

            @Override
            public void start(io.grpc.ClientCall.Listener<RespT> responseListener, Metadata headers) {
                super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
                    private boolean receivedAResponse = false;

                    public void onClose(Status status, Metadata trailers) {
                        if (status.getCode() == Code.UNAVAILABLE
                                && !receivedAResponse
                                && instance.canRetry()) {

                            instance = instance.retry(Context.current()
                                                             .wrap(() -> replay(next.newCall(method, callOptions))));
                        } else {
                            instance.cancel();
                            super.onClose(status, trailers);
                        }
                    }

                    @Override
                    public void onMessage(RespT message) {
                        receivedAResponse = true;
                        super.onMessage(message);
                    }

                }, headers);
            }

            @Override
            public void cancel(String msg, Throwable t) {
                instance.cancel();
                super.cancel(msg, t);
            }
        };
    }
}