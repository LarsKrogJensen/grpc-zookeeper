package se.lars.grpc.example;

import se.lars.grpc.discovery.ServiceDiscovery;
import com.google.common.collect.Lists;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import se.lars.proto.Health;
import se.lars.proto.PingPongGrpc;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;


public class Server {

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        io.grpc.Server server1 =
                ServerBuilder.forPort(port)
                             .addService(new PingPongGrpc.PingPongImplBase() {
                                 @Override
                                 public void pingit(Health.Ping request, StreamObserver<Health.Pong> responseObserver) {
                                     //System.out.println("pong at server at port: " + port);
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

    private static String resvoleAdress() {

        return unchecked(Server::getAllLocalIPs)
                .stream()
                .filter(Objects::nonNull)
                .map((inetAddress) -> {
                    String hostName = inetAddress.getCanonicalHostName();
                    return hostName.endsWith(".local") ? inetAddress.getHostAddress() : hostName;
                })
                .findFirst()
                .map(s -> s)
                .orElse("localhost");


    }

    public static Collection<InetAddress> getAllLocalIPs() throws SocketException {
        List<InetAddress> listAdr = Lists.newArrayList();
        Enumeration<NetworkInterface> nifs = NetworkInterface.getNetworkInterfaces();
        if (nifs == null) return listAdr;

        while (nifs.hasMoreElements()) {
            NetworkInterface nif = nifs.nextElement();
            // We ignore subinterfaces - as not yet needed.

            Enumeration<InetAddress> adrs = nif.getInetAddresses();
            while (adrs.hasMoreElements()) {
                InetAddress adr = adrs.nextElement();
                if (localIpFilter.get().use(nif, adr)) {
                    listAdr.add(adr);
                }
            }
        }
        return listAdr;
    }

    private static final AtomicReference<LocalIpFilter> localIpFilter =
            new AtomicReference<>
                    (
                            (nif, adr) -> (adr != null) && !adr.isLoopbackAddress() && (nif.isPointToPoint() || !adr.isLinkLocalAddress())
                    );

    public interface LocalIpFilter {
        boolean use(NetworkInterface networkInterface, InetAddress address) throws SocketException;
    }

    public interface ExceptionalSupplier<T> {
        T supply() throws Exception;
    }

    public static <T> T unchecked(ExceptionalSupplier<T> supplier) {
        try {
            return supplier.supply();
        } catch (Error | RuntimeException rex) {
            throw rex;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
