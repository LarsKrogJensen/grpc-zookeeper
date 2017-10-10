package se.lars.grpc.discovery;

import com.google.common.base.Throwables;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class ZookeeperZoneAwareNameResolver extends NameResolver {

    private static Logger log = LoggerFactory.getLogger(ZookeeperZoneAwareNameResolver.class);
    public final String ZONE_KEY = "ZONE";


    private final URI targetUri;
    private final ServiceDiscovery serviceDiscovery;
    private final Comparator<ServiceDiscovery.HostandZone> zoneComparator;

    public ZookeeperZoneAwareNameResolver(URI targetUri,
                                          ServiceDiscovery serviceDiscovery,
                                          Comparator<ServiceDiscovery.HostandZone> zoneComparator) {
        this.targetUri = targetUri;
        this.serviceDiscovery = serviceDiscovery;
        this.zoneComparator = zoneComparator;
    }


    @Override
    public String getServiceAuthority() {
        return targetUri.getAuthority();
    }

    @Override
    public void start(Listener listener) {
        //FORMAT WILL BE: zk://serviceName
        String serviceName = targetUri.getAuthority();

        try {
            List<ServiceDiscovery.HostandZone> initialDiscovery = serviceDiscovery.discover(serviceName);
            logDiscoveredNodes(initialDiscovery);
            List<EquivalentAddressGroup> initialServers = convertToResolvedServers(initialDiscovery);
            listener.onAddresses(initialServers, Attributes.EMPTY);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

        try {
            serviceDiscovery.watchForUpdates(serviceName, updatedList -> {
                logDiscoveredNodes(updatedList);
                List<EquivalentAddressGroup> resolvedServers = convertToResolvedServers(updatedList);
                listener.onAddresses(resolvedServers, Attributes.EMPTY);
            });
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

    }

    private void logDiscoveredNodes(List<ServiceDiscovery.HostandZone> nodes) {
        log.info("Discovered nodes: {}",
                 nodes.stream().map(ServiceDiscovery.HostandZone::toString).collect(Collectors.joining(", ")));
    }

    private List<EquivalentAddressGroup> convertToResolvedServers(List<ServiceDiscovery.HostandZone> newList) {
        return newList.stream()
                .sorted(zoneComparator)
                .map(hostandZone -> {
                    try {
                        URI hostURI = hostandZone.getHostURI();
                        InetAddress[] allByName = InetAddress.getAllByName(hostURI.getHost());
                        List<SocketAddress> builder = new ArrayList<>();
                        for (InetAddress inetAddress : allByName) {
                            InetSocketAddress address = new InetSocketAddress(inetAddress, hostURI.getPort());
                            builder.add(address);
                        }

                        return new EquivalentAddressGroup(builder);
                    } catch (UnknownHostException e) {
                        throw Throwables.propagate(e);
                    }
                }).collect(Collectors.toList());
    }

    @Override
    public void shutdown() {
        try {
            serviceDiscovery.close();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

}
