package se.lars.grpc.discovery;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import io.grpc.Attributes;
import io.grpc.NameResolver;
import io.grpc.ResolvedServerInfo;

import com.google.common.base.Throwables;
import io.grpc.ResolvedServerInfoGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Author stefanofranz
 */
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
            List<ResolvedServerInfoGroup> initialServers = convertToResolvedServers(initialDiscovery);
            listener.onUpdate(initialServers, Attributes.EMPTY);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

        try {
            serviceDiscovery.watchForUpdates(serviceName, updatedList -> {
                logDiscoveredNodes(updatedList);
                List<ResolvedServerInfoGroup> resolvedServers = convertToResolvedServers(updatedList);
                listener.onUpdate(resolvedServers, Attributes.EMPTY);
            });
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

    }

    private void logDiscoveredNodes(List<ServiceDiscovery.HostandZone> nodes) {
       log.info("Discovered nodes: {}",
               nodes.stream().map(ServiceDiscovery.HostandZone::toString).collect(Collectors.joining(", ")));
    }

    private List<ResolvedServerInfoGroup> convertToResolvedServers(List<ServiceDiscovery.HostandZone> newList) {
        return newList.stream()
                      .sorted(zoneComparator)
                      .map(hostandZone -> {
                          try {
                              URI hostURI = hostandZone.getHostURI();
                              InetAddress[] allByName = InetAddress.getAllByName(hostURI.getHost());
                              ResolvedServerInfoGroup.Builder builder = ResolvedServerInfoGroup.builder();
                              for (InetAddress inetAddress : allByName) {
                                  InetSocketAddress address = new InetSocketAddress(inetAddress, hostURI.getPort());
                                  Attributes attributes = Attributes.newBuilder()
                                                                    .set(Attributes.Key.of(ZONE_KEY), hostandZone.getZone())
                                                                    .build();
                                  builder.add(new ResolvedServerInfo(address, attributes));
                              }

                              return builder.build();
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
