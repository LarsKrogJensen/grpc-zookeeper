package com.byhiras.dist.discovery;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.byhiras.dist.discovery.ServiceDiscovery.HostandZone;

/**
 * Author stefanofranz
 */
public class ServiceDiscoveryTest {

    public static final String TEST_SERVICE_ID = "testService";
    TestingServer zkTestServer;
    String host;

    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServer();
        host = "localhost:" + zkTestServer.getPort();
    }

    @After
    public void stopZookeeper() throws IOException {
        zkTestServer.stop();
        zkTestServer.close();
    }

    @Test
    public void shouldStoreServiceRegistration() throws Exception {

        ServiceDiscovery serviceDiscovery1 = new ServiceDiscovery(host);

        serviceDiscovery1.removeServiceRegistry(TEST_SERVICE_ID);
        serviceDiscovery1.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere"));
        assertThat(serviceDiscovery1.discoverUnzoned(TEST_SERVICE_ID), is(equalTo(Collections.singletonList(URI.create("dns://somewhere")))));
        serviceDiscovery1.close();

    }

    @Test
    public void shouldStoreServiceRegistrationTillDisconnect() throws Exception {

        ServiceDiscovery serviceDiscovery1 = new ServiceDiscovery(host);
        serviceDiscovery1.removeServiceRegistry(TEST_SERVICE_ID);
        serviceDiscovery1.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere"));
        assertThat(serviceDiscovery1.discoverUnzoned(TEST_SERVICE_ID), is(equalTo(Collections.singletonList(URI.create("dns://somewhere")))));
        serviceDiscovery1.close();

        ServiceDiscovery serviceDiscovery2 = new ServiceDiscovery(host);
        assertThat(serviceDiscovery2.discoverUnzoned(TEST_SERVICE_ID), is(equalTo(Collections.emptyList())));
    }

    @Test
    public void shouldStoreServiceRegistrationFromMultipleServicesAndMaintainAgreement() throws Exception {

        ServiceDiscovery serviceDiscovery1 = new ServiceDiscovery(host);
        serviceDiscovery1.removeServiceRegistry(TEST_SERVICE_ID);
        serviceDiscovery1.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere1"));

        ServiceDiscovery serviceDiscovery2 = new ServiceDiscovery(host);
        serviceDiscovery2.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere2"));

        assertThat(serviceDiscovery2.discoverUnzoned(TEST_SERVICE_ID), is(equalTo(asList(URI.create("dns://somewhere2"), URI.create("dns://somewhere1")))));
        assertThat(serviceDiscovery1.discoverUnzoned(TEST_SERVICE_ID), is(equalTo(asList(URI.create("dns://somewhere2"), URI.create("dns://somewhere1")))));
    }

    @Test
    public void shouldWatchForUpdates() throws Exception {
        ReentrantLock lock = new ReentrantLock(true);
        Condition signal = lock.newCondition();
        AtomicReference<List<URI>> watchedResult = new AtomicReference<>();
        ServiceDiscovery serviceDiscovery1 = new ServiceDiscovery(host);
        serviceDiscovery1.removeServiceRegistry(TEST_SERVICE_ID);

        serviceDiscovery1.watchForUpdates(TEST_SERVICE_ID, newList -> {
            watchedResult.set(newList.stream().map(HostandZone::getHostURI).collect(Collectors.toList()));
            lock.lock();
            signal.signalAll();
            lock.unlock();
        });

        ServiceDiscovery serviceDiscovery2 = new ServiceDiscovery(host);
        ServiceDiscovery serviceDiscovery3 = new ServiceDiscovery(host);
        //REGISTER SERVICE 2
        serviceDiscovery2.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere2"));
        waitForConditionSignal(lock, signal);
        assertThat(watchedResult.get(), is(equalTo(Collections.singletonList(URI.create("dns://somewhere2")))));
        //REGISTER SERVICE 3
        serviceDiscovery3.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere3"));
        waitForConditionSignal(lock, signal);
        assertThat(watchedResult.get(), is(equalTo(asList(URI.create("dns://somewhere3"), URI.create("dns://somewhere2")))));

        //SHUTDOWN NUMBER2
        serviceDiscovery2.close();
        waitForConditionSignal(lock, signal);
        assertThat(watchedResult.get(), is(equalTo(Collections.singletonList(URI.create("dns://somewhere3")))));

        //SHUTDOWN NUMBER3
        serviceDiscovery3.close();
        waitForConditionSignal(lock, signal);
        assertThat(watchedResult.get(), is(equalTo(Collections.emptyList())));
    }

    @Test
    public void shouldDeregisterASingleInstanceOfAService() throws Exception {

        ServiceDiscovery serviceDiscovery1 = new ServiceDiscovery(host);
        serviceDiscovery1.removeServiceRegistry(TEST_SERVICE_ID);
        serviceDiscovery1.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere1"));
        ServiceDiscovery serviceDiscovery2 = new ServiceDiscovery(host);
        serviceDiscovery2.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere2"));
        assertThat(serviceDiscovery2.discoverUnzoned(TEST_SERVICE_ID), is(equalTo(asList(URI.create("dns://somewhere2"), URI.create("dns://somewhere1")))));
        serviceDiscovery1.deregister(TEST_SERVICE_ID, URI.create("dns://somewhere1"));
        assertThat(serviceDiscovery2.discoverUnzoned(TEST_SERVICE_ID), is(equalTo(Collections.singletonList(URI.create("dns://somewhere2")))));

    }

    @Test
    public void shouldRegisterWithZoneAndRetrieveWithZone() throws Exception {

        ServiceDiscovery ops1 = new ServiceDiscovery(host);
        ops1.removeServiceRegistry(TEST_SERVICE_ID);
        ops1.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere1"), "ZONE_ONE");
        ServiceDiscovery ops2 = new ServiceDiscovery(host);
        ops2.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere2"), "ZONE_TWO");

        assertThat(ops2.discover(TEST_SERVICE_ID),
                is(equalTo(asList(
                        new HostandZone(URI.create("dns://somewhere2"), "ZONE_TWO"),
                        new HostandZone(URI.create("dns://somewhere1"), "ZONE_ONE")
                ))));

        ops1.deregister(TEST_SERVICE_ID, URI.create("dns://somewhere1"), "ZONE_ONE");

        assertThat(ops2.discover(TEST_SERVICE_ID), is(equalTo(Collections.singletonList(
                new HostandZone(URI.create("dns://somewhere2"), "ZONE_TWO")
        ))));
    }

    @Test
    public void shouldWatchForZonedUpdates() throws Exception {
        ReentrantLock lock = new ReentrantLock(true);
        Condition signal = lock.newCondition();
        AtomicReference<List<HostandZone>> watchedResult = new AtomicReference<>();
        ServiceDiscovery serviceDiscovery1 = new ServiceDiscovery(host);
        serviceDiscovery1.removeServiceRegistry(TEST_SERVICE_ID);

        serviceDiscovery1.watchForUpdates(TEST_SERVICE_ID, newList -> {
            watchedResult.set(newList);
            lock.lock();
            signal.signalAll();
            lock.unlock();
        });

        ServiceDiscovery serviceDiscovery2 = new ServiceDiscovery(host);
        ServiceDiscovery serviceDiscovery3 = new ServiceDiscovery(host);
        //REGISTER SERVICE 2
        serviceDiscovery2.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere2"), "ZONE_TWO");
        waitForConditionSignal(lock, signal);
        assertThat(watchedResult.get(), is(equalTo(Collections.singletonList(new HostandZone(URI.create("dns://somewhere2"), "ZONE_TWO")))));
        //REGISTER SERVICE 3
        serviceDiscovery3.registerService(TEST_SERVICE_ID, URI.create("dns://somewhere3"), "ZONE_THREE");
        waitForConditionSignal(lock, signal);
        assertThat(watchedResult.get(), is(equalTo(asList(
                new HostandZone(URI.create("dns://somewhere3"), "ZONE_THREE"),
                new HostandZone(URI.create("dns://somewhere2"), "ZONE_TWO")
        ))));

        //SHUTDOWN NUMBER2
        serviceDiscovery2.close();
        waitForConditionSignal(lock, signal);
        assertThat(watchedResult.get(), is(equalTo(Collections.singletonList(new HostandZone(URI.create("dns://somewhere3"), "ZONE_THREE")))));

        //SHUTDOWN NUMBER3
        serviceDiscovery3.close();
        waitForConditionSignal(lock, signal);
        assertThat(watchedResult.get(), is(equalTo(Collections.emptyList())));
    }

    private void waitForConditionSignal(ReentrantLock lock, Condition signal) throws InterruptedException {
        lock.lock();
        signal.await();
        lock.unlock();
    }

    @Test
    public void uriShouldBeDecomposable() throws Exception {
        URI uri = URI.create("//testService");
        System.out.println(uri.getAuthority());
    }
}