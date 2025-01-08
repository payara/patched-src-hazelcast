/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cp.internal;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMap;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.CPObjectInfo;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.CPSubsystemManagementService;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.event.CPGroupAvailabilityListener;
import com.hazelcast.cp.event.CPMembershipListener;
import com.hazelcast.cp.exception.NotLeaderException;
import com.hazelcast.cp.internal.datastructures.atomiclong.AtomicLongService;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefService;
import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import com.hazelcast.cp.internal.datastructures.cpmap.CPMapServiceUtil;
import com.hazelcast.cp.internal.datastructures.lock.LockService;
import com.hazelcast.cp.internal.datastructures.semaphore.SemaphoreService;
import com.hazelcast.cp.internal.datastructures.spi.RaftRemoteService;
import com.hazelcast.cp.internal.datastructures.spi.atomic.RaftAtomicValueSnapshot;
import com.hazelcast.cp.internal.datastructures.spi.blocking.ResourceRegistry;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.session.RaftSessionService;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.session.CPSessionManagementService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.stream.Collectors.toList;

/**
 * Provides access to CP Subsystem utilities
 */
public class CPSubsystemImpl implements CPSubsystem {

    public static final String CPMAP_LICENSE_MESSAGE =
            "CPMap is not included in your license. Please also ensure you are using the enterprise client.";

    protected final boolean cpSubsystemEnabled;
    private final NodeEngine nodeEngine;
    private volatile CPSubsystemManagementService cpSubsystemManagementService;

    public CPSubsystemImpl(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        int cpMemberCount = nodeEngine.getConfig().getCPSubsystemConfig().getCPMemberCount();
        this.cpSubsystemEnabled = cpMemberCount > 0;
        ILogger logger = nodeEngine.getLogger(CPSubsystem.class);
        if (cpSubsystemEnabled) {
            logger.info("CP Subsystem is enabled with " + cpMemberCount + " members.");
        } else {
            logger.warning("CP Subsystem is not enabled. CP data structures will operate in UNSAFE mode! "
                           + "Please note that UNSAFE mode will not provide strong consistency guarantees.");
        }
    }

    @Nonnull
    @Override
    public IAtomicLong getAtomicLong(@Nonnull String name) {
        checkNotNull(name, "Retrieving an atomic long instance with a null name is not allowed!");
        return createProxy(AtomicLongService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public <E> IAtomicReference<E> getAtomicReference(@Nonnull String name) {
        checkNotNull(name, "Retrieving an atomic reference instance with a null name is not allowed!");
        return createProxy(AtomicRefService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public ICountDownLatch getCountDownLatch(@Nonnull String name) {
        checkNotNull(name, "Retrieving a count down latch instance with a null name is not allowed!");
        return createProxy(CountDownLatchService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public FencedLock getLock(@Nonnull String name) {
        checkNotNull(name, "Retrieving an fenced lock instance with a null name is not allowed!");
        return createProxy(LockService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public ISemaphore getSemaphore(@Nonnull String name) {
        checkNotNull(name, "Retrieving a semaphore instance with a null name is not allowed!");
        return createProxy(SemaphoreService.SERVICE_NAME, name);
    }

    @Override
    public CPMember getLocalCPMember() {
        return getCPSubsystemManagementService().getLocalCPMember();
    }

    @Override
    public CPSubsystemManagementService getCPSubsystemManagementService() {
        if (!cpSubsystemEnabled) {
            throw new HazelcastException("CP Subsystem is not enabled!");
        }

        if (cpSubsystemManagementService != null) {
            return cpSubsystemManagementService;
        }

        RaftService raftService = getService(RaftService.SERVICE_NAME);
        cpSubsystemManagementService = new CPSubsystemManagementServiceImpl(raftService);
        return cpSubsystemManagementService;
    }

    @Override
    public CPSessionManagementService getCPSessionManagementService() {
        if (!cpSubsystemEnabled) {
            throw new HazelcastException("CP Subsystem is not enabled!");
        }
        return getService(RaftSessionService.SERVICE_NAME);
    }

    private <T> T getService(@Nonnull String serviceName) {
        return nodeEngine.getService(serviceName);
    }

    protected <T extends DistributedObject> T createProxy(String serviceName, String name) {
        RaftRemoteService service = getService(serviceName);
        return service.createProxy(name);
    }

    @Override
    public UUID addMembershipListener(CPMembershipListener listener) {
        RaftService raftService = getService(RaftService.SERVICE_NAME);
        return raftService.registerMembershipListener(listener);
    }

    @Override
    public boolean removeMembershipListener(UUID id) {
        RaftService raftService = getService(RaftService.SERVICE_NAME);
        return raftService.removeMembershipListener(id);
    }

    @Override
    public UUID addGroupAvailabilityListener(CPGroupAvailabilityListener listener) {
        RaftService raftService = getService(RaftService.SERVICE_NAME);
        return raftService.registerAvailabilityListener(listener);
    }

    @Override
    public boolean removeGroupAvailabilityListener(UUID id) {
        RaftService raftService = getService(RaftService.SERVICE_NAME);
        return raftService.removeAvailabilityListener(id);
    }

    @Nonnull
    @Override
    public <K, V> CPMap<K, V> getMap(@Nonnull String name) {
        throw new UnsupportedOperationException(CPMAP_LICENSE_MESSAGE);
    }

    @Nonnull
    @Override
    public Collection<CPGroupId> getCPGroupIds() {
        try {
            return getCPSubsystemManagementService().getCPGroupIds().toCompletableFuture().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new HazelcastException("Could not retrieve CP group ids", e);
        }
    }

    @Nonnull
    @Override
    public Iterable<CPObjectInfo> getObjectInfos(@Nonnull CPGroupId groupId, @Nonnull String serviceName) {
        return getCPObjectInfos(groupId, serviceName, false);
    }

    @Nonnull
    @Override
    public Iterable<CPObjectInfo> getTombstoneInfos(@Nonnull CPGroupId groupId, @Nonnull String serviceName) {
        return getCPObjectInfos(groupId, serviceName, true);
    }

    private Iterable<CPObjectInfo> getCPObjectInfos(CPGroupId groupId, String serviceName, boolean returnTombstone) {
        RaftService raftService = getService(RaftService.SERVICE_NAME);
        try {
            // The names need to be collected from a snapshot on leader of the group
            Collection<String> names = raftService.getObjectNames(groupId, serviceName, returnTombstone).get();
            return toObjectInfos(names, serviceName, groupId);
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof NotLeaderException) {
                throw (NotLeaderException) e.getCause();
            }
            if (e.getCause() instanceof TargetNotMemberException) {
                throw new NotLeaderException(groupId, raftService.getLocalCPEndpoint(), null);
            }
            throw new HazelcastException(e);
        }
    }

    private static Iterable<CPObjectInfo> toObjectInfos(
            Collection<String> names,
            String serviceName,
            CPGroupId groupId) {

        return names.stream()
                .map(name -> new CPObjectInfoImpl(name, serviceName, groupId))
                .collect(toList());
    }

    public Collection<String> getCPObjectNames(CPGroupId groupId, String serviceName, boolean returnTombstone) {
        switch (serviceName) {
            case LockService.SERVICE_NAME:
            case SemaphoreService.SERVICE_NAME:
            case CountDownLatchService.SERVICE_NAME:
                return listSnapshotAwareServiceResourceNames(
                        groupId,
                        returnTombstone,
                        serviceName,
                        ResourceRegistry::getDestroyedNames,
                        (ResourceRegistry<?, ?> registry) -> new HashSet<>(registry.getResources().keySet())
                );

            case AtomicLongService.SERVICE_NAME:
            case AtomicRefService.SERVICE_NAME:
                return listSnapshotAwareServiceResourceNames(
                        groupId,
                        returnTombstone,
                        serviceName,
                        RaftAtomicValueSnapshot::getDestroyed,
                        (RaftAtomicValueSnapshot<?> snapshot) ->
                                StreamSupport.stream(snapshot.getValues().spliterator(), false)
                                        .map(Map.Entry::getKey)
                                        .collect(Collectors.toSet())
                );

            case CPMapServiceUtil.SERVICE_NAME:
                // This behavior is overridden in EE
                throw new UnsupportedOperationException(CPMAP_LICENSE_MESSAGE);

            default:
                throw new IllegalArgumentException("Calling getCPObjectInfo is not supported for " + serviceName);
        }
    }

    protected <T> Collection<String> listSnapshotAwareServiceResourceNames(
            CPGroupId groupId,
            boolean tombstone,
            String serviceName,
            Function<T, Collection<String>> tombstonesFn,
            Function<T, Collection<String>> resourceFn
    ) {
        SnapshotAwareService<T> service = nodeEngine.getService(serviceName);
        T snapshot = service.takeSnapshot(groupId, -1);
        if (snapshot == null) {
            return Collections.emptyList();
        }
        if (tombstone) {
            return tombstonesFn.apply(snapshot);
        } else {
            return resourceFn.apply(snapshot);
        }
    }

    /*
        'map' is already taken in HazelcastNamespaceProvider for Spring support. When you create a 'hz:cpmap' type (see
        fullConfig-applicationContext-hazelcast.xml) it will call this method as per the semantics of HazelcastNamespaceProvider.
         */
    private <K, V> CPMap<K, V> getCpmap(@Nonnull String name) {
        return getMap(name);
    }

    private static class CPSubsystemManagementServiceImpl implements CPSubsystemManagementService {
        private final RaftService raftService;

        CPSubsystemManagementServiceImpl(RaftService raftService) {
            this.raftService = raftService;
        }

        @Override
        public CPMember getLocalCPMember() {
            return raftService.getLocalCPMember();
        }

        @Override
        public InternalCompletableFuture<Collection<CPGroupId>> getCPGroupIds() {
            return raftService.getCPGroupIds();
        }

        @Override
        public InternalCompletableFuture<CPGroup> getCPGroup(String name) {
            return raftService.getCPGroup(name);
        }

        @Override
        public InternalCompletableFuture<Void> forceDestroyCPGroup(String groupName) {
            return raftService.forceDestroyCPGroup(groupName);
        }

        @Override
        public InternalCompletableFuture<Collection<CPMember>> getCPMembers() {
            return raftService.getCPMembers();
        }

        @Override
        public InternalCompletableFuture<Void> promoteToCPMember() {
            return raftService.promoteToCPMember();
        }

        @Override
        public InternalCompletableFuture<Void> removeCPMember(UUID cpMemberUuid) {
            return raftService.removeCPMember(cpMemberUuid);
        }

        @Override
        public InternalCompletableFuture<Void> reset() {
            return raftService.resetCPSubsystem();
        }

        @Override
        public boolean isDiscoveryCompleted() {
            return raftService.isDiscoveryCompleted();
        }

        @Override
        public boolean awaitUntilDiscoveryCompleted(long timeout, TimeUnit timeUnit) throws InterruptedException {
            return raftService.awaitUntilDiscoveryCompleted(timeout, timeUnit);
        }
    }

}
