
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

enum ComponentStatus {
    HEALTHY, DEGRADED, FAILED, RECOVERING
}

enum FaultType {
    RETRY, DRIFT, BACKPRESSURE, NODE_PARTITION, LIVELOCK, CRASH, TIMEOUT, REPLAY
}

class Component {
    final String name;
    final double latencyMs;
    final double throughputMbps;
    final double reliabilityPercent;
    final int requestsPerSec;
    final String dependency;
    final Set<FaultType> faultEvents;
    
    volatile ComponentStatus status;
    volatile long lastHealthCheck;
    
    public Component(String name, double latencyMs, double throughputMbps, 
                    double reliabilityPercent, int requestsPerSec, 
                    String dependency, Set<FaultType> faultEvents) {
        this.name = name;
        this.latencyMs = latencyMs;
        this.throughputMbps = throughputMbps;
        this.reliabilityPercent = reliabilityPercent;
        this.requestsPerSec = requestsPerSec;
        this.dependency = dependency;
        this.faultEvents = faultEvents;
        this.status = ComponentStatus.HEALTHY;
        this.lastHealthCheck = System.currentTimeMillis();
    }
    
    public boolean isDominant() {
        
        return "WebGateway".equals(this.name);
    }
}

class Proposal {
    final String requestId;
    final long timestamp;
    final Map<String, Object> data;
    final AtomicInteger promises;
    final AtomicInteger accepts;
    final AtomicBoolean committed;
    
    public Proposal(String requestId, long timestamp, Map<String, Object> data) {
        this.requestId = requestId;
        this.timestamp = timestamp;
        this.data = new HashMap<>(data);
        this.promises = new AtomicInteger(0);
        this.accepts = new AtomicInteger(0);
        this.committed = new AtomicBoolean(false);
    }
}

class AssumptionValidator {
    private final Component dominantComponent;
    private final int quorumSize;
    private final double minReliability = 94.7;
    private final double minQuorumAvailability = 0.67;
    
    private volatile boolean latencyDominanceHolds = true;
    private volatile boolean contentionInvariantsHold = true;
    
    public AssumptionValidator(Component dominantComponent, int quorumSize) {
        this.dominantComponent = dominantComponent;
        this.quorumSize = quorumSize;
    }
    

    public boolean validateLatencyDominance(List<Component> allComponents) {
        double maxLatency = allComponents.stream()
            .mapToDouble(c -> c.latencyMs)
            .max()
            .orElse(0.0);
        
        latencyDominanceHolds = (dominantComponent.latencyMs >= maxLatency * 0.95);
        return latencyDominanceHolds;
    }
    
 
    public boolean validateContentionInvariants(double quorumAvailability) {
        boolean i1 = latencyDominanceHolds;
        boolean i2 = dominantComponent.reliabilityPercent >= minReliability;
        boolean i3 = quorumAvailability >= minQuorumAvailability;
        
        contentionInvariantsHold = i1 && i2 && i3;
        return contentionInvariantsHold;
    }
    
    public boolean validateJointAssumptions(List<Component> allComponents, 
                                          double quorumAvailability) {
        boolean bHolds = validateLatencyDominance(allComponents);
        boolean cHolds = validateContentionInvariants(quorumAvailability);
        
        return bHolds && cHolds;
    }
    
    public boolean areSafetyConditionsMet() {
        return latencyDominanceHolds && contentionInvariantsHold;
    }
}


class ConsensusFailoverManager {
    private final Component primaryComponent;
    private final List<Component> backupComponents;
    private final AssumptionValidator validator;
    private final int quorumSize;
    
    private final ConcurrentHashMap<String, Proposal> proposals;
    private final ConcurrentHashMap<String, Long> nodeTimestamps;
    private final AtomicLong globalTimestamp;
    private final ReentrantReadWriteLock safeguardLock;
    
    private volatile boolean safetyEnabled = true;
    
    public ConsensusFailoverManager(Component primary, List<Component> backups, 
                                   int quorumSize) {
        this.primaryComponent = primary;
        this.backupComponents = new ArrayList<>(backups);
        this.quorumSize = quorumSize;
        this.validator = new AssumptionValidator(primary, quorumSize);
        
        this.proposals = new ConcurrentHashMap<>();
        this.nodeTimestamps = new ConcurrentHashMap<>();
        this.globalTimestamp = new AtomicLong(0);
        this.safeguardLock = new ReentrantReadWriteLock();
    }
    

    public boolean prepareFailover(String requestId, Map<String, Object> data, 
                                  List<Component> allComponents) {
        safeguardLock.readLock().lock();
        try {
            // SAFETY CHECK: Validate assumptions from (b) and (c)
            double quorumAvail = calculateQuorumAvailability();
            boolean assumptionsValid = validator.validateJointAssumptions(
                allComponents, quorumAvail);
            
            if (!assumptionsValid || !safetyEnabled) {
                System.err.println("[SAFETY VIOLATION] Failover rejected - " +
                    "assumptions from (b) and (c) do not jointly hold");
                return false;
            }
            
            long timestamp = globalTimestamp.incrementAndGet();
            Proposal proposal = new Proposal(requestId, timestamp, data);
            proposals.put(requestId, proposal);
            
            System.out.println("[PREPARE] Request: " + requestId + 
                             " | Timestamp: " + timestamp);
            return true;
            
        } finally {
            safeguardLock.readLock().unlock();
        }
    }
    
    public boolean promise(String requestId, String nodeId) {
        Proposal proposal = proposals.get(requestId);
        if (proposal == null || proposal.committed.get()) {
            return false;
        }
        
        long lastTimestamp = nodeTimestamps.getOrDefault(nodeId, 0L);
        
        if (proposal.timestamp > lastTimestamp) {
            nodeTimestamps.put(nodeId, proposal.timestamp);
            int promiseCount = proposal.promises.incrementAndGet();
            
            System.out.println("[PROMISE] Node: " + nodeId + 
                             " | Request: " + requestId + 
                             " | Count: " + promiseCount + "/" + quorumSize);
            return true;
        }
        
        return false;
    }
    
    public boolean acceptFailover(String requestId, List<Component> allComponents) {
        safeguardLock.readLock().lock();
        try {
            Proposal proposal = proposals.get(requestId);
            if (proposal == null || proposal.committed.get()) {
                return false;
            }
            
            // SAFETY CHECK: Re-validate before commit
            double quorumAvail = calculateQuorumAvailability();
            if (!validator.validateJointAssumptions(allComponents, quorumAvail)) {
                System.err.println("[SAFETY VIOLATION] Accept rejected - " +
                    "assumptions violated before commit");
                return false;
            }
            
            if (proposal.promises.get() >= quorumSize) {
                proposal.committed.set(true);
                proposal.accepts.set(proposal.promises.get());
                
                System.out.println("[ACCEPT] Request: " + requestId + 
                                 " | Quorum: " + proposal.promises.get() + 
                                 "/" + quorumSize + " | COMMITTED");
                return true;
            }
            
            return false;
            
        } finally {
            safeguardLock.readLock().unlock();
        }
    }
    
    
    public FailoverResult executeFailover(String requestId, 
                                         List<Component> allComponents) {
        safeguardLock.writeLock().lock();
        try {
            Proposal proposal = proposals.get(requestId);
            if (proposal == null || !proposal.committed.get()) {
                return new FailoverResult(false, "Proposal not committed", null);
            }
            
            // Final safety check
            if (!validator.areSafetyConditionsMet()) {
                return new FailoverResult(false, 
                    "Safety conditions from (b) and (c) not met", null);
            }
            
            // Determine target based on component status
            Component target = selectFailoverTarget();
            if (target == null) {
                return new FailoverResult(false, "No healthy backup available", null);
            }
            
            // Execute failover
            System.out.println("[EXECUTE] Failover to: " + target.name);
            primaryComponent.status = ComponentStatus.DEGRADED;
            target.status = ComponentStatus.HEALTHY;
            
            return new FailoverResult(true, "Failover successful", target);
            
        } finally {
            safeguardLock.writeLock().unlock();
        }
    }
    
    private double calculateQuorumAvailability() {
        long healthyCount = backupComponents.stream()
            .filter(c -> c.status == ComponentStatus.HEALTHY || 
                        c.status == ComponentStatus.DEGRADED)
            .count();
        
        return (double) healthyCount / backupComponents.size();
    }

    private Component selectFailoverTarget() {
        return backupComponents.stream()
            .filter(c -> c.status == ComponentStatus.HEALTHY)
            .min(Comparator.comparingDouble(c -> c.latencyMs))
            .orElse(null);
    }
    
    public void validateSafetyPeriodic(List<Component> allComponents) {
        double quorumAvail = calculateQuorumAvailability();
        boolean valid = validator.validateJointAssumptions(allComponents, quorumAvail);
        
        if (!valid) {
            System.err.println("[WARNING] Safety assumptions violated - " +
                "failover operations will be rejected");
            safetyEnabled = false;
        } else {
            safetyEnabled = true;
        }
    }
}

class FailoverResult {
    final boolean success;
    final String message;
    final Component targetComponent;
    
    public FailoverResult(boolean success, String message, Component target) {
        this.success = success;
        this.message = message;
        this.targetComponent = target;
    }
    
    @Override
    public String toString() {
        return String.format("FailoverResult{success=%s, message='%s', target=%s}",
            success, message, targetComponent != null ? targetComponent.name : "none");
    }
}


public class TelecomFailoverSystem {
    
    public static void main(String[] args) throws InterruptedException {
        System.out.println("=" .repeat(70));
        System.out.println("TELECOM FAILOVER SYSTEM - JAVA IMPLEMENTATION");
        System.out.println("Safety preserved only if assumptions (b) and (c) jointly hold");
        System.out.println("=" .repeat(70));
        
        // Initialize components
        Component authCore = new Component("AuthCore", 21, 520, 97.6, 165, 
            null, EnumSet.of(FaultType.RETRY, FaultType.DRIFT));
        
        Component queueRelay = new Component("QueueRelay", 27, 430, 96.8, 118,
            "AuthCore", EnumSet.of(FaultType.BACKPRESSURE));
        
        Component edgeOrch = new Component("EdgeOrchestrator", 31, 610, 96.9, 190,
            "QueueRelay", EnumSet.of(FaultType.NODE_PARTITION));
        
        Component webGateway = new Component("WebGateway", 38, 690, 94.7, 215,
            "EdgeOrchestrator", EnumSet.of(FaultType.LIVELOCK));
        
        Component cloudInf = new Component("CloudInference", 44, 1150, 92.4, 275,
            "WebGateway", EnumSet.of(FaultType.CRASH, FaultType.TIMEOUT, FaultType.REPLAY));
        
        // Create backup gateways (simulate redundancy)
        Component webGateway2 = new Component("WebGateway-Backup1", 40, 680, 95.0, 210,
            "EdgeOrchestrator", EnumSet.of(FaultType.LIVELOCK));
        
        Component webGateway3 = new Component("WebGateway-Backup2", 42, 670, 94.5, 205,
            "EdgeOrchestrator", EnumSet.of(FaultType.LIVELOCK));
        
        List<Component> allComponents = Arrays.asList(
            authCore, queueRelay, edgeOrch, webGateway, cloudInf);
        
        List<Component> backups = Arrays.asList(webGateway2, webGateway3);
        
        // Initialize failover manager
        ConsensusFailoverManager failoverMgr = new ConsensusFailoverManager(
            webGateway, backups, 2);
        
        System.out.println("\n[TEST 1] Normal Failover - All Assumptions Valid");
        System.out.println("-" .repeat(70));
        
        boolean prepared = failoverMgr.prepareFailover("failover-001", 
            Map.of("reason", "high latency detected"), allComponents);
        
        if (prepared) {
            failoverMgr.promise("failover-001", "backup-node-1");
            failoverMgr.promise("failover-001", "backup-node-2");
            
            boolean accepted = failoverMgr.acceptFailover("failover-001", allComponents);
            
            if (accepted) {
                FailoverResult result = failoverMgr.executeFailover("failover-001", 
                    allComponents);
                System.out.println("Result: " + result);
            }
        }
        
        System.out.println("\n[TEST 2] Safety Violation - Reliability Below Threshold");
        System.out.println("-" .repeat(70));
        
        // Simulate reliability degradation (violates assumption from part c)
        webGateway.reliabilityPercent = 93.0;  // Below 94.7% threshold
        
        boolean prepared2 = failoverMgr.prepareFailover("failover-002",
            Map.of("reason", "test safety"), allComponents);
        
        System.out.println("Failover allowed: " + prepared2 + 
            " (Expected: false due to safety violation)");
        
        // Restore reliability
        webGateway.reliabilityPercent = 94.7;
        
        System.out.println("\n[TEST 3] Periodic Safety Validation");
        System.out.println("-" .repeat(70));
        
        failoverMgr.validateSafetyPeriodic(allComponents);
        System.out.println("Safety validation completed");
        
        System.out.println("\n" + "=" .repeat(70));
        System.out.println("FAILOVER TESTS COMPLETE");
        System.out.println("Safety guaranteed only when (b) latency dominance AND");
        System.out.println("(c) contention invariants jointly hold");
        System.out.println("=" .repeat(70));
    }
                    }
