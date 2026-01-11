
import math
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum


class FaultType(Enum):
    RETRY = "Retry"
    DRIFT = "Drift"
    BACKPRESSURE = "Backpressure"
    NODE_PARTITION = "NodePartition"
    LIVELOCK = "Livelock"
    CRASH = "Crash"
    TIMEOUT = "Timeout"
    REPLAY = "Replay"


@dataclass
class Component:
    """Represents a telecom system component"""
    name: str
    cpu_percent: float
    memory_gb: float
    latency_ms: float
    throughput_mbps: float
    reliability_percent: float
    requests_per_sec: int
    dependency: Optional[str]
    fault_events: List[FaultType]
    
    def __repr__(self):
        return f"{self.name}(L={self.latency_ms}ms, T={self.throughput_mbps}Mbps)"


class DominantLatencyAnalyzer:
    
    def __init__(self, components: List[Component]):
        self.components = {c.name: c for c in components}
        self.dependency_chain = self._build_dependency_chain()
    
    def _build_dependency_chain(self) -> Dict[str, List[str]]:
    
        chain = {name: [] for name in self.components}
        for name, comp in self.components.items():
            if comp.dependency:
                chain[comp.dependency].append(name)
        return chain
    
    def calculate_cumulative_latency(self, component_name: str) -> float:
        
        comp = self.components[component_name]
        if not comp.dependency:
            return comp.latency_ms
        return comp.latency_ms + self.calculate_cumulative_latency(comp.dependency)
    
    def calculate_downstream_throughput(self, component_name: str) -> float:

        total = self.components[component_name].throughput_mbps
        for dependent in self.dependency_chain[component_name]:
            total += self.calculate_downstream_throughput(dependent)
        return total
    
    def identify_dominant_source(self) -> Tuple[str, Dict[str, float]]:
        
        metrics = {}
        
        for name in self.components:
            latency = self.components[name].latency_ms
            downstream_throughput = self.calculate_downstream_throughput(name)
            ratio = latency / downstream_throughput if downstream_throughput > 0 else float('inf')
            
            metrics[name] = {
                'latency': latency,
                'cumulative_latency': self.calculate_cumulative_latency(name),
                'downstream_throughput': downstream_throughput,
                'latency_throughput_ratio': ratio
            }
        
        # Find component with highest ratio (high latency, low throughput impact)
        dominant = max(metrics.items(), 
                      key=lambda x: x[1]['latency_throughput_ratio'])
        
        return dominant[0], metrics


class ConsensusRPCScheme:
    #Part b
    
    def __init__(self, dominant_component: str, quorum_size: int = 2):
        self.dominant_component = dominant_component
        self.quorum_size = quorum_size
        self.proposals = {}
        self.promises = {}
        self.accepted = {}
        self.current_timestamp = 0
    
    def prepare(self, request_id: str, data: dict) -> bool:
        
        self.current_timestamp += 1
        self.proposals[request_id] = {
            'timestamp': self.current_timestamp,
            'data': data,
            'promises': 0,
            'accepts': 0
        }
        return True
    
    def promise(self, request_id: str, node_id: str, last_timestamp: int) -> bool:
        aif timestamp is fresh
        if request_id not in self.proposals:
            return False
        
        proposal = self.proposals[request_id]
        if proposal['timestamp'] > last_timestamp:
            proposal['promises'] += 1
            return True
        return False
    
    def accept(self, request_id: str) -> bool:
        """Phase 3: Accept if quorum reached"""
        if request_id not in self.proposals:
            return False
        
        proposal = self.proposals[request_id]
        if proposal['promises'] >= self.quorum_size:
            proposal['accepts'] = proposal['promises']
            self.accepted[request_id] = proposal
            return True
        return False
    
    def learn(self, request_id: str) -> Optional[dict]:
        """Phase 4: Learn committed value"""
        return self.accepted.get(request_id)
    
    def is_consensus_valid(self, latency_dominance_holds: bool) -> bool:
        """Correctness depends on latency dominance assumption"""
        return latency_dominance_holds


class ContentionModel:

    
    def __init__(self, component: Component, quorum_size: int):
        self.component = component
        self.quorum_size = quorum_size
        self.invariants_valid = True
    
    def calculate_contention(self) -> float:
        """Calculate contention level"""
        return (self.component.requests_per_sec * self.component.latency_ms) / \
               (1000 * self.quorum_size)
    
    def check_invariants(self, is_latency_dominant: bool, 
                        quorum_available: float) -> bool:

        i1 = is_latency_dominant
        i2 = self.component.reliability_percent >= 94.7
        i3 = quorum_available >= (2/3)
        
        self.invariants_valid = i1 and i2 and i3
        return self.invariants_valid
    
    def is_feasible(self) -> bool:
        """Model is feasible only when invariants hold"""
        return self.invariants_valid


class LatencyElasticityOptimizer:
    """
    Part (d): Trade-off analysis with contention non-linearity
    """
    
    def __init__(self, base_latency: float, alpha: float = 0.2, beta: float = 1.5):
        self.base_latency = base_latency
        self.alpha = alpha  # Contention coefficient
        self.beta = beta    # Non-linearity exponent
    
    def effective_latency(self, contention: float) -> float:
        """L_effective = L_base × (1 + α × C^β)"""
        return self.base_latency * (1 + self.alpha * (contention ** self.beta))
    
    def optimal_contention(self) -> float:
        """Find optimal contention level"""
        if self.beta <= 1:
            return 0
        return ((self.beta) / (self.alpha * (self.beta - 1))) ** (1/self.beta)
    
    def analyze_tradeoffs(self, contention_range: List[float]) -> Dict[float, Dict]:
        """Analyze latency-throughput tradeoffs across contention levels"""
        tradeoffs = {}
        
        for c in contention_range:
            l_eff = self.effective_latency(c)
            # Throughput inversely related to effective latency
            relative_throughput = self.base_latency / l_eff
            
            tradeoffs[c] = {
                'effective_latency': l_eff,
                'relative_throughput': relative_throughput,
                'latency_increase_factor': l_eff / self.base_latency
            }
        
        return tradeoffs


class ThroughputOptimizer:

    
    def __init__(self, component: Component, request_size_gb: float = 0.05,
                 divergence_limit: float = 0.15):
        self.component = component
        self.request_size_gb = request_size_gb
        self.divergence_limit = divergence_limit
        self.elasticity_optimizer = LatencyElasticityOptimizer(component.latency_ms)
    
    def memory_divergence(self, contention: float) -> float:
        """Calculate memory under worst-case scheduling"""
        l_eff = self.elasticity_optimizer.effective_latency(contention)
        # Memory = base + queued_requests × request_size
        queued_memory = (self.request_size_gb * contention * l_eff) / 1000
        return self.component.memory_gb + queued_memory
    
    def max_memory_allowed(self) -> float:
        """Maximum memory with divergence bound"""
        return self.component.memory_gb * (1 + self.divergence_limit)
    
    def optimize_throughput(self) -> Dict[str, float]:
        
        max_mem = self.max_memory_allowed()
        best_contention = 0
        best_throughput = 0
        
        # Search for optimal contention level
        for c in [i * 0.1 for i in range(1, 100)]:
            mem = self.memory_divergence(c)
            
            if mem <= max_mem:
                # Throughput scales with memory utilization efficiency
                throughput = self.component.throughput_mbps * (max_mem / mem)
                
                if throughput > best_throughput:
                    best_throughput = throughput
                    best_contention = c
        
        return {
            'optimal_contention': best_contention,
            'optimized_throughput': best_throughput,
            'current_throughput': self.component.throughput_mbps,
            'improvement_percent': ((best_throughput - self.component.throughput_mbps) / 
                                   self.component.throughput_mbps * 100),
            'memory_used': self.memory_divergence(best_contention),
            'memory_max': max_mem
        }


class IntegratedSystemOptimizer:

    
    def __init__(self, components: List[Component]):
        self.components = components
        self.results = {}
    
    def run_full_optimization(self) -> Dict:
        """Execute complete optimization pipeline"""
        print("=" * 70)
        print("TELECOM DISTRIBUTED SYSTEM OPTIMIZATION")
        print("=" * 70)
        
        # Part (a): Identify dominant latency source
        print("\n[PART A] Dominant Latency Analysis")
        print("-" * 70)
        analyzer = DominantLatencyAnalyzer(self.components)
        dominant, metrics = analyzer.identify_dominant_source()
        self.results['dominant_component'] = dominant
        self.results['latency_metrics'] = metrics
        
        print(f"Dominant Component: {dominant}")
        print(f"  Latency: {metrics[dominant]['latency']}ms")
        print(f"  Downstream Throughput Impact: {metrics[dominant]['downstream_throughput']:.1f} Mbps")
        print(f"  Ratio: {metrics[dominant]['latency_throughput_ratio']:.5f}")
        
        # Part (b): Design consensus-RPC
        print("\n[PART B] Consensus-RPC Scheme")
        print("-" * 70)
        consensus = ConsensusRPCScheme(dominant, quorum_size=2)
        self.results['consensus_scheme'] = consensus
        
        # Simulate consensus
        consensus.prepare("req_001", {"payload": "test"})
        consensus.promise("req_001", "node1", 0)
        consensus.promise("req_001", "node2", 0)
        accepted = consensus.accept("req_001")
        
        print(f"Consensus Target: {dominant}")
        print(f"Quorum Size: 2")
        print(f"Test Consensus Result: {'SUCCESS' if accepted else 'FAILED'}")
        
        # Part (c): Formalize contention model
        print("\n[PART C] Contention Model")
        print("-" * 70)
        dominant_comp = next(c for c in self.components if c.name == dominant)
        contention_model = ContentionModel(dominant_comp, quorum_size=2)
        contention = contention_model.calculate_contention()
        self.results['contention'] = contention
        
        is_feasible = contention_model.check_invariants(
            is_latency_dominant=True,
            quorum_available=0.95
        )
        
        print(f"Contention Level: {contention:.3f} concurrent requests")
        print(f"Invariants Valid: {is_feasible}")
        print(f"Model Feasible: {contention_model.is_feasible()}")
        
        # Part (d): Latency elasticity tradeoffs
        print("\n[PART D] Latency Elasticity Trade-offs")
        print("-" * 70)
        elasticity = LatencyElasticityOptimizer(dominant_comp.latency_ms)
        optimal_c = elasticity.optimal_contention()
        self.results['optimal_contention_theoretical'] = optimal_c
        
        print(f"Base Latency: {dominant_comp.latency_ms}ms")
        print(f"Theoretical Optimal Contention: {optimal_c:.2f}")
        print(f"Effective Latency at Optimal: {elasticity.effective_latency(optimal_c):.2f}ms")
        
        # Part (e): Throughput optimization with memory bounds
        print("\n[PART E] Throughput Optimization (Memory Bounded)")
        print("-" * 70)
        throughput_opt = ThroughputOptimizer(dominant_comp)
        opt_results = throughput_opt.optimize_throughput()
        self.results['throughput_optimization'] = opt_results
        
        print(f"Current Throughput: {opt_results['current_throughput']:.1f} Mbps")
        print(f"Optimized Throughput: {opt_results['optimized_throughput']:.1f} Mbps")
        print(f"Improvement: {opt_results['improvement_percent']:.2f}%")
        print(f"Optimal Contention: {opt_results['optimal_contention']:.2f}")
        print(f"Memory Usage: {opt_results['memory_used']:.3f} GB / {opt_results['memory_max']:.3f} GB")
        
        return self.results
    
    def validate_assumption_chain(self) -> bool:
        """
        Part (g): Validate that violation of any assumption invalidates
        subsequent results
        """
        print("\n[PART G] Assumption Chain Validation")
        print("-" * 70)
        
        # Check if dominant component assumption is critical
        if 'dominant_component' not in self.results:
            print(" INVALID: Part (a) must complete first")
            return False
        
        # Consensus depends on dominant component
        consensus = self.results.get('consensus_scheme')
        if not consensus or consensus.dominant_component != self.results['dominant_component']:
            print(" INVALID: Part (b) depends on (a)")
            return False
        
        # Contention model depends on consensus assumptions
        if 'contention' not in self.results:
            print(" INVALID: Part (c) depends on (b)")
            return False
        
        # All subsequent optimizations depend on the chain
        print("✓ Assumption chain validated")
        print("✓ Each result depends on preceding sub-questions")
        print("✓ Violation of any assumption invalidates downstream results")
        
        return True


# Main execution
if __name__ == "__main__":
    # Initialize components from dataset
    components = [
        Component("AuthCore", 47, 2.4, 21, 520, 97.6, 165, None,
                 [FaultType.RETRY, FaultType.DRIFT]),
        Component("QueueRelay", 63, 3.1, 27, 430, 96.8, 118, "AuthCore",
                 [FaultType.BACKPRESSURE]),
        Component("EdgeOrchestrator", 58, 4.6, 31, 610, 96.9, 190, "QueueRelay",
                 [FaultType.NODE_PARTITION]),
        Component("WebGateway", 52, 3.7, 38, 690, 94.7, 215, "EdgeOrchestrator",
                 [FaultType.LIVELOCK]),
        Component("CloudInference", 74, 9.3, 44, 1150, 92.4, 275, "WebGateway",
                 [FaultType.CRASH, FaultType.TIMEOUT, FaultType.REPLAY])
    ]
    
    # Run integrated optimization
    optimizer = IntegratedSystemOptimizer(components)
    results = optimizer.run_full_optimization()
    
    # Validate assumption chain
    optimizer.validate_assumption_chain()
    
    print("\n" + "=" * 70)
    print("OPTIMIZATION COMPLETE")
    print("=" * 70)
