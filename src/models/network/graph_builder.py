"""
Network Analysis for Corruption Ring Detection
Builds graphs of relationships between officials, vendors, and contracts.
"""

import pandas as pd
import networkx as nx
import community as community_louvain
from pathlib import Path
from collections import defaultdict


class CorruptionNetwork:
    """Build and analyze relationship networks."""
    
    def __init__(self):
        self.graph = nx.Graph()
        self.contracts_df = None
        self.vendors_df = None
        self.officials_df = None
    
    def load_data(self, contracts: pd.DataFrame, vendors: pd.DataFrame, officials: pd.DataFrame):
        """Load dataframes for network building."""
        self.contracts_df = contracts
        self.vendors_df = vendors
        self.officials_df = officials
    
    def build_graph(self) -> nx.Graph:
        """Build bipartite graph: officials <-> vendors <-> contracts."""
        
        G = self.graph
        
        # Add vendor nodes
        for _, vendor in self.vendors_df.iterrows():
            G.add_node(
                f"vendor_{vendor['nit']}",
                node_type="vendor",
                name=vendor["name"]
            )
        
        # Add official nodes
        for _, official in self.officials_df.iterrows():
            G.add_node(
                f"official_{official['name']}",
                node_type="official",
                position=official["position"]
            )
        
        # Add contract nodes and edges
        for _, contract in self.contracts_df.iterrows():
            contract_id = contract["contract_id"]
            vendor_nit = contract["vendor_nit"]
            contractor_name = contract.get("contractor_name", "")
            
            # Contract node
            G.add_node(
                f"contract_{contract_id}",
                node_type="contract",
                value=contract["contract_value"],
                title=contract["title"]
            )
            
            # Edge: Contract -> Vendor
            G.add_edge(
                f"contract_{contract_id}",
                f"vendor_{vendor_nit}",
                edge_type="awarded_to"
            )
            
            # Edge: Contract -> Contractor (person)
            if contractor_name:
                # Add contractor as node if not exists
                if not G.has_node(f"person_{contractor_name}"):
                    G.add_node(
                        f"person_{contractor_name}",
                        node_type="person",
                        name=contractor_name
                    )
                G.add_edge(
                    f"contract_{contract_id}",
                    f"person_{contractor_name}",
                    edge_type="executed_by"
                )
        
        # TODO: Add cross-references (future: link officials to vendors via declared interests)
        
        self.graph = G
        print(f"📈 Built graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
        return G
    
    def detect_communities(self) -> dict:
        """Detect communities (potential corruption rings) using Louvain."""
        
        G = self.graph
        if G.number_of_nodes() == 0:
            return {}
        
        # Run Louvain community detection
        partition = community_louvain.best_partition(G)
        
        # Group by community
        communities = defaultdict(list)
        for node, comm_id in partition.items():
            communities[comm_id].append(node)
        
        # Store in graph
        nx.set_node_attributes(G, partition, "community")
        
        print(f"🔍 Detected {len(communities)} communities")
        
        # Analyze suspicious communities
        suspicious = self._find_suspicious_communities(communities)
        
        return {
            "all": communities,
            "suspicious": suspicious
        }
    
    def _find_suspicious_communities(self, communities: dict) -> list:
        """Identify communities that look like corruption rings."""
        
        suspicious = []
        
        for comm_id, nodes in communities.items():
            # Get node types in this community
            node_types = [self.graph.nodes[n].get("node_type") for n in nodes]
            
            # Suspicious pattern: multiple vendors + officials together
            vendor_count = node_types.count("vendor")
            official_count = node_types.count("official")
            contract_count = node_types.count("contract")
            
            # Calculate community total contract value
            total_value = sum(
                self.graph.nodes[n].get("value", 0)
                for n in nodes if self.graph.nodes[n].get("node_type") == "contract"
            )
            
            # Flag as suspicious if:
            # - Has officials AND vendors (conflict of interest)
            # - High contract value
            # - Multiple contracts between same players
            if (official_count > 0 and vendor_count > 0) or contract_count > 3:
                suspicious.append({
                    "community_id": comm_id,
                    "nodes": nodes,
                    "vendor_count": vendor_count,
                    "official_count": official_count,
                    "contract_count": contract_count,
                    "total_value": total_value,
                    "risk_level": "HIGH" if (official_count > 0 and vendor_count > 0) else "MEDIUM"
                })
        
        return suspicious
    
    def find_connections(self, node_name: str) -> dict:
        """Find all connections for a specific entity."""
        
        G = self.graph
        if not G.has_node(node_name):
            return {"error": "Node not found"}
        
        # Get immediate neighbors
        neighbors = list(G.neighbors(node_name))
        
        # Get edge types
        edge_types = []
        for n in neighbors:
            edge_data = G.get_edge_data(node_name, n)
            edge_types.append({
                "target": n,
                "type": edge_data.get("edge_type", "unknown"),
                "target_type": G.nodes[n].get("node_type", "unknown")
            })
        
        return {
            "node": node_name,
            "node_info": G.nodes[node_name],
            "neighbors": edge_types,
            "neighbor_count": len(neighbors)
        }
    
    def export_for_gephi(self, filepath: str):
        """Export graph in Gephi-compatible format."""
        
        # Create edges dataframe
        edges = []
        for u, v, data in self.graph.edges(data=True):
            edges.append({
                "Source": u,
                "Target": v,
                "Type": data.get("edge_type", "undirected"),
                "Weight": 1
            })
        edges_df = pd.DataFrame(edges)
        edges_df.to_csv(filepath.replace(".gexf", "_edges.csv"), index=False)
        
        # Create nodes dataframe
        nodes = []
        for node, data in self.graph.nodes(data=True):
            nodes.append({
                "Id": node,
                "Label": data.get("name", data.get("title", node)),
                "Type": data.get("node_type", "unknown"),
                "Community": data.get("community", -1)
            })
        nodes_df = pd.DataFrame(nodes)
        nodes_df.to_csv(filepath.replace(".gexf", "_nodes.csv"), index=False)
        
        # Also save as GEXF
        nx.write_gexf(self.graph, filepath)
        
        print(f"📤 Exported to {filepath} (CSV + GEXF)")


if __name__ == "__main__":
    # Quick test
    from scraper_secop import SecopScraper
    from features.engineering import engineer_all_features
    
    scraper = SecopScraper()
    contracts = scraper.fetch_contracts()
    vendors = scraper.fetch_vendor_registry()
    officials = scraper.fetch_officials()
    contracts = engineer_all_features(contracts)
    
    # Build network
    network = CorruptionNetwork()
    network.load_data(contracts, vendors, officials)
    network.build_graph()
    
    # Detect communities
    communities = network.detect_communities()
    
    print("\n🚨 Suspicious Communities:")
    for comm in communities["suspicious"]:
        print(f"  Community {comm['community_id']}: {comm['vendor_count']} vendors, "
              f"{comm['official_count']} officials, ${comm['total_value']/1e6:.1f}M — {comm['risk_level']}")