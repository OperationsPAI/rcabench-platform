import json
import statistics

import networkx as nx
from rcabench.openapi import (
    DtoInjectionV2CustomLabelManageReq,
    DtoLabelItem,
    InjectionsApi,
)

from rcabench_platform.v2.clients.rcabench_ import RCABenchClient

from ..datasets.spec import DatasetAnalyzer
from ..logging import logger


class DatasetMetricsCalculator:
    def __init__(self, loader: DatasetAnalyzer):
        self.loader = loader
        self.graph = loader.get_service_dependency_graph()
        self.root_services = loader.get_root_services()
        self.services = loader.get_all_services()

        for svc in self.root_services:
            assert svc in self.graph, f"Service '{svc}' not found in graph"

    def compute_sdd(self, k: int = 1) -> float | list[float]:
        """
        Compute Service Distance to root cause (SDD@k).

        This metric measures the shortest path distance from the top-k services
        with the largest anomaly magnitude to the root cause services.
        When multiple root cause services exist, the maximum distance is selected.

        Formula:
        $$SDD@k = \\max_{r \\in R} \\min_{s \\in T_k} d(s, r)$$

        Where:
        - $R$ = set of root cause services
        - $T_k$ = top-k services ranked by anomaly magnitude $\\Delta_s$
        - $d(s, r)$ = shortest path distance from service $s$ to root cause $r$
        - $\\Delta_s = \\sum_{m \\in M_s} |\\bar{m}_{abnormal} - \\bar{m}_{normal}|$
        - $M_s$ = set of golden signal metrics for service $s$
        - $\\bar{m}_{normal/abnormal}$ = mean value of metric $m$ in normal/abnormal period

        Args:
            k (int): Number of top services to consider

        Returns:
            float | list[float]: Distance(s) to root cause service(s)
        """
        if not self.root_services or not self.graph:
            return 0.0 if k == 1 else [0.0] * k

        # Calculate anomaly magnitude for each service
        service_deltas = {}
        for service in self.services:
            normal_metrics = self.loader.get_service_metrics(service, abnormal=False)
            abnormal_metrics = self.loader.get_service_metrics(service, abnormal=True)

            delta = 0.0
            for metric in abnormal_metrics:
                if metric in normal_metrics:
                    v1 = normal_metrics[metric]
                    v2 = abnormal_metrics[metric]
                    if v1 and v2:
                        delta += abs((sum(v2) / len(v2)) - (sum(v1) / len(v1)))
            service_deltas[service] = delta

        # Select top-k services
        topk_services = sorted(service_deltas, key=lambda x: service_deltas[x], reverse=True)[:k]

        # Calculate distances - select maximum distance among root cause services
        distances = []
        for service in topk_services:
            max_distance = 0
            for root_service in self.root_services:
                try:
                    d = nx.shortest_path_length(self.graph, service, root_service)
                    max_distance = max(max_distance, d)
                except Exception:
                    continue
            distances.append(max_distance)

        return distances[0] if k == 1 else distances

    def compute_ac(self, service_name: str | None = None) -> dict[str, int]:
        """
        Compute Anomaly Cardinality (AC).

        This metric counts the number of golden signal metrics that are detected
        as anomalous for each service during the fault injection period.

        Formula:
        $$AC_s = |\\{m \\in M_s : \\text{isAnomalous}(m)\\}|$$

        Where:
        - $AC_s$ = anomaly cardinality for service $s$
        - $M_s$ = set of golden signal metrics for service $s$
        - $\\text{isAnomalous}(m)$ = anomaly detection function for metric $m$
        - A metric is considered anomalous if:
            $\\frac{|\\bar{m}_{abnormal} - \\bar{m}_{normal}|}{|\\bar{m}_{normal}|} > \\theta$
        - $\\theta = 0.2$ (20% change threshold)
        - $\\bar{m}_{normal/abnormal}$ = mean value of metric $m$ in normal/abnormal period

        Args:
            service_name (str | None): Specific service name, or None for all services

        Returns:
            dict[str, int]: Mapping from service name to anomaly cardinality
        """
        result = {}
        services = [service_name] if service_name else self.services

        for service in services:
            normal_metrics = self.loader.get_service_metrics(service, abnormal=False)
            abnormal_metrics = self.loader.get_service_metrics(service, abnormal=True)

            anomaly_count = 0

            # Check each metric for anomalies
            for metric in abnormal_metrics:
                if metric in normal_metrics:
                    normal_values = normal_metrics[metric]
                    abnormal_values = abnormal_metrics[metric]

                    if normal_values and abnormal_values:
                        normal_mean = statistics.mean(normal_values)
                        abnormal_mean = statistics.mean(abnormal_values)

                        # Simple anomaly detection: change > 20% threshold
                        if normal_mean != 0:
                            change_ratio = abs(abnormal_mean - normal_mean) / abs(normal_mean)
                            if change_ratio > 0.2:  # 20% threshold
                                anomaly_count += 1
                        elif abnormal_mean != 0:  # Normal period is 0 but abnormal period is not
                            anomaly_count += 1

            result[service] = anomaly_count

        return result

    def compute_cpl(self) -> float:
        """
        Compute Causal Path Length (CPL).

        This metric measures the shortest path length from root cause services
        to the entry service of the system. When multiple root cause services
        exist, the maximum path length is selected.

        Formula:
        $$CPL = \\max_{r \\in R} d(r, e)$$

        Where:
        - $CPL$ = causal path length
        - $R$ = set of root cause services
        - $e$ = entry service (typically the load generator service)
        - $d(r, e)$ = shortest path distance from root cause $r$ to entry service $e$

        Returns:
            float: Maximum causal path length among all root cause services
        """
        if not self.root_services or not self.graph:
            return 0.0

        entry_service = self.loader.get_entry_service()
        if not entry_service:
            return 0.0

        if entry_service not in self.graph:
            logger.warning(f"Entry service '{entry_service}' not found in graph")
            return 0.0

        max_path_length = 99999
        for root_service in self.root_services:
            try:
                path_length = nx.shortest_path_length(self.graph, entry_service, root_service)
                max_path_length = min(max_path_length, path_length)
            except Exception as e:
                logger.error(e)
                continue

        return max_path_length

    def get_root_cause_degree(self) -> int | None:
        """
        Get the root cause service with maximum degree.

        Formula:
        $$r^* = \\arg\\max_{r \\in R} \\deg(r)$$

        Where:
        - $r^*$ = root cause service with maximum degree
        - $R$ = set of root cause services
        - $\\deg(r)$ = degree of service $r$ in the dependency graph

        Returns:
            str | None: Service name with maximum degree, or None if no root services exist
        """
        if not self.root_services or not self.graph:
            return None

        max_degree = -1

        for root_service in self.root_services:
            if root_service in self.graph:
                degree = int(self.graph.degree[root_service])  # type: ignore
                if degree > max_degree:
                    max_degree = degree

        return max_degree

    def calculate_and_report(self):
        results = {}
        results["SDD@1"] = self.compute_sdd(k=1)
        results["SDD@3"] = self.compute_sdd(k=3)
        results["SDD@5"] = self.compute_sdd(k=5)
        results["AC"] = self.compute_ac()
        results["CPL"] = self.compute_cpl()
        results["RootServiceDegree"] = self.get_root_cause_degree()

        with RCABenchClient() as client:
            api = InjectionsApi(client)
            api.api_v2_injections_name_labels_patch(
                name=self.loader.get_datapack(),
                manage=DtoInjectionV2CustomLabelManageReq(
                    add_labels=[
                        DtoLabelItem(key="SDD@1", value=str(results["SDD@1"])),
                        DtoLabelItem(key="SDD@3", value=str(results["SDD@3"])),
                        DtoLabelItem(key="SDD@5", value=str(results["SDD@5"])),
                        DtoLabelItem(key="CPL", value=str(results["CPL"])),
                        DtoLabelItem(key="RootServiceDegree", value=str(results["RootServiceDegree"])),
                    ]
                ),
            )
        return results
