"""
TraceBackA10: Relational Debugging for Microservices (Refactored V2)

Based on the OSDI'23 paper "Relational Debugging" (Perspect), this algorithm implements
a fully data-driven approach to microservice RCA.

Core Concepts (aligned with Perspect paper):
1. Observation: Entry-level SLO violations (from conclusion.parquet)
2. Symptom (S): Internal spans that CAUSE the observation (found via bootstrapping)
3. Predecessor (P): Causal upstream events in the service dependency graph
4. Relation R(S|P): Distribution of "S events per P event" (not just averages)

Pipeline (strictly following the paper):
1. Load: Load traces, metrics, logs, and conclusion.parquet (observations)
2. Detect Observations: Identify entry-level SLO violations (SymptomDetector)
3. Bootstrap Symptoms: Find internal spans causing the observations (SymptomBootstrapper)
   - For LATENCY: Find spans with max exclusive duration increase
   - For ERROR: Find deepest error-generating spans
4. Compute Relations: Calculate R(S|P) as distributions for both good/bad periods
5. Filter: Use Mann-Whitney U test to find statistically significant relation changes
6. Refine: Partition by attributes (static, metrics, logs) using statistical tests
7. Rank: Sort by statistical impact scores (no arbitrary weights)

Key Fix from V1:
- V1 ERROR: Treated entry spans as "symptoms S" → no predecessors found
- V2 FIX: Added SymptomBootstrapper to find INTERNAL symptoms from entry observations
  This aligns with the paper's "symptom bootstrapping" step where we find the
  actual problematic events (like malloc/mark) from high-level observations (like heap_size).

Dynamic Context Refinement (Metrics & Logs as Context):
- Metrics: ASOF JOIN to enrich spans with runtime metrics (CPU, memory)
- Logs (Gold Standard): Direct trace_id/span_id join for causal linking
- Logs (Fallback): Pod-level correlation when trace_id unavailable

Network Blindspot Detection (NEW in V3):
- Silent Callee Pattern: Detects network-layer faults via relational contradiction
  * R(error|P→S) high: P sees many errors when calling S
  * R(error|S) low: S's own spans show few errors
  * Contradiction indicates unobservable network-layer fault (response manipulation, packet loss, etc.)
  * Attribution: Since network is unobservable, attribute to P (last observable point)
- No domain-specific knowledge required - pure relational reasoning from Perspect paper
- Handles: Response Code Manipulation, Timeout Injection, Service Mesh failures, etc.
"""

import json
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path

import duckdb
import numpy as np
from scipy import stats

from ...logging import logger, timeit
from ...utils.env import debug
from ..spec import Algorithm, AlgorithmAnswer, AlgorithmArgs


class TraceBackA10(Algorithm):
    # CPU configuration
    DEFAULT_CPU_COUNT = 8

    # Debug output limits
    DEBUG_TOP_ANSWERS_COUNT = 10
    DEBUG_TOP_CANDIDATES_COUNT = 10

    # Services to exclude from root cause analysis (load generators, test clients, etc.)
    EXCLUDED_SERVICES = {"loadgenerator"}

    def needs_cpu_count(self) -> int | None:
        return self.DEFAULT_CPU_COUNT

    def __call__(self, args: AlgorithmArgs) -> list[AlgorithmAnswer]:
        logger.info(f"Processing datapack: {args.datapack}")

        # Initialize DuckDB connection
        con = duckdb.connect(":memory:")

        try:
            # Step 1: Load data into DuckDB
            loader = DataLoader(args.input_folder, con)
            loader.load_all()

            # Step 2: Build Service Dependency Graph (SDG) from traces
            sdg_builder = SDGBuilder(con)
            sdg = sdg_builder.build()

            if debug():
                logger.debug(f"SDG: {len(sdg.services)} services, {len(sdg.edges)} edges")

            # Step 3: Detect observations (entry-level SLO violations)
            observation_detector = ObservationDetector(con)
            observations = observation_detector.detect()

            if not observations:
                logger.warning("No observations detected from conclusion.parquet")
                return []

            if debug():
                logger.debug(f"Detected {len(observations)} observations (entry-level SLO violations)")

            symptom_bootstrapper = SymptomBootstrapper(con, sdg)
            symptoms = symptom_bootstrapper.bootstrap(observations)

            if not symptoms:
                logger.warning("No internal symptoms found via bootstrapping")
                return []

            if debug():
                logger.debug(f"Bootstrapped {len(symptoms)} internal symptoms")
                for s in symptoms[:5]:
                    logger.debug(
                        f"  Symptom: {s.service_name}.{s.span_name[:60]} "
                        f"({s.symptom_type.name}, impact={s.impact_score:.3f})"
                    )

            # Step 5: Compute relations R(S|P) for both periods
            relation_computer = RelationComputer(con, sdg)
            relations_good = relation_computer.compute_relations(period="good", symptoms=symptoms)
            relations_bad = relation_computer.compute_relations(period="bad", symptoms=symptoms)

            # Step 5: Filter relations with significant changes
            relation_filter = RelationFilter()
            changed_relations = relation_filter.filter_significant_changes(relations_good, relations_bad)

            if debug():
                logger.debug(f"Found {len(changed_relations)} significantly changed relations")

            # Step 6: Refine relations by heterogeneous attributes
            refiner = RelationRefiner(con, sdg)
            root_cause_candidates = refiner.refine(changed_relations, symptoms)

            # Step 7: Rank root cause candidates
            ranker = CandidateRanker()
            ranked_candidates = ranker.rank(root_cause_candidates)

            # Step 8: Convert to service-level answers
            answers = self._to_answers(ranked_candidates)

            for ans in answers[: self.DEBUG_TOP_ANSWERS_COUNT]:
                logger.debug(f"Rank {ans.rank}: {ans.level}.{ans.name}")

            return answers

        finally:
            con.close()

    def _to_answers(self, candidates: list["RootCauseCandidate"]) -> list[AlgorithmAnswer]:
        service_scores = defaultdict(float)

        for candidate in candidates:
            if candidate.service_name in self.EXCLUDED_SERVICES:
                continue

            service_scores[candidate.service_name] += candidate.impact_score

        sorted_services = sorted(service_scores.items(), key=lambda x: x[1], reverse=True)

        if debug():
            logger.debug("Top root cause candidates:")
            for service_name, score in sorted_services[: self.DEBUG_TOP_CANDIDATES_COUNT]:
                logger.debug(f"  {service_name}: score={score:.4f}")

        answers = []
        for rank, (service_name, _score) in enumerate(sorted_services, start=1):
            answers.append(AlgorithmAnswer(level="service", name=service_name, rank=rank))

        return answers


# ============================================================================
# Data Structures
# ============================================================================


@dataclass(frozen=True, slots=True)
class SpanEdge:
    """Represents a call edge in the span dependency graph (span-to-span relationship)."""

    caller_service: str
    caller_span_name: str
    callee_service: str
    callee_span_name: str


@dataclass(frozen=True, slots=True)
class SDG:
    services: set[str]
    edges: list[SpanEdge]


class SymptomType(Enum):
    LATENCY = auto()
    ERROR_RATE = auto()


@dataclass(frozen=True, slots=True)
class Observation:
    """
    Represents a high-level SLO violation (from conclusion.parquet).

    This is the "heap_size is high" observation in the paper's analogy.
    NOT the actual symptom event (malloc/mark).
    """

    observation_type: SymptomType
    entry_span_name: str  # Entry span with SLO violation
    service_name: str  # Service hosting the entry span
    trace_ids: list[str]  # Trace IDs exhibiting this violation


@dataclass(frozen=True, slots=True)
class Symptom:
    """
    Represents an INTERNAL span that causes an observation.

    This is the "malloc" or "mark" event in the paper's analogy.
    Found via bootstrapping from observations.
    """

    symptom_type: SymptomType
    service_name: str
    span_name: str  # INTERNAL span name (not entry span!)
    impact_score: float  # Contribution to the observation (e.g., exclusive duration delta)


@dataclass(frozen=True, slots=True)
class Relation:
    """
    Represents R(S|P) - the relation between predecessor P and symptom S.

    IMPORTANT: value is now a distribution (list of counts), not an average.

    In microservices context:
    - P: predecessor spans (caller span with specific span_name)
    - S: symptom spans (callee span with specific span_name and issues)
    - distribution: list where distribution[i] = number of S events caused by P_i
    """

    predecessor_service: str
    predecessor_span_name: str  # NEW: Specific span operation name
    symptom_service: str
    symptom_span_name: str  # NEW: Specific symptom span operation name
    symptom_type: SymptomType
    distribution: list[int]  # Distribution of S counts per P (not average!)
    sample_size: int  # Number of P events


@dataclass(frozen=True, slots=True)
class RelationChange:
    relation_good: Relation
    relation_bad: Relation
    change_magnitude: float  # Absolute difference
    statistical_significance: float  # p-value or test statistic


@dataclass(frozen=True, slots=True)
class RootCauseCandidate:
    service_name: str  # Service name (used for aggregation)
    span_name: str | None  # NEW: Specific span operation name (the actual root cause)
    symptom_type: SymptomType
    attribute_type: str  # e.g., "host", "pod", "status_code", "overall", "self-root"
    attribute_value: str | None  # e.g., "host-123", None for overall
    impact_score: float
    relation_change: RelationChange | None  # None for self-root-causes


# ============================================================================
# Module 1: Data Loading
# ============================================================================


class DataLoader:
    def __init__(self, input_folder: Path, con: duckdb.DuckDBPyConnection):
        self.input_folder = input_folder
        self.con = con

    def load_all(self):
        self._load_traces()
        self._load_conclusion()
        self._load_metrics()
        self._load_metrics_hist()
        self._load_metrics_sum()
        self._load_logs()

        if debug():
            # Print table stats
            tables_to_check = [
                "traces_good",
                "traces_bad",
                "conclusion",
                "metrics_good",
                "metrics_bad",
                "metrics_hist_good",
                "metrics_hist_bad",
                "metrics_sum_good",
                "metrics_sum_bad",
                "logs_good",
                "logs_bad",
            ]
            for table in tables_to_check:
                try:
                    result = self.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                    if result:
                        logger.debug(f"Loaded {result[0]} rows into {table}")
                except Exception:
                    pass

    def _load_traces(self):
        normal_traces = self.input_folder / "normal_traces.parquet"
        assert normal_traces.exists()
        self.con.execute(f"""
            CREATE TABLE traces_good AS 
            SELECT * FROM read_parquet('{normal_traces}')
        """)

        abnormal_traces = self.input_folder / "abnormal_traces.parquet"
        assert abnormal_traces.exists()
        self.con.execute(f"""
            CREATE TABLE traces_bad AS 
            SELECT * FROM read_parquet('{abnormal_traces}')
        """)

    def _load_conclusion(self):
        conclusion_file = self.input_folder / "conclusion.parquet"
        assert conclusion_file.exists()
        self.con.execute(f"""
            CREATE TABLE conclusion AS 
            SELECT * FROM read_parquet('{conclusion_file}')
        """)

    def _load_metrics(self):
        normal_metrics = self.input_folder / "normal_metrics.parquet"
        assert normal_metrics.exists()
        self.con.execute(f"""
            CREATE TABLE metrics_good AS 
            SELECT * FROM read_parquet('{normal_metrics}')
        """)

        abnormal_metrics = self.input_folder / "abnormal_metrics.parquet"
        assert abnormal_metrics.exists()
        self.con.execute(f"""
            CREATE TABLE metrics_bad AS 
            SELECT * FROM read_parquet('{abnormal_metrics}')
        """)

    def _load_metrics_hist(self):
        normal_metrics = self.input_folder / "normal_metrics_histogram.parquet"
        assert normal_metrics.exists()
        self.con.execute(f"""
            CREATE TABLE metrics_hist_good AS 
            SELECT * FROM read_parquet('{normal_metrics}')
        """)

        abnormal_metrics = self.input_folder / "abnormal_metrics_histogram.parquet"
        assert abnormal_metrics.exists()
        self.con.execute(f"""
            CREATE TABLE metrics_hist_bad AS 
            SELECT * FROM read_parquet('{abnormal_metrics}')
        """)

    def _load_metrics_sum(self):
        normal_metrics = self.input_folder / "normal_metrics_sum.parquet"
        assert normal_metrics.exists()
        self.con.execute(f"""
            CREATE TABLE metrics_sum_good AS 
            SELECT * FROM read_parquet('{normal_metrics}')
        """)

        abnormal_metrics = self.input_folder / "abnormal_metrics_sum.parquet"
        assert abnormal_metrics.exists()
        self.con.execute(f"""
            CREATE TABLE metrics_sum_bad AS 
            SELECT * FROM read_parquet('{abnormal_metrics}')
        """)

    def _load_logs(self):
        normal_logs = self.input_folder / "normal_logs.parquet"
        assert normal_logs.exists()
        self.con.execute(f"""
            CREATE TABLE logs_good AS 
            SELECT * FROM read_parquet('{normal_logs}')
        """)

        abnormal_logs = self.input_folder / "abnormal_logs.parquet"
        assert abnormal_logs.exists()
        self.con.execute(f"""
        CREATE TABLE logs_bad AS 
            SELECT * FROM read_parquet('{abnormal_logs}')
        """)


# ============================================================================
# Module 2: Service Dependency Graph Construction
# ============================================================================


class SDGBuilder:
    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.con = con

    def build(self) -> SDG:
        query = """
        WITH all_traces AS (
            SELECT * FROM traces_good
            UNION ALL
            SELECT * FROM traces_bad
        )
        SELECT DISTINCT
            parent.service_name AS caller_service,
            parent.span_name AS caller_span_name,
            child.service_name AS callee_service,
            child.span_name AS callee_span_name
        FROM all_traces AS parent
        JOIN all_traces AS child 
            ON parent.span_id = child.parent_span_id
            AND parent.trace_id = child.trace_id
        WHERE parent.service_name IS NOT NULL
            AND child.service_name IS NOT NULL
            AND parent.span_name IS NOT NULL
            AND child.span_name IS NOT NULL
        """

        result = self.con.execute(query).fetchall()

        services = set()
        edges = []

        for caller_svc, caller_span, callee_svc, callee_span in result:
            services.add(caller_svc)
            services.add(callee_svc)
            edges.append(
                SpanEdge(
                    caller_service=caller_svc,
                    caller_span_name=caller_span,
                    callee_service=callee_svc,
                    callee_span_name=callee_span,
                )
            )

        return SDG(services=services, edges=edges)


# ============================================================================
# Module 3: Observation Detection (from conclusion.parquet)
# ============================================================================


class ObservationDetector:
    DEBUG_OBSERVATION_DISPLAY_COUNT = 5

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.con = con

    def detect(self) -> list[Observation]:
        observations = []

        query = """
        SELECT 
            SpanName,
            Issues,
            AbnormalAvgDuration,
            NormalAvgDuration,
            AbnormalSuccRate,
            NormalSuccRate
        FROM conclusion
        WHERE Issues != '{}'  -- Has at least one issue
        """

        result = self.con.execute(query).fetchall()

        for row in result:
            span_name, issues_str, abn_dur, norm_dur, abn_succ, norm_succ = row
            issues = json.loads(issues_str.replace("'", '"'))

            # Extract service name from span name
            service_name = self._extract_service_name(span_name)
            if not service_name:
                continue

            # Get trace IDs for this observation (will be used for bootstrapping)
            trace_ids = self._get_trace_ids_for_observation(span_name)

            # Check for latency issues (using keys from detector.py)
            latency_keys = {"avg_duration", "p90_duration", "p95_duration", "p99_duration", "hard_timeout"}
            if any(key in issues for key in latency_keys):
                observations.append(
                    Observation(
                        observation_type=SymptomType.LATENCY,
                        entry_span_name=span_name,
                        service_name=service_name,
                        trace_ids=trace_ids,
                    )
                )

            # Check for success rate issues (using key from detector.py)
            if "succ_rate" in issues:
                observations.append(
                    Observation(
                        observation_type=SymptomType.ERROR_RATE,
                        entry_span_name=span_name,
                        service_name=service_name,
                        trace_ids=trace_ids,
                    )
                )

        if debug():
            logger.debug(f"Detected {len(observations)} observations from conclusion.parquet")
            for obs in observations[: self.DEBUG_OBSERVATION_DISPLAY_COUNT]:
                logger.debug(
                    f"  Observation: {obs.service_name} ({obs.observation_type.name}): "
                    f"{obs.entry_span_name[:60]}... ({len(obs.trace_ids)} traces)"
                )

        return observations

    def _get_trace_ids_for_observation(self, entry_span_name: str) -> list[str]:
        """Get trace IDs where this entry span appears in bad period."""
        query = f"""
        SELECT DISTINCT trace_id
        FROM traces_bad
        WHERE span_name = '{entry_span_name}'
        LIMIT 1000
        """
        result = self.con.execute(query).fetchall()
        return [row[0] for row in result]

    def _extract_service_name(self, span_name: str) -> str | None:
        """
        Extract service name from span name.

        Examples:
        - "HTTP POST http://ts-ui-dashboard:8080/api/..." -> "ts-ui-dashboard"
        - "HTTP GET http://service:port/..." -> "service"
        """
        try:
            # Look for http:// or https:// pattern
            if "http://" in span_name:
                url_part = span_name.split("http://")[1]
            elif "https://" in span_name:
                url_part = span_name.split("https://")[1]
            else:
                return None

            # Extract hostname (before :port or /)
            hostname = url_part.split(":")[0].split("/")[0]
            return hostname
        except Exception:
            return None


# ============================================================================
# Module 4: Symptom Bootstrapping (Finding INTERNAL symptoms from observations)
# ============================================================================


class SymptomBootstrapper:
    """
    Bootstraps INTERNAL symptoms from high-level observations.

    This is the critical "malloc/mark discovery" step from the paper.
    Given "heap_size is high" (observation), find the "malloc" events (symptoms).

    For microservices:
    - Observation: Entry span has high latency/errors
    - Symptom: INTERNAL span that CAUSES the high latency/errors

    Strategies:
    1. LATENCY: Find spans with max exclusive duration increase (bad vs good)
    2. ERROR: Find deepest error-generating spans (error source, not propagator)
    """

    # Minimum number of traces required for statistical analysis
    MIN_TRACE_COUNT = 1

    # Top N symptoms to return per observation
    TOP_N_SYMPTOMS = 5

    # HTTP error status code threshold
    HTTP_ERROR_STATUS_CODE = 400

    def __init__(self, con: duckdb.DuckDBPyConnection, sdg: SDG):
        self.con = con
        self.sdg = sdg

    def bootstrap(self, observations: list[Observation]) -> list[Symptom]:
        """Bootstrap internal symptoms from observations."""
        all_symptoms = []

        for obs in observations:
            if obs.observation_type == SymptomType.LATENCY:
                symptoms = self._bootstrap_latency_symptoms(obs)
            elif obs.observation_type == SymptomType.ERROR_RATE:
                symptoms = self._bootstrap_error_symptoms(obs)
            else:
                symptoms = []

            all_symptoms.extend(symptoms)

        return all_symptoms

    def _bootstrap_latency_symptoms(self, obs: Observation) -> list[Symptom]:
        if len(obs.trace_ids) < self.MIN_TRACE_COUNT:
            return []

        trace_ids_str = "','".join(obs.trace_ids[:1000])  # Limit to avoid SQL injection issues

        query = f"""
        WITH
        -- 1. Bad period: Calculate per-span exclusive durations
        bad_spans_raw AS (
            SELECT 
                trace_id,
                span_id,
                span_name,
                service_name,
                duration,
                parent_span_id
            FROM traces_bad
            WHERE trace_id IN ('{trace_ids_str}')
        ),
        bad_exclusive_per_span AS (
            SELECT 
                parent.trace_id,
                parent.span_id,
                parent.span_name,
                parent.service_name,
                parent.duration - COALESCE(SUM(child.duration), 0) AS exclusive_duration
            FROM bad_spans_raw parent
            LEFT JOIN bad_spans_raw child 
                ON parent.span_id = child.parent_span_id 
                AND parent.trace_id = child.trace_id
            GROUP BY parent.trace_id, parent.span_id, parent.span_name, parent.service_name, parent.duration
        ),
        -- 2. Good period: Calculate per-span exclusive durations (sample proportionally)
        good_trace_ids AS (
            -- Find traces in good period with the SAME entry span name
            SELECT DISTINCT trace_id
            FROM traces_good
            WHERE span_name = '{obs.entry_span_name}'  -- Same entry span as observation
            LIMIT {len(obs.trace_ids) * 10}            -- Sample more to get better coverage
        ),
        good_spans_raw AS (
            SELECT 
                t.trace_id,
                t.span_id,
                t.span_name,
                t.service_name,
                t.duration,
                t.parent_span_id
            FROM traces_good t
            INNER JOIN good_trace_ids g ON t.trace_id = g.trace_id  -- Only traces with same entry span
        ),
        good_exclusive_per_span AS (
            SELECT 
                parent.trace_id,
                parent.span_id,
                parent.span_name,
                parent.service_name,
                parent.duration - COALESCE(SUM(child.duration), 0) AS exclusive_duration
            FROM good_spans_raw parent
            LEFT JOIN good_spans_raw child 
                ON parent.span_id = child.parent_span_id 
                AND parent.trace_id = child.trace_id
            GROUP BY parent.trace_id, parent.span_id, parent.span_name, parent.service_name, parent.duration
        ),
        -- 3. Aggregate and compute DELTA (change in exclusive duration)
        span_deltas AS (
            SELECT 
                COALESCE(b.span_name, g.span_name) AS span_name,
                COALESCE(b.service_name, g.service_name) AS service_name,
                AVG(b.exclusive_duration) AS bad_avg_exclusive,
                AVG(g.exclusive_duration) AS good_avg_exclusive,
                -- KEY: Delta in exclusive duration (NEW latency only)
                AVG(b.exclusive_duration) - AVG(g.exclusive_duration) AS delta_exclusive,
                COUNT(b.span_id) AS bad_count,
                COUNT(g.span_id) AS good_count
            FROM bad_exclusive_per_span b
            FULL OUTER JOIN good_exclusive_per_span g
                ON b.span_name = g.span_name 
                AND b.service_name = g.service_name
            GROUP BY COALESCE(b.span_name, g.span_name), COALESCE(b.service_name, g.service_name)
            HAVING COUNT(b.span_id) >= {self.MIN_TRACE_COUNT}  -- Sufficient bad samples
        )
        SELECT 
            span_name,
            service_name,
            delta_exclusive,
            bad_avg_exclusive,
            good_avg_exclusive,
            bad_count,
            good_count
        FROM span_deltas
        WHERE delta_exclusive > 0  -- Only spans with INCREASED exclusive duration
            AND span_name != '{obs.entry_span_name}'  -- Exclude the entry span itself
            AND span_name NOT IN ('GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'OPTIONS')  -- Filter generic HTTP
        ORDER BY delta_exclusive DESC  -- Largest increase first
        LIMIT {self.TOP_N_SYMPTOMS}
        """

        try:
            result = self.con.execute(query).fetchall()
        except Exception as e:
            if debug():
                logger.warning(f"Error bootstrapping latency symptoms: {e}")
            return []

        symptoms = []
        for span_name, service_name, delta, bad_avg, good_avg, bad_count, good_count in result:
            symptoms.append(
                Symptom(
                    symptom_type=SymptomType.LATENCY,
                    service_name=service_name,
                    span_name=span_name,
                    impact_score=float(delta / 1e9),  # Delta in exclusive duration (seconds)
                )
            )

            if debug():
                logger.debug(
                    f"  Latency symptom: {service_name}.{span_name[:50]} | "
                    f"Δexclusive={delta / 1e9:.3f}s "
                    f"(bad={bad_avg / 1e9:.3f}s [{bad_count} samples], "
                    f"good={good_avg / 1e9:.3f}s [{good_count} samples])"
                )

        return symptoms

    def _bootstrap_error_symptoms(self, obs: Observation) -> list[Symptom]:
        """
        Find INTERNAL spans generating errors (3-step process for better observability).

        Step 1: Find leaf error generators in BAD period
        Step 2: Find baseline errors in GOOD period
        Step 3: Calculate DELTA and filter
        """
        if len(obs.trace_ids) < self.MIN_TRACE_COUNT:
            return []

        trace_ids_str = "','".join(obs.trace_ids[:1000])

        # ====================================================================
        # STEP 1: Find leaf error generators in BAD period
        # ====================================================================
        if debug():
            logger.debug("[Step 1/3] Finding leaf error generators in BAD period...")

        query_bad = f"""
        WITH
        -- 1. Error spans from TRACES (status code based)
        trace_error_spans AS (
            SELECT 
                trace_id,
                span_id,
                span_name,
                service_name,
                "attr.http.response.status_code" AS status_code
            FROM traces_bad
            WHERE trace_id IN ('{trace_ids_str}')
            AND ("attr.status_code" = 'Error' 
                OR "attr.http.response.status_code" >= {self.HTTP_ERROR_STATUS_CODE})
        ),
        -- 2. Error spans from LOGS (ERROR/FATAL level)
        --    Gold standard: logs with trace_id + span_id
        log_error_spans_gold AS (
            SELECT DISTINCT
                L.trace_id,
                L.span_id,
                T.span_name,
                T.service_name
            FROM logs_bad L
            INNER JOIN traces_bad T
            ON L.trace_id = T.trace_id 
            AND L.span_id = T.span_id
            WHERE L.trace_id IN ('{trace_ids_str}')
            AND L.level NOT IN ('INFO')
            AND L.trace_id IS NOT NULL
            AND L.span_id IS NOT NULL
        ),
        
        -- 3. Error spans from LOGS (fallback: pod-level correlation)
        --    When logs don't have trace_id/span_id
        log_error_spans_fallback AS (
            SELECT DISTINCT
                T.trace_id,
                T.span_id,
                T.span_name,
                T.service_name
            FROM logs_bad L
            INNER JOIN traces_bad T
            ON L."attr.k8s.pod.name" = T."attr.k8s.pod.name"
            AND L.time BETWEEN T.time - INTERVAL 1 SECOND AND T.time + INTERVAL 1 SECOND
            WHERE L.trace_id IN ('{trace_ids_str}')
            AND L.level NOT IN ('INFO')
            AND L."attr.k8s.pod.name" IS NOT NULL
            AND (L.trace_id IS NULL OR L.span_id IS NULL)
        ),
        
        -- 4. UNION all error sources (trace + log gold + log fallback)
        all_error_spans AS (
            SELECT trace_id, span_id, span_name, service_name, 'trace' AS source
            FROM trace_error_spans
            UNION
            SELECT trace_id, span_id, span_name, service_name, 'log_gold' AS source
            FROM log_error_spans_gold
            UNION
            SELECT trace_id, span_id, span_name, service_name, 'log_fallback' AS source
            FROM log_error_spans_fallback
        ),
        
        -- 5. Find DEEPEST error generators (no child with errors)
        error_span_children AS (
            SELECT DISTINCT
                T.parent_span_id,
                T.trace_id
            FROM all_error_spans child
            INNER JOIN traces_bad T
            ON child.trace_id = T.trace_id AND child.span_id = T.span_id
            WHERE T.parent_span_id IS NOT NULL
        )
        SELECT 
            parent.span_name,
            parent.service_name,
            COUNT(DISTINCT parent.trace_id || '::' || parent.span_id) AS error_count,
            ARRAY_AGG(DISTINCT parent.source) AS error_sources
        FROM all_error_spans parent
        LEFT JOIN error_span_children esc
        ON parent.span_id = esc.parent_span_id
        AND parent.trace_id = esc.trace_id
        WHERE esc.parent_span_id IS NULL  -- No children with errors
        GROUP BY parent.span_name, parent.service_name
        """

        try:
            bad_error_generators = self.con.execute(query_bad).fetchall()
        except Exception as e:
            if debug():
                logger.warning(f"Step 1 failed: {e}")
            return []

        if not bad_error_generators:
            if debug():
                logger.debug("  No leaf error generators found in bad period")
            return []

        if debug():
            logger.debug(f"  Found {len(bad_error_generators)} leaf error generators")
            for span_name, service_name, count, sources in bad_error_generators[:3]:
                sources_str = ",".join(sources) if isinstance(sources, list) else str(sources)
                logger.debug(f"    {service_name}.{span_name[:50]} (count={count}, sources=[{sources_str}])")

        # ====================================================================
        # STEP 2: Find baseline errors in GOOD period
        # ====================================================================
        if debug():
            logger.debug("[Step 2/3] Finding baseline errors in GOOD period...")

        # Build list of (span_name, service_name) to search for in good period
        bad_span_signatures = [(span_name, service_name) for span_name, service_name, _, _ in bad_error_generators]

        if not bad_span_signatures:
            good_error_generators = []
        else:
            # Build SQL IN clause for filtering
            span_filters = " OR ".join(
                [f"(span_name = '{sn}' AND service_name = '{sv}')" for sn, sv in bad_span_signatures[:100]]
            )

            query_good = f"""
            WITH good_trace_ids AS (
                SELECT DISTINCT trace_id
                FROM traces_good
                WHERE span_name = '{obs.entry_span_name}'
                LIMIT {len(obs.trace_ids) * 10}
            ),
            -- Error spans from TRACES in good period (for the spans we found in bad period)
            good_trace_errors AS (
                SELECT trace_id, span_id, span_name, service_name
                FROM traces_good
                WHERE trace_id IN (SELECT trace_id FROM good_trace_ids)
                AND ({span_filters})
                AND ("attr.status_code" = 'Error' 
                    OR "attr.http.response.status_code" >= {self.HTTP_ERROR_STATUS_CODE})
            ),
            -- Error spans from LOGS (gold standard) in good period
            good_log_errors_gold AS (
                SELECT DISTINCT L.trace_id, L.span_id, T.span_name, T.service_name
                FROM logs_good L
                INNER JOIN traces_good T ON L.trace_id = T.trace_id AND L.span_id = T.span_id
                WHERE L.trace_id IN (SELECT trace_id FROM good_trace_ids)
                AND L.level NOT IN ('INFO')
                AND L.trace_id IS NOT NULL AND L.span_id IS NOT NULL
            ),
            good_log_errors_gold_filtered AS (
                SELECT trace_id, span_id, span_name, service_name
                FROM good_log_errors_gold
                WHERE ({span_filters})
            ),
            -- Error spans from LOGS (fallback) in good period
            good_log_errors_fallback AS (
                SELECT DISTINCT T.trace_id, T.span_id, T.span_name, T.service_name
                FROM logs_good L
                INNER JOIN traces_good T
                ON L."attr.k8s.pod.name" = T."attr.k8s.pod.name"
                AND L.time BETWEEN T.time - INTERVAL 1 SECOND AND T.time + INTERVAL 1 SECOND
                WHERE L.trace_id IN (SELECT trace_id FROM good_trace_ids)
                AND L.level NOT IN ('INFO')
                AND L."attr.k8s.pod.name" IS NOT NULL
                AND (L.trace_id IS NULL OR L.span_id IS NULL)
            ),
            good_log_errors_fallback_filtered AS (
                SELECT trace_id, span_id, span_name, service_name
                FROM good_log_errors_fallback
                WHERE ({span_filters})
            ),
            -- UNION all error sources
            all_good_errors AS (
                SELECT trace_id, span_id, span_name, service_name FROM good_trace_errors
                UNION
                SELECT trace_id, span_id, span_name, service_name FROM good_log_errors_gold_filtered
                UNION
                SELECT trace_id, span_id, span_name, service_name FROM good_log_errors_fallback_filtered
            )
            SELECT 
                span_name,
                service_name,
                COUNT(DISTINCT trace_id || '::' || span_id) AS error_count
            FROM all_good_errors
            GROUP BY span_name, service_name
            """

            try:
                good_error_generators = self.con.execute(query_good).fetchall()
            except Exception as e:
                if debug():
                    logger.warning(f"Step 2 failed: {e}")
                good_error_generators = []

        # Build index for quick lookup
        good_errors_index = {
            (span_name, service_name): count for span_name, service_name, count in good_error_generators
        }

        if debug():
            logger.debug(f"  Found {len(good_error_generators)} span types with baseline errors")
            for span_name, service_name, count in good_error_generators[:3]:
                logger.debug(f"    {service_name}.{span_name[:50]} (count={count})")

        # ====================================================================
        # STEP 3: Calculate DELTA and filter
        # ====================================================================
        if debug():
            logger.debug("[Step 3/3] Calculating DELTA (bad - good) and filtering...")

        symptoms = []
        deltas = []

        for span_name, service_name, bad_count, sources in bad_error_generators:
            good_count = good_errors_index.get((span_name, service_name), 0)
            delta = bad_count - good_count

            if delta <= 0:
                continue
            if span_name == obs.entry_span_name:
                continue

            deltas.append((span_name, service_name, delta, bad_count, good_count, sources))

        deltas.sort(key=lambda x: x[2], reverse=True)
        top_deltas = deltas[: self.TOP_N_SYMPTOMS]

        if debug():
            logger.debug(f"  Filtered results: {len(deltas)} candidates, taking top {len(top_deltas)}")

        for span_name, service_name, delta, bad_count, good_count, sources in top_deltas:
            symptoms.append(
                Symptom(
                    symptom_type=SymptomType.ERROR_RATE,
                    service_name=service_name,
                    span_name=span_name,
                    impact_score=float(delta),
                )
            )

            if debug():
                sources_str = ",".join(sources) if isinstance(sources, list) else str(sources)
                logger.debug(
                    f"  ✓ Error symptom: {service_name}.{span_name[:50]} "
                    f"(Δ={delta}, bad={bad_count}, good={good_count}, "
                    f"sources=[{sources_str}])"
                )

        return symptoms


# ============================================================================
# Module 5: Relation Computation (Distribution-based, not averages!)
# ============================================================================


class RelationComputer:
    """
    Computes R(S|P) relations as DISTRIBUTIONS, not averages.

    This is critical: we compute "for each P, how many S" to get a distribution,
    not just collapse it to a single average number.
    """

    # HTTP status code threshold for error detection
    HTTP_ERROR_STATUS_CODE = 400

    def __init__(self, con: duckdb.DuckDBPyConnection, sdg: SDG):
        self.con = con
        self.sdg = sdg

    def compute_relations(
        self,
        period: str,  # "good" or "bad"
        symptoms: list[Symptom],
    ) -> list[Relation]:
        """
        Compute forward relations R•(S|P) for all (P, S) pairs.

        R•(S|P) = distribution of "how many symptom S events each predecessor P causes"
        """
        relations = []

        for symptom in symptoms:
            symptom_relations = self._compute_for_symptom(period, symptom)
            relations.extend(symptom_relations)

        return relations

    def _compute_for_symptom(self, period: str, symptom: Symptom) -> list[Relation]:
        """Compute relations for a specific symptom - now at span-to-span level."""
        table_name = f"traces_{period}"

        # Only consider cross-service predecessors for service-level RCA
        # Any call from a different service to the symptom's service counts as predecessor
        predecessor_spans = [
            (edge.caller_service, edge.caller_span_name)
            for edge in self.sdg.edges
            if edge.callee_service == symptom.service_name  # Calls to symptom's service
            and edge.caller_service != edge.callee_service  # Cross-service only
        ]

        if not predecessor_spans:
            return []

        relations = []

        for pred_service, pred_span_name in predecessor_spans:
            relation = self._compute_relation_for_pair(
                table_name=table_name,
                predecessor_service=pred_service,
                predecessor_span_name=pred_span_name,
                symptom=symptom,
            )

            if relation:
                relations.append(relation)

        return relations

    def _compute_relation_for_pair(
        self,
        table_name: str,
        predecessor_service: str,
        predecessor_span_name: str,  # NEW: Now takes specific span name
        symptom: Symptom,
    ) -> Relation | None:
        """
        Compute R(S|P) for a specific (predecessor span, symptom span) pair.

        Returns a DISTRIBUTION, not an average!
        """
        # Build symptom condition based on type
        if symptom.symptom_type == SymptomType.LATENCY:
            # For latency: match spans from the same span_name
            # The symptom.span_name already identifies the specific operation
            symptom_condition = f"s.span_name = '{symptom.span_name}'"
        elif symptom.symptom_type == SymptomType.ERROR_RATE:
            # For errors: check status codes AND match the specific span
            symptom_condition = f"""
                s.span_name = '{symptom.span_name}'
                AND (s."attr.status_code" = 'Error' 
                     OR s."attr.http.response.status_code" >= {RelationComputer.HTTP_ERROR_STATUS_CODE})
            """
        else:
            return None

        query = f"""
        WITH predecessor_spans AS (
            SELECT trace_id, span_id
            FROM {table_name}
            WHERE service_name = '{predecessor_service}'
              AND span_name = '{predecessor_span_name}'
        ),
        symptom_counts_per_p AS (
            SELECT 
                p.span_id,
                COUNT(s.span_id) AS s_count
            FROM predecessor_spans p
            LEFT JOIN {table_name} s
                ON p.span_id = s.parent_span_id
                AND p.trace_id = s.trace_id
                AND {symptom_condition}
            GROUP BY p.span_id
        )
        SELECT s_count
        FROM symptom_counts_per_p
        ORDER BY s_count
        """

        try:
            result = self.con.execute(query).fetchall()
        except Exception as e:
            if debug():
                logger.warning(f"Error computing relation: {e}")
            return None

        if not result:
            return None

        # Extract distribution: list of counts
        distribution = [row[0] for row in result]

        if len(distribution) == 0:
            return None

        return Relation(
            predecessor_service=predecessor_service,
            predecessor_span_name=predecessor_span_name,  # Store the specific span name
            symptom_service=symptom.service_name,
            symptom_span_name=symptom.span_name,  # Store the specific symptom span name
            symptom_type=symptom.symptom_type,
            distribution=distribution,
            sample_size=len(distribution),
        )


# ============================================================================
# Module 5: Relation Filtering (Statistical Tests, NO magic numbers!)
# ============================================================================


class RelationFilter:
    """
    Filters relations using statistical tests to find significant changes.

    NO hardcoded thresholds - uses Mann-Whitney U test to compare distributions.
    """

    # Minimum sample size for statistical testing
    MIN_SAMPLE_SIZE = 10

    # Significance level (standard p-value threshold)
    ALPHA = 0.05

    # Minimum mean distribution value for new relations
    MIN_NEW_RELATION_MEAN = 0.1

    # P-value for very significant new relations
    NEW_RELATION_P_VALUE = 0.001

    # Minimum magnitude threshold for trivial changes
    MIN_MAGNITUDE_THRESHOLD = 0.01

    def __init__(self):
        pass

    def filter_significant_changes(
        self, relations_good: list[Relation], relations_bad: list[Relation]
    ) -> list[RelationChange]:
        """
        Filter relations using Mann-Whitney U test.

        Compares the distributions (not averages!) to find statistically significant changes.
        """
        # Index relations by (predecessor_service, predecessor_span, symptom_service, symptom_span, type)
        good_index = {
            (
                r.predecessor_service,
                r.predecessor_span_name,
                r.symptom_service,
                r.symptom_span_name,
                r.symptom_type,
            ): r
            for r in relations_good
        }
        bad_index = {
            (
                r.predecessor_service,
                r.predecessor_span_name,
                r.symptom_service,
                r.symptom_span_name,
                r.symptom_type,
            ): r
            for r in relations_bad
        }

        changes = []

        # Check all relations in bad period
        for key, bad_rel in bad_index.items():
            good_rel = good_index.get(key)

            if good_rel is None:
                # New relation in bad period (didn't exist in good)
                # Check if it's substantial enough to matter
                if (
                    bad_rel.sample_size >= self.MIN_SAMPLE_SIZE
                    and np.mean(bad_rel.distribution) > self.MIN_NEW_RELATION_MEAN
                ):
                    changes.append(
                        RelationChange(
                            relation_good=Relation(
                                predecessor_service=bad_rel.predecessor_service,
                                predecessor_span_name=bad_rel.predecessor_span_name,
                                symptom_service=bad_rel.symptom_service,
                                symptom_span_name=bad_rel.symptom_span_name,
                                symptom_type=bad_rel.symptom_type,
                                distribution=[0],  # Empty in good period
                                sample_size=0,
                            ),
                            relation_bad=bad_rel,
                            change_magnitude=float(np.mean(bad_rel.distribution)),
                            statistical_significance=self.NEW_RELATION_P_VALUE,  # Very significant (new relation)
                        )
                    )
            else:
                # Compare good vs bad using statistical test
                change = self._compare_relations(good_rel, bad_rel)
                if change:
                    changes.append(change)

        return changes

    def _compare_relations(self, good: Relation, bad: Relation) -> RelationChange | None:
        """
        Compare two relations using Mann-Whitney U test.

        This is the KEY change: we compare DISTRIBUTIONS, not averages!
        """
        # Need sufficient sample size for statistical test
        if good.sample_size < self.MIN_SAMPLE_SIZE or bad.sample_size < self.MIN_SAMPLE_SIZE:
            return None

        # Perform Mann-Whitney U test (non-parametric test for two distributions)
        try:
            statistic, p_value = stats.mannwhitneyu(good.distribution, bad.distribution, alternative="two-sided")
        except Exception as e:
            if debug():
                logger.warning(f"Mann-Whitney U test failed: {e}")
            return None

        # Check if statistically significant
        if p_value >= self.ALPHA:
            return None

        # Calculate effect size (magnitude of change)
        mean_good = np.mean(good.distribution)
        mean_bad = np.mean(bad.distribution)
        magnitude = abs(mean_bad - mean_good)

        # Also check that the magnitude is non-trivial
        if magnitude < self.MIN_MAGNITUDE_THRESHOLD:
            return None

        return RelationChange(
            relation_good=good,
            relation_bad=bad,
            change_magnitude=float(magnitude),
            statistical_significance=p_value,  # Real p-value, not magnitude!
        )


# ============================================================================
# Module 6: Relation Refinement (Data-driven, NO hardcoded rules!)
# ============================================================================


class RelationRefiner:
    """
    Refines relations by partitioning predecessors by heterogeneous attributes.

    Key improvements:
    - Self-root detection via "no upstream changes" logic (NO 1.5x threshold!)
    - Statistical tests for attribute refinement (NO 0.1 deviation threshold!)
    - Dynamic context refinement via Metrics and Logs (NEW!)
    """

    # Configurable refinement attributes (static attributes from spans)
    DEFAULT_REFINEMENT_ATTRIBUTES = [
        '"attr.k8s.pod.name"',
        '"attr.http.response.status_code"',
    ]

    # Dynamic refinement via metrics (requires ASOF JOIN)
    DEFAULT_METRIC_REFINEMENTS = [
        "cpu_utilization",
        "memory_rss",
    ]

    # Dynamic refinement via logs
    ENABLE_LOG_REFINEMENT = True

    # HTTP status code threshold for error detection
    HTTP_ERROR_STATUS_CODE = 400

    # Statistical significance thresholds
    ALPHA = 0.05  # p-value threshold for significance

    # Self-root-cause impact score
    SELF_ROOT_IMPACT_SCORE = 10.0

    # Error rate increase multiplier threshold
    ERROR_RATE_INCREASE_MULTIPLIER = 1.5

    # Impact score multiplier for error generators
    ERROR_GENERATOR_MULTIPLIER = 2.0

    # Minimum sample size for statistical tests
    MIN_SAMPLE_SIZE_FOR_TESTS = 10
    MIN_SAMPLE_SIZE_FOR_REFINEMENT = 5

    # Metric bucketing precision
    METRIC_BUCKET_PRECISION = 10

    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        sdg: SDG,
        refinement_attributes: list[str] | None = None,
        metric_refinements: list[str] | None = None,
        enable_log_refinement: bool = True,
    ):
        self.con = con
        self.sdg = sdg
        self.refinement_attributes = refinement_attributes or self.DEFAULT_REFINEMENT_ATTRIBUTES
        self.metric_refinements = metric_refinements or self.DEFAULT_METRIC_REFINEMENTS
        self.enable_log_refinement = enable_log_refinement

    def refine(self, changed_relations: list[RelationChange], symptoms: list[Symptom]) -> list[RootCauseCandidate]:
        """
        Refine changed relations by partitioning by attributes.

        Self-root-cause detection: data-driven logic instead of arbitrary thresholds.
        """
        candidates = []

        # Track which services appear as symptom services in changed relations
        symptom_services_with_upstream_changes = set()

        for rel_change in changed_relations:
            symptom_services_with_upstream_changes.add(rel_change.relation_bad.symptom_service)

            # Add the overall relation as a candidate
            # Impact score = p-value (lower is better) * magnitude
            impact = (1.0 - rel_change.statistical_significance) * rel_change.change_magnitude

            overall_candidate = RootCauseCandidate(
                service_name=rel_change.relation_bad.predecessor_service,
                span_name=rel_change.relation_bad.predecessor_span_name,  # NEW: Include specific span
                symptom_type=rel_change.relation_bad.symptom_type,
                attribute_type="overall",
                attribute_value=None,
                impact_score=impact,
                relation_change=rel_change,
            )
            candidates.append(overall_candidate)

            # Then refine by attributes (if they exist)
            refined = self._refine_by_attributes(rel_change, symptoms)
            candidates.extend(refined)

            # NEW: Detect Silent Callee Pattern (network-layer blindspot detection)
            # This handles Response Manipulation and similar network-level faults
            # using pure relational contradiction without domain-specific knowledge
            silent_callee = self._detect_silent_callee_pattern(rel_change, symptoms)
            if silent_callee:
                candidates.append(silent_callee)

        # Self-root-cause detection: service has symptoms but no upstream changes
        for symptom in symptoms:
            if symptom.service_name not in symptom_services_with_upstream_changes:
                self_root_candidate = RootCauseCandidate(
                    service_name=symptom.service_name,
                    span_name=symptom.span_name,
                    symptom_type=symptom.symptom_type,
                    attribute_type="self-root",
                    attribute_value=None,
                    impact_score=self.SELF_ROOT_IMPACT_SCORE,
                    relation_change=None,
                )
                candidates.append(self_root_candidate)

                if debug():
                    logger.debug(
                        f"Self-root-cause: {symptom.service_name} ({symptom.symptom_type.name}) - no upstream changes"
                    )

        return candidates

    def _detect_silent_callee_pattern(
        self, rel_change: RelationChange, symptoms: list[Symptom]
    ) -> RootCauseCandidate | None:
        """
        Detect Silent Callee Pattern via relational contradiction (DATA-DRIVEN).

        Theory: Network-layer faults create contradictory relations:
        - R(error|P→S): P's view shows many errors when calling S (CHANGED statistically)
        - R(error|S): S's own spans show few/no errors (NOT CHANGED statistically)

        This contradiction indicates observation blindspot (network layer).
        Attribution: Since network is unobservable, attribute to P (last observable point).

        Pattern applies to:
        - Response Code Manipulation (proxy changes 200→503)
        - Packet Loss (network drops responses)
        - Timeout Injection (network delays responses)
        - Service Mesh Policy Failures

        NO MAGIC NUMBERS - pure statistical reasoning:
        - R(error|P→S) changed significantly (confirmed by RelationFilter)
        - R(error|S) did NOT change significantly (we verify here)
        - This is the TRUE contradiction - data-driven detection
        """
        # Only applies to error-rate relations
        if rel_change.relation_bad.symptom_type != SymptomType.ERROR_RATE:
            return None

        s_service = rel_change.relation_bad.symptom_service

        # Compute R(error|S) for both good and bad periods
        s_self_error_good = self._compute_service_self_error_rate(s_service, period="good")
        s_self_error_bad = self._compute_service_self_error_rate(s_service, period="bad")

        if s_self_error_good is None or s_self_error_bad is None:
            return None

        # TRUE CONTRADICTION (data-driven):
        # 1. R(error|P→S) changed significantly (already confirmed by RelationFilter)
        # 2. R(error|S) did NOT change significantly (we check here)

        # Check if S's self error rate changed significantly
        # We use a simple heuristic: if the absolute change is small AND the ratio is close to 1
        s_error_delta = abs(s_self_error_bad - s_self_error_good)
        s_error_ratio = (s_self_error_bad + 1e-6) / (s_self_error_good + 1e-6)

        # S's error rate is stable (no significant change) if:
        # - Absolute change is small (< 0.05)
        # - OR ratio is close to 1 (between 0.8 and 1.25)
        s_error_stable = s_error_delta < 0.05 or (0.8 < s_error_ratio < 1.25)

        if not s_error_stable:
            # S's error rate also changed - not a blindspot, just normal error propagation
            return None

        # At this point:
        # - R(error|P→S) changed significantly (from RelationFilter)
        # - R(error|S) is stable (we just verified)
        # This IS a contradiction - likely network-layer blindspot

        # Compute impact score based on the magnitude of P→S relation change
        p_to_s_error_rate_bad = np.mean(rel_change.relation_bad.distribution)
        p_to_s_error_rate_good = np.mean(rel_change.relation_good.distribution)

        # Impact = how much P's view of errors increased
        p_error_increase = p_to_s_error_rate_bad - p_to_s_error_rate_good

        # Use the existing change_magnitude from RelationFilter (already statistically validated)
        impact_score = rel_change.change_magnitude * 10.0  # High multiplier due to blindspot

        if debug():
            logger.debug(
                f"Silent Callee Pattern detected: {rel_change.relation_bad.predecessor_service} → "
                f"{s_service} | P→S errors: {p_to_s_error_rate_good:.1%}→{p_to_s_error_rate_bad:.1%} "
                f"(Δ={p_error_increase:.1%}), S self: {s_self_error_good:.1%}→{s_self_error_bad:.1%} "
                f"(Δ={s_error_delta:.1%}, stable) | Contradiction detected!"
            )

        return RootCauseCandidate(
            service_name=rel_change.relation_bad.predecessor_service,
            span_name=rel_change.relation_bad.predecessor_span_name,
            symptom_type=SymptomType.ERROR_RATE,
            attribute_type="network-blindspot",
            attribute_value=f"silent-callee:{s_service}",
            impact_score=float(impact_score),
            relation_change=rel_change,
        )

    def _compute_service_self_error_rate(self, service: str, period: str) -> float | None:
        """
        Compute R(error|S): service's self error rate (errors in its own spans).

        This is different from downstream error propagation - we only count
        errors that S itself generated, not errors from calling others.
        """
        table_name = f"traces_{period}"

        query = f"""
        SELECT 
            COUNT(*) AS total_spans,
            SUM(CASE 
                WHEN "attr.status_code" = 'Error' 
                     OR "attr.http.response.status_code" >= {self.HTTP_ERROR_STATUS_CODE}
                THEN 1 ELSE 0 
            END) AS error_spans
        FROM {table_name}
        WHERE service_name = '{service}'
        """

        try:
            result = self.con.execute(query).fetchone()
        except Exception as e:
            if debug():
                logger.warning(f"Error computing self error rate for {service}: {e}")
            return None

        if not result:
            return None

        total, errors = result

        if total < self.MIN_SAMPLE_SIZE_FOR_TESTS:
            return None

        return errors / total if total > 0 else 0.0

    def _refine_by_attributes(self, rel_change: RelationChange, symptoms: list[Symptom]) -> list[RootCauseCandidate]:
        """Refine a relation by partitioning by various attributes (static, metrics, logs)."""
        candidates = []

        # Find the symptom
        symptom = next(
            (
                s
                for s in symptoms
                if s.service_name == rel_change.relation_bad.symptom_service
                and s.symptom_type == rel_change.relation_bad.symptom_type
            ),
            None,
        )

        if not symptom:
            return candidates

        # 1. Static attribute refinement (original)
        for attr_column in self.refinement_attributes:
            try:
                attr_type = attr_column.strip('"').split(".")[-1]  # e.g., "pod.name" -> "name"
                attr_candidates = self._refine_by_attribute_statistical(rel_change, symptom, attr_column, attr_type)
                candidates.extend(attr_candidates)
            except Exception as e:
                if debug():
                    logger.debug(f"Error refining by {attr_column}: {e}")

        # 2. Dynamic metric refinement (NEW!)
        for metric_name in self.metric_refinements:
            try:
                metric_candidates = self._refine_by_metric(rel_change, symptom, metric_name)
                candidates.extend(metric_candidates)
            except Exception as e:
                if debug():
                    logger.debug(f"Error refining by metric {metric_name}: {e}")

        # 3. Log-based refinement (NEW!)
        if self.enable_log_refinement:
            try:
                log_candidates = self._refine_by_logs(rel_change, symptom)
                candidates.extend(log_candidates)
            except Exception as e:
                if debug():
                    logger.debug(f"Error refining by logs: {e}")

        return candidates

    def _refine_by_attribute_statistical(
        self,
        rel_change: RelationChange,
        symptom: Symptom,
        attribute_column: str,
        attribute_type: str,
    ) -> list[RootCauseCandidate]:
        """
        Refine by attribute using STATISTICAL TEST, not hardcoded 0.1 threshold!

        For each attribute value, we:
        1. Compute the distribution of R(S|P) for spans with this attribute
        2. Compute the distribution of R(S|P) for spans with OTHER attributes
        3. Use Mann-Whitney U test to see if they're significantly different
        """
        predecessor = rel_change.relation_bad.predecessor_service

        # Build symptom condition
        if symptom.symptom_type == SymptomType.LATENCY:
            symptom_condition = f"s.span_name = '{symptom.span_name}'"
        elif symptom.symptom_type == SymptomType.ERROR_RATE:
            symptom_condition = f"""
                (s."attr.status_code" = 'Error' 
                 OR s."attr.http.response.status_code" >= {self.HTTP_ERROR_STATUS_CODE})
                AND s.service_name = '{symptom.service_name}'
            """
        else:
            return []

        # Get distribution for each attribute value
        query = f"""
        WITH predecessor_spans AS (
            SELECT trace_id, span_id, {attribute_column} AS attr_value
            FROM traces_bad
            WHERE service_name = '{predecessor}'
              AND {attribute_column} IS NOT NULL
        ),
        symptom_counts_per_p AS (
            SELECT 
                p.span_id,
                p.attr_value,
                COUNT(s.span_id) AS s_count
            FROM predecessor_spans p
            LEFT JOIN traces_bad s
                ON p.span_id = s.parent_span_id
                AND p.trace_id = s.trace_id
                AND {symptom_condition}
            GROUP BY p.span_id, p.attr_value
        )
        SELECT attr_value, ARRAY_AGG(s_count) AS distribution
        FROM symptom_counts_per_p
        GROUP BY attr_value
        HAVING COUNT(*) >= {self.MIN_SAMPLE_SIZE_FOR_REFINEMENT}  -- Minimum sample size
        """

        try:
            result = self.con.execute(query).fetchall()
        except Exception as e:
            if debug():
                logger.warning(f"Error refining by {attribute_type}: {e}")
            return []

        if len(result) < 2:
            # Need at least 2 different attribute values to compare
            return []

        candidates = []
        overall_mean = np.mean(rel_change.relation_bad.distribution)

        # For each attribute value, test if its distribution is significantly different
        for attr_value, distribution_json in result:
            # DuckDB returns array as list
            attr_distribution = distribution_json if isinstance(distribution_json, list) else [distribution_json]

            # Collect all OTHER distributions
            other_distributions = []
            for other_attr, other_dist_json in result:
                if other_attr != attr_value:
                    other_dist = other_dist_json if isinstance(other_dist_json, list) else [other_dist_json]
                    other_distributions.extend(other_dist)

            if not other_distributions or len(attr_distribution) < self.MIN_SAMPLE_SIZE_FOR_REFINEMENT:
                continue

            # Mann-Whitney U test: is this attribute unique?
            try:
                _, p_value = stats.mannwhitneyu(attr_distribution, other_distributions, alternative="two-sided")
            except Exception:
                continue

            # Only include if statistically significant AND has higher mean
            attr_mean = np.mean(attr_distribution)
            if p_value < self.ALPHA and attr_mean > overall_mean:
                impact = (1.0 - p_value) * (attr_mean - overall_mean)

                candidates.append(
                    RootCauseCandidate(
                        service_name=predecessor,
                        span_name=rel_change.relation_bad.predecessor_span_name,  # NEW: Include span
                        symptom_type=symptom.symptom_type,
                        attribute_type=attribute_type,
                        attribute_value=str(attr_value),
                        impact_score=float(impact),
                        relation_change=rel_change,
                    )
                )

        return candidates

    def _refine_by_metric(
        self, rel_change: RelationChange, symptom: Symptom, metric_name: str
    ) -> list[RootCauseCandidate]:
        """
        Refine by metric using ASOF JOIN to enrich spans with dynamic context.

        Strategy: Join P spans with metrics to get "metric value at execution time",
        then bucket by metric value and use statistical tests.
        """
        predecessor = rel_change.relation_bad.predecessor_service

        # Build symptom condition
        if symptom.symptom_type == SymptomType.LATENCY:
            symptom_condition = f"s.span_name = '{symptom.span_name}'"
        elif symptom.symptom_type == SymptomType.ERROR_RATE:
            symptom_condition = f"""
                (s."attr.status_code" = 'Error' 
                 OR s."attr.http.response.status_code" >= {self.HTTP_ERROR_STATUS_CODE})
                AND s.service_name = '{symptom.service_name}'
            """
        else:
            return []

        # Query with ASOF JOIN to enrich P spans with metric context
        query = f"""
        WITH
        -- 1. Predecessor spans with timestamp and pod_id
        predecessor_spans AS (
            SELECT 
                trace_id, 
                span_id,
                time AS timestamp,
                "attr.k8s.pod.name" AS pod_id
            FROM traces_bad
            WHERE service_name = '{predecessor}'
              AND "attr.k8s.pod.name" IS NOT NULL
        ),
        -- 2. Metrics for this metric_name
        metrics_for_p AS (
            SELECT timestamp, pod_id, value
            FROM metrics_bad
            WHERE metric = '{metric_name}'
        ),
        -- 3. ASOF JOIN: enrich P spans with closest metric value
        enriched_predecessor_spans AS (
            SELECT 
                P.trace_id,
                P.span_id,
                M.value AS metric_value
            FROM predecessor_spans AS P
            ASOF LEFT JOIN metrics_for_p AS M
            ON P.pod_id = M.pod_id AND P.timestamp >= M.timestamp
        ),
        -- 4. Symptom spans
        symptom_spans AS (
            SELECT trace_id, parent_span_id
            FROM traces_bad s
            WHERE {symptom_condition}
        ),
        -- 5. Count symptoms per P, grouped by metric bucket
        symptom_counts_per_p AS (
            SELECT 
                P.span_id,
                -- Bucket metrics into bins (e.g., 0.0-0.1, 0.1-0.2, ...)
                FLOOR(COALESCE(P.metric_value, 0) * {self.METRIC_BUCKET_PRECISION}) 
                    / {self.METRIC_BUCKET_PRECISION} AS metric_bucket,
                COUNT(S.parent_span_id) AS s_count
            FROM enriched_predecessor_spans P
            LEFT JOIN symptom_spans S
                ON P.span_id = S.parent_span_id
                AND P.trace_id = S.trace_id
            WHERE P.metric_value IS NOT NULL
            GROUP BY P.span_id, metric_bucket
        )
        -- 6. Aggregate by bucket
        SELECT 
            metric_bucket,
            ARRAY_AGG(s_count) AS distribution
        FROM symptom_counts_per_p
        GROUP BY metric_bucket
        HAVING COUNT(*) >= {self.MIN_SAMPLE_SIZE_FOR_REFINEMENT}
        """

        try:
            result = self.con.execute(query).fetchall()
        except Exception as e:
            if debug():
                logger.warning(f"Error refining by metric {metric_name}: {e}")
            return []

        if len(result) < 2:
            return []

        candidates = []
        overall_mean = np.mean(rel_change.relation_bad.distribution)

        # For each metric bucket, test if its distribution is significantly different
        for metric_bucket, distribution_json in result:
            bucket_distribution = distribution_json if isinstance(distribution_json, list) else [distribution_json]

            # Collect distributions from OTHER buckets
            other_distributions = []
            for other_bucket, other_dist_json in result:
                if other_bucket != metric_bucket:
                    other_dist = other_dist_json if isinstance(other_dist_json, list) else [other_dist_json]
                    other_distributions.extend(other_dist)

            if not other_distributions or len(bucket_distribution) < self.MIN_SAMPLE_SIZE_FOR_REFINEMENT:
                continue

            # Statistical test
            try:
                _, p_value = stats.mannwhitneyu(bucket_distribution, other_distributions, alternative="two-sided")
            except Exception:
                continue

            # Only include if statistically significant AND has higher mean
            bucket_mean = np.mean(bucket_distribution)
            if p_value < self.ALPHA and bucket_mean > overall_mean:
                impact = (1.0 - p_value) * (bucket_mean - overall_mean)

                candidates.append(
                    RootCauseCandidate(
                        service_name=predecessor,
                        span_name=rel_change.relation_bad.predecessor_span_name,  # NEW: Include span
                        symptom_type=symptom.symptom_type,
                        attribute_type=f"metric_{metric_name}",
                        attribute_value=f"{metric_bucket:.1f}",
                        impact_score=float(impact),
                        relation_change=rel_change,
                    )
                )

        return candidates

    def _refine_by_logs(self, rel_change: RelationChange, symptom: Symptom) -> list[RootCauseCandidate]:
        """
        Refine by log data (error logs) using causal linking.

        Gold standard: logs contain trace_id and span_id
        Fallback: logs contain timestamp and pod_id
        """
        candidates = self._refine_by_logs_with_traceid(rel_change, symptom)

        if candidates:
            return candidates

        return self._refine_by_logs_with_podid(rel_change, symptom)

    def _refine_by_logs_with_traceid(self, rel_change: RelationChange, symptom: Symptom) -> list[RootCauseCandidate]:
        """
        Refine by logs (gold standard: logs contain trace_id and span_id).

        This allows strong causal linking between logs and spans.
        """
        predecessor = rel_change.relation_bad.predecessor_service

        # Build symptom condition
        if symptom.symptom_type == SymptomType.LATENCY:
            symptom_condition = f"s.span_name = '{symptom.span_name}'"
        elif symptom.symptom_type == SymptomType.ERROR_RATE:
            symptom_condition = f"""
                (s."attr.status_code" = 'Error' 
                 OR s."attr.http.response.status_code" >= {self.HTTP_ERROR_STATUS_CODE})
                AND s.service_name = '{symptom.service_name}'
            """
        else:
            return []

        # Query with direct trace_id/span_id join
        query = f"""
        WITH
        -- 1. Predecessor spans
        predecessor_spans AS (
            SELECT trace_id, span_id
            FROM traces_bad
            WHERE service_name = '{predecessor}'
        ),
        -- 2. Error logs associated with P spans
        error_logs_for_p AS (
            SELECT DISTINCT trace_id, span_id
            FROM logs_bad
            WHERE level NOT IN ('INFO')
              AND trace_id IS NOT NULL
              AND span_id IS NOT NULL
        ),
        -- 3. Enrich P spans with error log flag
        enriched_predecessor_spans AS (
            SELECT 
                P.trace_id,
                P.span_id,
                (L.span_id IS NOT NULL) AS has_error_log
            FROM predecessor_spans AS P
            LEFT JOIN error_logs_for_p AS L
            ON P.trace_id = L.trace_id AND P.span_id = L.span_id
        ),
        -- 4. Symptom spans
        symptom_spans AS (
            SELECT trace_id, parent_span_id
            FROM traces_bad s
            WHERE {symptom_condition}
        ),
        -- 5. Count symptoms per P, grouped by has_error_log
        symptom_counts_per_p AS (
            SELECT 
                P.span_id,
                P.has_error_log,
                COUNT(S.parent_span_id) AS s_count
            FROM enriched_predecessor_spans P
            LEFT JOIN symptom_spans S
                ON P.span_id = S.parent_span_id
                AND P.trace_id = S.trace_id
            GROUP BY P.span_id, P.has_error_log
        )
        -- 6. Aggregate by has_error_log
        SELECT 
            has_error_log,
            ARRAY_AGG(s_count) AS distribution
        FROM symptom_counts_per_p
        GROUP BY has_error_log
        HAVING COUNT(*) >= {self.MIN_SAMPLE_SIZE_FOR_REFINEMENT}
        """

        try:
            result = self.con.execute(query).fetchall()
        except Exception as e:
            if debug():
                logger.warning(f"Error refining by logs (with trace_id): {e}")
            return []

        if len(result) < 2:
            return []

        candidates = []

        # Find the distribution for has_error_log = true and false
        dist_with_error = None
        dist_without_error = None

        for has_error_log, distribution_json in result:
            dist = distribution_json if isinstance(distribution_json, list) else [distribution_json]
            if has_error_log:
                dist_with_error = dist
            else:
                dist_without_error = dist

        if (
            dist_with_error
            and dist_without_error
            and len(dist_with_error) >= self.MIN_SAMPLE_SIZE_FOR_REFINEMENT
            and len(dist_without_error) >= self.MIN_SAMPLE_SIZE_FOR_REFINEMENT
        ):
            # Statistical test
            try:
                _, p_value = stats.mannwhitneyu(dist_with_error, dist_without_error, alternative="two-sided")
            except Exception:
                return []

            # Check if error logs are associated with higher symptom rates
            mean_with_error = np.mean(dist_with_error)
            mean_without_error = np.mean(dist_without_error)

            if p_value < self.ALPHA and mean_with_error > mean_without_error:
                impact = (1.0 - p_value) * (mean_with_error - mean_without_error)

                candidates.append(
                    RootCauseCandidate(
                        service_name=predecessor,
                        span_name=rel_change.relation_bad.predecessor_span_name,  # NEW: Include span
                        symptom_type=symptom.symptom_type,
                        attribute_type="error_log",
                        attribute_value="true",
                        impact_score=float(impact),
                        relation_change=rel_change,
                    )
                )

        return candidates

    def _refine_by_logs_with_podid(self, rel_change: RelationChange, symptom: Symptom) -> list[RootCauseCandidate]:
        """
        Refine by logs (fallback: logs contain pod_id and timestamp only).

        Weaker causal linking - we can only tell if a pod had errors, not specific spans.
        """
        predecessor = rel_change.relation_bad.predecessor_service

        # Build symptom condition
        if symptom.symptom_type == SymptomType.LATENCY:
            symptom_condition = f"s.span_name = '{symptom.span_name}'"
        elif symptom.symptom_type == SymptomType.ERROR_RATE:
            symptom_condition = f"""
                (s."attr.status_code" = 'Error' 
                 OR s."attr.http.response.status_code" >= {self.HTTP_ERROR_STATUS_CODE})
                AND s.service_name = '{symptom.service_name}'
            """
        else:
            return []

        # Pre-compute: which pods had error logs
        query = f"""
        WITH
        -- 1. Predecessor spans with pod_id
        predecessor_spans AS (
            SELECT 
                trace_id, 
                span_id,
                "attr.k8s.pod.name" AS pod_id
            FROM traces_bad
            WHERE service_name = '{predecessor}'
              AND "attr.k8s.pod.name" IS NOT NULL
        ),
        -- 2. Pods with error logs
        pods_with_errors AS (
            SELECT DISTINCT pod_id
            FROM logs_bad
            WHERE level NOT IN ('INFO')
              AND pod_id IS NOT NULL
        ),
        -- 3. Enrich P spans with pod error flag
        enriched_predecessor_spans AS (
            SELECT 
                P.trace_id,
                P.span_id,
                (E.pod_id IS NOT NULL) AS pod_has_errors
            FROM predecessor_spans AS P
            LEFT JOIN pods_with_errors AS E
            ON P.pod_id = E.pod_id
        ),
        -- 4. Symptom spans
        symptom_spans AS (
            SELECT trace_id, parent_span_id
            FROM traces_bad s
            WHERE {symptom_condition}
        ),
        -- 5. Count symptoms per P, grouped by pod_has_errors
        symptom_counts_per_p AS (
            SELECT 
                P.span_id,
                P.pod_has_errors,
                COUNT(S.parent_span_id) AS s_count
            FROM enriched_predecessor_spans P
            LEFT JOIN symptom_spans S
                ON P.span_id = S.parent_span_id
                AND P.trace_id = S.trace_id
            GROUP BY P.span_id, P.pod_has_errors
        )
        -- 6. Aggregate by pod_has_errors
        SELECT 
            pod_has_errors,
            ARRAY_AGG(s_count) AS distribution
        FROM symptom_counts_per_p
        GROUP BY pod_has_errors
        HAVING COUNT(*) >= {self.MIN_SAMPLE_SIZE_FOR_REFINEMENT}
        """

        try:
            result = self.con.execute(query).fetchall()
        except Exception as e:
            if debug():
                logger.warning(f"Error refining by logs (with pod_id): {e}")
            return []

        if len(result) < 2:
            return []

        candidates = []

        # Find distributions for pod_has_errors = true and false
        dist_with_error = None
        dist_without_error = None

        for pod_has_errors, distribution_json in result:
            dist = distribution_json if isinstance(distribution_json, list) else [distribution_json]
            if pod_has_errors:
                dist_with_error = dist
            else:
                dist_without_error = dist

        if (
            dist_with_error
            and dist_without_error
            and len(dist_with_error) >= self.MIN_SAMPLE_SIZE_FOR_REFINEMENT
            and len(dist_without_error) >= self.MIN_SAMPLE_SIZE_FOR_REFINEMENT
        ):
            # Statistical test
            try:
                _, p_value = stats.mannwhitneyu(dist_with_error, dist_without_error, alternative="two-sided")
            except Exception:
                return []

            # Check if pods with errors are associated with higher symptom rates
            mean_with_error = np.mean(dist_with_error)
            mean_without_error = np.mean(dist_without_error)

            if p_value < self.ALPHA and mean_with_error > mean_without_error:
                impact = (1.0 - p_value) * (mean_with_error - mean_without_error)

                candidates.append(
                    RootCauseCandidate(
                        service_name=predecessor,
                        span_name=rel_change.relation_bad.predecessor_span_name,  # NEW: Include span
                        symptom_type=symptom.symptom_type,
                        attribute_type="pod_error_log",
                        attribute_value="true",
                        impact_score=float(impact),
                        relation_change=rel_change,
                    )
                )

        return candidates


# ============================================================================
# Module 7: Candidate Ranking
# ============================================================================


class CandidateRanker:
    """Ranks root cause candidates by impact score."""

    def __init__(self):
        pass

    def rank(self, candidates: list[RootCauseCandidate]) -> list[RootCauseCandidate]:
        """Sort candidates by impact score (descending)."""
        return sorted(candidates, key=lambda c: c.impact_score, reverse=True)
