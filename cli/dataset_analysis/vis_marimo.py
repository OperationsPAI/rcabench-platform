import marimo

__generated_with = "0.14.16"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _():
    import os
    import sys
    import traceback
    from datetime import datetime
    from pathlib import Path
    from typing import Any

    import marimo as mo

    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

    import altair as alt
    from rcabench.openapi import (
        ApiClient,
        DatasetsApi,
        DtoDatapackDetectorReq,
        DtoDetectorRecord,
        DtoInjectionV2Response,
        DtoInjectionV2SearchReq,
        EvaluationApi,
        InjectionsApi,
        ProjectsApi,
    )

    from cli.dataset_analysis.metric_cli import Analyzer, Distribution
    from rcabench_platform.v2.cli.main import app, logger
    from rcabench_platform.v2.clients.rcabench_ import RCABenchClient
    from rcabench_platform.v2.datasets.train_ticket import extract_path

    return (
        Analyzer,
        Any,
        ApiClient,
        DatasetsApi,
        DtoInjectionV2Response,
        DtoInjectionV2SearchReq,
        InjectionsApi,
        Path,
        ProjectsApi,
        RCABenchClient,
        alt,
        datetime,
        logger,
        mo,
        traceback,
    )


@app.cell(hide_code=True)
def _():
    DEFAULT_NAMESPACE = "ts"

    ALGORITHMS = ["baro", "simplerca", "microdig", "traceback"]
    DEGREES = ["absolute_anomaly", "may_anomaly", "no_anomaly"]
    METRICS = ["SDD@1", "CPL", "RootServiceDegree"]

    FAULT_CLASSES: dict[str, list[str]] = {
        "dnschaos": ["DNSError", "DNSRandom"],
        "httpchaos": [
            "HTTPRequestAbort",
            "HTTPResponseAbort",
            "HTTPRequestDelay",
            "HTTPResponseDelay",
            "HTTPResponseReplaceBody",
            "HTTPResponsePatchBody",
            "HTTPRequestReplacePath",
            "HTTPRequestReplaceMethod",
            "HTTPResponseReplaceCode",
        ],
        "jvmchaos": [
            "JVMLatency",
            "JVMReturn",
            "JVMException",
            "JVMGarbageCollector",
            "JVMCPUStress",
            "JVMMemoryStress",
            "JVMMySQLLatency",
            "JVMMySQLException",
        ],
        "networkchaos": [
            "NetworkDelay",
            "NetworkLoss",
            "NetworkDuplicate",
            "NetworkCorrupt",
            "NetworkBandwidth",
            "NetworkPartition",
        ],
        "podchaos": [
            "PodKill",
            "PodFailure",
            "ContainerKill",
        ],
        "stresschaos": [
            "MemoryStress",
            "CPUStress",
        ],
        "timechaos": ["TimeSkew"],
    }
    return ALGORITHMS, DEFAULT_NAMESPACE, DEGREES, FAULT_CLASSES, METRICS


@app.cell(hide_code=True)
def _(FAULT_CLASSES: dict[str, list[str]], pl):
    fault_classes_df = pl.DataFrame(
        [
            {"chaos_type": chaos_type, "fault_name": fault_name}
            for chaos_type, fault_list in FAULT_CLASSES.items()
            for fault_name in fault_list
        ]
    )
    return (fault_classes_df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Prepare Injections Data""")
    return


@app.cell(hide_code=True)
def _(
    ALGORITHMS,
    Analyzer,
    Any,
    ApiClient,
    DEFAULT_NAMESPACE,
    DEGREES,
    DatasetsApi,
    DtoInjectionV2Response,
    DtoInjectionV2SearchReq,
    InjectionsApi,
    METRICS,
    Path,
    ProjectsApi,
    datetime,
    logger,
    traceback,
):
    def get_timestamp() -> str:
        """Generate timestamp in YYYY-MM-DD_HH-MM-SS format"""
        time_format = "%Y-%m-%d_%H-%M-%S"
        return datetime.now().strftime(time_format)

    def prepare_injections_data(
        client: ApiClient,
        dataset_id: int | None = None,
        project_id: int | None = None,
    ) -> tuple[dict[str, list[DtoInjectionV2Response]], Path]:
        def _get_injections() -> tuple[dict[str, list[DtoInjectionV2Response]], Path]:
            folder_name = "injections"
            api = InjectionsApi(client)

            injections_dict: dict[str, list[DtoInjectionV2Response]] = {}
            for degree in DEGREES:
                resp = api.api_v2_injections_search_post(
                    search=DtoInjectionV2SearchReq(
                        tags=[degree],
                        include_labels=True,
                    )
                )
                if not resp or not resp.data or not resp.data.items:
                    raise ValueError(f"No injections found for degree {degree}")

                injections_dict[degree] = resp.data.items

            return injections_dict, Path(folder_name) / get_timestamp()

        def _get_injections_by_id() -> tuple[list[DtoInjectionV2Response], Path]:
            if dataset_id is not None:
                folder_name = f"dataset_{dataset_id}"
                api = DatasetsApi(client)
                resp = api.api_v2_datasets_id_get(id=dataset_id, include_injections=True)

                if not resp or not resp.data or not resp.data.injections:
                    raise ValueError(f"No injections found for dataset {dataset_id}")

                return resp.data.injections, Path(folder_name) / get_timestamp()

            elif project_id is not None:
                folder_name = f"project_{project_id}"
                api = ProjectsApi(client)
                resp = api.api_v2_projects_id_get(id=project_id, include_injections=True)

                if not resp or not resp.data or not resp.data.injections:
                    raise ValueError(f"No injections found for project {project_id}")

                return resp.data.injections, Path(folder_name) / get_timestamp()

            else:
                raise ValueError("Either dataset_id or project_id must be provided")

        def _filter_injections(
            injections: list[DtoInjectionV2Response],
        ) -> dict[str, list[DtoInjectionV2Response]]:
            items_dict: dict[str, list[DtoInjectionV2Response]] = dict([(degree, []) for degree in DEGREES])
            for injection in injections:
                if injection.labels is not None:
                    for label in injection.labels:
                        if label.value is not None and label.value in items_dict:
                            items_dict[label.value].append(injection)

            return items_dict

        if dataset_id is not None or project_id is not None:
            injections, folder_path = _get_injections_by_id()
            injections_dict = _filter_injections(injections)
            return injections_dict, folder_path
        else:
            return _get_injections()

    def get_distributions_dict(
        client: ApiClient,
        injections_dict: dict[str, DtoInjectionV2Response],
    ) -> dict[str, dict[str, Any]] | None:
        distributions_dict: dict[str, dict[str, Any]] = {}
        for degree, injections in injections_dict.items():
            try:
                analyzer = Analyzer(
                    client=client,
                    namespace=DEFAULT_NAMESPACE,
                    algorithms=ALGORITHMS,
                    metrics=METRICS,
                    injections=injections,
                )

                distributions_dict[degree] = analyzer.get_distribution().to_dict()
            except Exception as e:
                traceback.print_exc()
                logger.error(f"Error processing distribution for {degree}: {e}")
                return None

        return distributions_dict

    return get_distributions_dict, prepare_injections_data


@app.cell(hide_code=True)
def _(
    Any,
    DtoInjectionV2Response,
    RCABenchClient,
    get_distributions_dict,
    prepare_injections_data,
):
    injections_dict: dict[str, list[DtoInjectionV2Response]] = {}
    distributions_dict: dict[str, dict[str, Any]] = {}

    with RCABenchClient() as client:
        injections_dict, folder_path = prepare_injections_data(client=client)
        assert injections_dict is not None
        distributions_dict = get_distributions_dict(client=client, injections_dict=injections_dict)
    return distributions_dict, injections_dict


@app.cell(hide_code=True)
def _(injections_dict: "dict[str, list[DtoInjectionV2Response]]", mo):
    absolutely_stat = mo.stat(
        value=str(len(injections_dict["absolute_anomaly"])),
        label="Absolutely Anomaly",
    )

    may_stat = mo.stat(
        value=str(len(injections_dict["may_anomaly"])),
        label="May Anomaly",
    )

    no_stat = mo.stat(
        value=str(len(injections_dict["no_anomaly"])),
        label="No Anomaly",
    )

    mo.hstack([absolutely_stat, may_stat, no_stat], justify="center", gap="5rem")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Visuliaze Injections Data""")
    return


@app.cell(hide_code=True)
def _():
    import polars as pl

    from cli.dataset_analysis.vis.models import (
        BarMeta,
        BubbleMeta,
        HeatmapMeta,
        VisInjectionsConfig,
        NewVisInjectionsConfig,
    )

    return BarMeta, NewVisInjectionsConfig, VisInjectionsConfig, pl


@app.cell(hide_code=True)
def _(BarMeta, alt, pl):
    def plot_bar(meta: BarMeta, extra_legend: bool = False) -> alt.Chart:
        df = pl.DataFrame(meta.data)
        base = alt.Chart(data=df.to_pandas())

        degree_selection = alt.selection_point(fields=["degree"], bind="legend")
        degree_color = alt.Color(
            "degree",
            title="Degree",
            scale=alt.Scale(
                domain=["absolute_anomaly", "may_anomaly", "no_anomaly"],
                range=["#e74c3c", "#f39c12", "#2ecc71"],
            ),
        )

        degree_opacity = alt.when(degree_selection).then(alt.value(0.7)).otherwise(alt.value(0.1))

        tooltip_fields = ["degree", "name", "count"]
        x_axis_config = alt.Axis(labelAngle=-45)

        if "chaos_type" in df.columns:
            tooltip_fields.append("chaos_type:N")

        bar = (
            base.mark_bar(stroke="navy", strokeWidth=1, opacity=0.7)
            .encode(
                x=alt.X(
                    "name",
                    title=meta.x_label,
                    axis=alt.Axis(labelAngle=-45),
                    sort="-y",
                ),
                y=alt.Y("count", title="Injection Count"),
                color=degree_color,
                opacity=degree_opacity,
                xOffset="degree",
                tooltip=tooltip_fields,
            )
            .properties(width=850, height=300, title=meta.title)
            .add_params(degree_selection)
        )

        return bar

    return (plot_bar,)


@app.cell
def _(
    Any,
    BarMeta,
    NewVisInjectionsConfig,
    VisInjectionsConfig,
    alt,
    distributions_dict: "dict[str, dict[str, Any]]",
    plot_bar,
):
    class VisInjections:
        def __init__(
            self,
            config: VisInjectionsConfig,
            distributions_dict: dict[str, dict[str, Any]],
        ):
            self.config = config
            self.distributions_dict = distributions_dict

        def get_bar_data(self, key: str) -> list[dict[str, Any]]:
            combined_data = []

            for degree, distributions in self.distributions_dict.items():
                data: dict[str, int] = distributions.get(key, {})
                if not data:
                    continue

                for name, count in data.items():
                    combined_data.append(
                        {
                            "degree": degree,
                            "name": name,
                            "count": count,
                        }
                    )

            return combined_data

        def get_heatmap_data(self, key: str) -> list[dict[str, Any]]:
            combined_data = []

            for degree, distributions in self.distributions_dict.items():
                data: dict[str, int] = distributions.get(key, {})
                if not data:
                    continue

                for name, count in data.items():
                    combined_data.append(
                        {
                            "degree": degree,
                            "name": name,
                            "count": count,
                        }
                    )

            return combined_data

        def display_bar(self, bar_data: list[dict[str, Any]], key: str) -> alt.Chart:
            """
            Display bar charts for fault, service distributions.
            """
            bar_config = self.config.bar_configs[key]

            return plot_bar(
                BarMeta(
                    data=bar_data,
                    x_label=bar_config.x_label,
                    y_label="Injection Count",
                    title=bar_config.title,
                ),
                extra_legend=key == "faults",
            )

    config = NewVisInjectionsConfig()
    processor = VisInjections(config=NewVisInjectionsConfig(), distributions_dict=distributions_dict)
    return (processor,)


@app.cell(hide_code=True)
def _(Any, alt, pl, processor):
    def display_bar(
        key: str,
        ori_bar_data: list[dict[str, Any]],
        column_name: str,
        options: list[str],
        exclude: bool = False,
    ) -> alt.Chart:
        """
        筛选数据

        Args:
            ori_bar_data: 原始数据
            column_name: 要筛选的列名
            options: 筛选选项
            exclude: 如果为 True，则排除 options 中的值；如果为 False，则只保留 options 中的值

        Returns:
            筛选后的数据
        """
        df = pl.DataFrame(data=ori_bar_data)

        if exclude:
            filtered_df = df.filter(~pl.col(column_name).is_in(options))
        else:
            filtered_df = df.filter(pl.col(column_name).is_in(options))

        filtered_bar_data = filtered_df.to_dicts()

        return processor.display_bar(bar_data=filtered_bar_data, key=key)

    return (display_bar,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Fault  Distribution""")
    return


@app.cell(hide_code=True)
def _(fault_classes_df, mo, pl, processor):
    faults_df = pl.DataFrame(data=processor.get_bar_data("faults"))
    faults_merged_df = faults_df.join(fault_classes_df, left_on="name", right_on="fault_name", how="left")

    chaoses_options: list[str] = faults_merged_df.get_column("chaos_type").unique().to_list()
    chaoses_multiselect = mo.ui.multiselect(options=chaoses_options, value=chaoses_options)

    def get_faults_options(chaoses: list[str]) -> list[str]:
        filtered_df = fault_classes_df.filter(pl.col("chaos_type").is_in(chaoses))
        return filtered_df["fault_name"].to_list()

    return chaoses_multiselect, faults_merged_df, get_faults_options


@app.cell(hide_code=True)
def _(chaoses_multiselect, get_faults_options, mo):
    faults_multiselect = mo.ui.multiselect(
        options=get_faults_options(chaoses_multiselect.value),
        value=get_faults_options(chaoses_multiselect.value),
    )
    return (faults_multiselect,)


@app.cell(hide_code=True)
def _(chaoses_multiselect, faults_multiselect, mo):
    chaos_selector_card = mo.vstack(
        [mo.md("### 🔥 混沌类型"), chaoses_multiselect, mo.md(f"*已选择 {len(chaoses_multiselect.value)} 种类型*")],
        align="center",
        gap="0.5rem",
    )

    fault_selector_card = mo.vstack(
        [mo.md("### ⚡ 故障类型"), faults_multiselect, mo.md(f"*已选择 {len(faults_multiselect.value)} 种故障*")],
        align="center",
        gap="0.5rem",
    )

    faults_horizontal = mo.hstack([chaos_selector_card, fault_selector_card], justify="center", gap="2rem", wrap=True)
    return (faults_horizontal,)


@app.cell(hide_code=True)
def _(
    display_bar,
    faults_horizontal,
    faults_merged_df,
    faults_multiselect,
    mo,
):
    mo.vstack(
        [
            faults_horizontal,
            mo.ui.altair_chart(
                display_bar(
                    key="faults",
                    ori_bar_data=faults_merged_df.to_dict(),
                    column_name="name",
                    options=faults_multiselect.value,
                )
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Service Distribuiton""")
    return


@app.cell(hide_code=True)
def _(mo, pl, processor):
    services_bar_data = processor.get_bar_data("services")

    services_options: list[str] = pl.DataFrame(data=services_bar_data).get_column("name").unique().to_list()

    services_multiselect = mo.ui.multiselect(options=services_options, value=services_options)
    return services_bar_data, services_multiselect


@app.cell
def _(display_bar, mo, services_bar_data, services_multiselect):
    service_selector_card = mo.vstack(
        [mo.md("### ⚡ 服务类型"), services_multiselect, mo.md(f"*已选择 {len(services_multiselect.value)} 种服务*")],
        align="center",
        gap="0.5rem",
    )

    mo.vstack(
        [
            service_selector_card,
            mo.ui.altair_chart(
                display_bar(
                    key="services",
                    ori_bar_data=services_bar_data,
                    column_name="name",
                    options=services_multiselect.value,
                )
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Fault Services Distribution""")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
