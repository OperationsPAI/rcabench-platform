import marimo

__generated_with = "0.14.16"
app = marimo.App(width="medium")


@app.cell
def _():
    import json
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

    from cli.dataset_analysis.dataset_analysis import Analyzer, Distribution
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


@app.cell
def _():
    DEFAULT_NAMESPACE = "ts"
    ALGORITHMS = ["baro", "simplerca", "microdig", "traceback"]
    DEGREES = ["absolute_anomaly", "may_anomaly", "no_anomaly"]
    METRICS = ["SDD@1", "CPL", "RootServiceDegree"]
    return ALGORITHMS, DEFAULT_NAMESPACE, DEGREES, METRICS


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
        def _get_injections() -> tuple[
            dict[str, list[DtoInjectionV2Response]], Path
        ]:
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
                resp = api.api_v2_datasets_id_get(
                    id=dataset_id, include_injections=True
                )

                if not resp or not resp.data or not resp.data.injections:
                    raise ValueError(
                        f"No injections found for dataset {dataset_id}"
                    )

                return resp.data.injections, Path(folder_name) / get_timestamp()

            elif project_id is not None:
                folder_name = f"project_{project_id}"
                api = ProjectsApi(client)
                resp = api.api_v2_projects_id_get(
                    id=project_id, include_injections=True
                )

                if not resp or not resp.data or not resp.data.injections:
                    raise ValueError(
                        f"No injections found for project {project_id}"
                    )

                return resp.data.injections, Path(folder_name) / get_timestamp()

            else:
                raise ValueError(
                    "Either dataset_id or project_id must be provided"
                )

        def _filter_injections(
            injections: list[DtoInjectionV2Response],
        ) -> dict[str, list[DtoInjectionV2Response]]:
            items_dict: dict[str, list[DtoInjectionV2Response]] = dict(
                [(degree, []) for degree in DEGREES]
            )
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
    return (distributions_dict,)


@app.cell
def _():
    from cli.dataset_analysis.vis.models import BarMeta, BubbleMeta, HeatmapMeta, NewVisInjectionsConfig
    return BarMeta, BubbleMeta, HeatmapMeta, NewVisInjectionsConfig


@app.cell
def _(BarMeta, BubbleMeta, HeatmapMeta, alt):
    import matplotlib.pyplot as plt
    import plotly.express as px
    import plotly.graph_objects as go
    import polars as pl

    def plot_bar(meta: BarMeta) -> alt.HConcatChart | alt.ConcatChart:
        assert meta.data, "Data for bar chart cannot be empty"

        df = pl.DataFrame(meta.data)
        base = alt.Chart(data=df.to_pandas())

        degree_selection = alt.selection_point(fields=["degree"], bind="legend")
        degree_color = alt.Color(
            "degree:N",
            title="Degree",
            scale=alt.Scale(
                domain=["absolute_anomaly", "may_anomaly", "no_anomaly"],
                range=["#e74c3c", "#f39c12", "#2ecc71"],
            ),
        )
        degree_opacity = (
            alt.when(degree_selection)
            .then(alt.value(0.9))
            .otherwise(alt.value(0.1))
        )

        bar = (
            base.mark_bar(stroke="navy", strokeWidth=1, opacity=0.7)
            .encode(
                x=alt.X(
                    "name:N",
                    title=meta.x_label,
                    axis=alt.Axis(labelAngle=-45),
                    sort="-y",
                ),
                y=alt.Y("count:Q", title="Injection Count"),
                color=degree_color,
                opacity=degree_opacity,
                xOffset="degree:N",
                tooltip=["degree:N", "name:N", "count:Q"],
            )
            .properties(width=800, height=400, title=meta.title)
        )

        return bar


    def plot_heatmap(meta: HeatmapMeta) -> None:
        plt.figure(figsize=(max(12, len(meta.x)), max(8, len(meta.y) * 0.5)))

        im = plt.imshow(meta.matrix, cmap="YlOrRd", aspect="auto")

        plt.xlabel(meta.x_label)
        plt.ylabel(meta.y_label)
        plt.title(meta.title)

        plt.xticks(range(len(meta.x)), [str(f) for f in meta.x], rotation=45)
        plt.yticks(range(len(meta.y)), meta.y)

        cbar = plt.colorbar(im)
        cbar.set_label("Injection Count")

        for i in range(len(meta.y)):
            for j in range(len(meta.x)):
                value = meta.matrix[i, j]
                if value == int(value):
                    display_text = f"{int(value)}"
                else:
                    display_text = f"{value:.4f}"

                if meta.matrix[i, j] > 0:
                    plt.text(
                        j,
                        i,
                        display_text,
                        ha="center",
                        va="center",
                        color="white"
                        if meta.matrix[i, j] > meta.matrix.max() / 2
                        else "black",
                    )

        plt.tight_layout()

        plt.savefig(meta.save_path, dpi=300, bbox_inches="tight")


    def plot_3d_bubble(meta: BubbleMeta) -> None:
        colors = px.colors.qualitative.Set3
        if len(meta.layer_datas) > len(colors):
            colors = colors * ((len(meta.layer_datas) // len(colors)) + 1)

        value_key_to_color = {
            key: colors[i % len(colors)]
            for i, key in enumerate(meta.layer_datas.keys())
        }

        fig = go.Figure()

        for value_key, layer_data in meta.layer_datas.items():
            hover_texts = []
            for item in layer_data.key_datas:
                hover_text = (
                    f"<b>Fault:</b> {item.fault}<br>"
                    f"<b>{meta.mapping_key_name}:</b> {item.mapping_key}<br>"
                    f"<b>{meta.metric} Value:</b> {item.value_key}<br>"
                    f"<b>Count:</b> {item.count}<br>"
                    f"<b>Numeric Value:</b> {item.metric_value:.4f}"
                )
                hover_texts.append(hover_text)

            fig.add_trace(
                go.Scatter3d(
                    x=layer_data.x_list,
                    y=layer_data.y_list,
                    z=layer_data.z_list,
                    mode="markers",
                    name=f"Value: {value_key}",
                    marker=dict(
                        size=layer_data.sizes,
                        color=value_key_to_color[value_key],
                        opacity=0.8,
                        sizemode="diameter",
                        sizemin=5,
                        line=dict(width=1, color="darkslategrey"),
                    ),
                    text=hover_texts,
                    hovertemplate="%{text}<extra></extra>",
                    showlegend=True,
                )
            )

        fig.update_layout(
            title=dict(
                text=f"3D Distribution: {meta.metric} by Fault Type and Service",
                x=0.5,
                font=dict(size=16, family="Arial Black"),
            ),
            scene=dict(
                xaxis=dict(
                    title="Fault Types",
                    tickvals=list(range(len(meta.x_texts))),
                    ticktext=meta.x_texts,
                    tickfont=dict(size=10),
                ),
                yaxis=dict(
                    title=f"{meta.mapping_key_name}",
                    tickvals=list(range(len(meta.y_texts))),
                    ticktext=meta.y_texts,
                    tickfont=dict(size=10),
                ),
                zaxis=dict(title=f"{meta.metric} Count", tickfont=dict(size=10)),
                camera=dict(eye=dict(x=1.5, y=1.5, z=1.5)),
            ),
            width=1200,
            height=800,
            font=dict(family="Arial", size=12),
            legend=dict(
                x=0.02,
                y=0.98,
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor="rgba(0,0,0,0.2)",
                borderwidth=1,
                itemsizing="constant",
            ),
        )

        fig.write_image(
            meta.save_path, width=1200, height=800, scale=2, engine="kaleido"
        )
    return pl, plot_bar


@app.cell(hide_code=True)
def _(Any, BarMeta, NewVisInjectionsConfig, alt, plot_bar):
    class VisInjections:
        config = NewVisInjectionsConfig()

        def __init__(
            self, distributions_dict: dict[str, dict[str, Any]], metrics: list[str]
        ):
            self.distributions_dict = distributions_dict
            self.metrics = metrics

        def display_bars(
            self,
        ) -> dict[
            str,
            alt.HConcatChart
            | alt.ConcatChart
            | dict[str, alt.HConcatChart | alt.ConcatChart],
        ]:
            """
            Display bar charts for fault, service distributions.
            """

            def _display_bar(
                key: str,
                x_label: str,
                title: str,
                extra_key: str | None = None,
            ) -> alt.HConcatChart | alt.ConcatChart:
                combined_data = []

                for degree, distributions in self.distributions_dict.items():
                    data: dict[str, int | dict[str, int]] = distributions.get(
                        key, {}
                    )
                    if not data:
                        continue

                    for name, count in data.items():
                        if not isinstance(count, dict):
                            combined_data.append(
                                {
                                    "degree": degree,
                                    "name": name,
                                    "count": count,
                                }
                            )
                            continue

                        for sub_name, sub_count in count.items():
                            data_item: dict[str, Any] = {"degree": degree}
                            data_item[sub_name] = sub_count
                            combined_data.append(data_item)

                return plot_bar(
                    BarMeta(
                        data=combined_data,
                        x_label=x_label,
                        y_label="Injection Count",
                        title=title,
                    )
                )

            bars: dict[
                str,
                alt.HConcatChart
                | alt.ConcatChart
                | dict[str, alt.HConcatChart | alt.ConcatChart],
            ] = {}

            for key, config in self.config.bar_configs.items():
                bars[key] = _display_bar(key, config.x_label, config.title)

            return bars
    return (VisInjections,)


@app.cell
def _(
    METRICS,
    VisInjections,
    distributions_dict: "dict[str, dict[str, Any]]",
    logger,
    mo,
):
    processor = VisInjections(distributions_dict=distributions_dict, metrics=METRICS)

    bars = processor.display_bars()
    if not bars:
        logger.warning("No valid bars found for visualization")

    mo.ui.altair_chart(bars["faults"])
    return (bars,)


@app.cell
def _(bars, mo):
    mo.md(f"""{mo.ui.altair_chart(bars["services"])}""")
    return


@app.cell
def _(bars, mo):
    mo.md(f"""{mo.ui.altair_chart(bars["metrics"]["SDD@1"])}""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Analyze SDD""")
    return


@app.cell
def _(Any, DEGREES, distributions_dict: "dict[str, dict[str, Any]]", pl):
    def analyze_sdd():
        output_content: dict[str, dict[str, Any]] = {}

        for degree in DEGREES:
            sdd_datas: list[dict[str, Any]] = []

            data = distributions_dict[degree].fault_service_metrics

            for fault, mapping in data.items():
                for mapping_key, metric_data in mapping.items():
                    for metric, metric_stats in metric_data.items():
                        if "SDD" in metric:
                            for metric_value, count in metric_stats.items():
                                sdd_datas.append(
                                    {
                                        "fault": fault,
                                        "mapping_key": mapping_key,
                                        "metric_value": float(metric_value),
                                        "count": count,
                                    }
                                )

            df = pl.DataFrame(data=sdd_datas)
            if df.is_empty():
                continue

            df_le_filtered = df.filter(pl.col("metric_value") != 0)
            df_le_sorted = df_le_filtered.sort("count", descending=True)

            output_content[f"{degree}_le"] = df_le_sorted.to_dicts()

            df_eq_filtered = df.filter(pl.col("metric_value") == 0)
            df_eq_sorted = df_eq_filtered.sort("count", descending=True)

            output_content[f"{degree}_eq"] = df_eq_sorted.to_dicts()

        return output_content

    return (analyze_sdd,)


@app.cell
def _(analyze_sdd, pl):
    content = analyze_sdd()
    df_ab_le = pl.DataFrame(data=content["absolute_anomaly_le"])
    df_ab_eq = pl.DataFrame(data=content["absolute_anomaly_eq"])
    return content, df_ab_eq, df_ab_le


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### 检测是否有重叠""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### SDD 数据分析""")
    return


@app.cell(hide_code=True)
def _(Any, df_ab_eq, df_ab_le, mo, pl):
    def check_overlap(df1: pl.DataFrame, df2: pl.DataFrame, name1: str, name2: str) -> dict[str, Any]:
        df1_combinations = df1.select([pl.col("fault"), pl.col("mapping_key")]).unique()

        df2_combinations = df2.select([pl.col("fault"), pl.col("mapping_key")]).unique()

        overlap = df1_combinations.join(df2_combinations, on=["fault", "mapping_key"], how="inner")

        df1_only = df1_combinations.join(df2_combinations, on=["fault", "mapping_key"], how="anti")

        df2_only = df2_combinations.join(df1_combinations, on=["fault", "mapping_key"], how="anti")

        return {
            "overlap": overlap,
            f"{name1}_only": df1_only,
            f"{name2}_only": df2_only,
            "overlap_count": len(overlap),
            f"{name1}_total": df1_combinations.shape[0],
            f"{name2}_total": df2_combinations.shape[0],
            f"{name1}_only_count": df1_only.shape[0],
            f"{name2}_only_count": df2_only.shape[0],
        }

    name1 = "ab_le"
    name2 = "ab_eq"
    overlap_result = check_overlap(df_ab_le, df_ab_eq, name1, name2)

    mo.md(f"""
        #### DataFrame 重叠检查结果

        **总体情况:**

        - df_{name1} (SDD≠0) 的 fault-mapping_key 组合数: {overlap_result[f"{name1}_total"]}
        - df_{name2} (SDD=0) 的 fault-mapping_key 组合数: {overlap_result[f"{name2}_total"]}
        - 重叠的组合数: {overlap_result["overlap_count"]}
        - 仅在 df_{name1} 中的组合数: {overlap_result[f"{name1}_only_count"]}
        - 仅在 df_{name2} 中的组合数: {overlap_result[f"{name2}_only_count"]}

        **重叠比例:**

        - df_{name1} 中重叠的比例: {overlap_result["overlap_count"] / overlap_result[f"{name1}_total"] * 100:.1f}%
        - df_{name2} 中重叠的比例: {overlap_result["overlap_count"] / overlap_result[f"{name2}_total"] * 100:.1f}%

        **分析结论:**

        {"✅ 有重叠 - 相同的故障-服务组合在SDD=0和SDD≠0中都出现" if overlap_result["overlap_count"] > 0 else "❌ 无重叠 - 所有故障-服务组合要么只有SDD=0，要么只有SDD≠0"}
        """)
    return name1, name2, overlap_result


@app.cell(hide_code=True)
def _(mo, overlap_result, pl):
    overlap_table_by_fault = mo.ui.table(
        overlap_result["overlap"]
        .group_by("fault")
        .agg(
            [
                pl.col("mapping_key").alias("mapping_keys")  # 自动聚合为数组
            ]
        )
        .sort("fault")
        .to_pandas()
    )

    mo.md(f"""
            #### 同时在 SDD=0 和 SDD≠0 中出现的组合

            {overlap_table_by_fault}
            """)
    return


@app.cell(hide_code=True)
def _(mo, name1, overlap_result, pl):
    mo.md(
        f"""
    #### 仅在 df_{name1} (SDD≠0) 中的组合

            {
            mo.ui.table(
                overlap_result[f"{name1}_only"]
                .group_by("fault")
                .agg(
                    [
                        pl.col("mapping_key").alias("mapping_keys")  # 自动聚合为数组
                    ]
                )
                .sort("fault")
            )
        }
    """
    )
    return


@app.cell(hide_code=True)
def _(mo, name2, overlap_result, pl):
    mo.md(
        f"""
    #### 仅在 df_{name2} (SDD=0) 中的组合

            {
            mo.ui.table(
                overlap_result[f"{name2}_only"]
                .group_by("fault")
                .agg(
                    [
                        pl.col("mapping_key").alias("mapping_keys")  # 自动聚合为数组
                    ]
                )
                .sort("fault")
            )
        }
    """
    )
    return


@app.cell(hide_code=True)
def _(DEGREES, mo):
    degree_selector = mo.ui.dropdown(
        options=DEGREES,
        value=DEGREES[0],
        label="选择异常标签进行分析 ",
    )

    mo.md(f"""
            #### 选择异常标签

            {degree_selector}
            """)
    return (degree_selector,)


@app.cell(hide_code=True)
def _(degree_selector, df_ab_eq, df_ab_le, mo):
    df_dict = {"ab_le": df_ab_le, "ab_eq": df_ab_eq}

    condtion_selector_dict = {"absolute_anomaly": ["ab_le", "ab_eq"]}

    condtion_selector = mo.ui.dropdown(
        options=condtion_selector_dict[degree_selector.value],
        value=condtion_selector_dict[degree_selector.value][0],
        label="选择异常数据进行分析 ",
    )

    mo.md(f"""
            #### 选择异常数据

            {condtion_selector}

            """)
    return condtion_selector, df_dict


@app.cell
def _(condtion_selector, df_dict, mo, pl):
    ori_data = df_dict[condtion_selector.value]

    fault_summary_df: pl.DataFrame = (
        ori_data.group_by("fault")
        .agg(
            [
                pl.col("mapping_key").n_unique().alias("service_count"),
                pl.col("count").mean().alias("avg_count"),
                pl.col("count").max().alias("max_count"),
            ]
        )
        .sort(["max_count", "avg_count"], descending=True)
    )

    fault_summary_table = mo.ui.table(fault_summary_df.to_pandas())
    return (fault_summary_df,)


@app.cell(hide_code=True)
def _(fault_summary_df: "pl.DataFrame", mo):
    all_faults = fault_summary_df["fault"].unique().sort().to_list()
    fault_selector = mo.ui.dropdown(
        options=all_faults, value=all_faults[0] if all_faults else None, label="选择故障类型进行深入分析:"
    )

    mo.md(f"""
            #### 2. 选择故障类型

            {fault_selector}

            **可选故障类型:** {len(all_faults)} 个
            """)
    return (fault_selector,)


@app.cell(hide_code=True)
def _(content, fault_selector, mo, pl):
    selected_fault = fault_selector.value
    fault_data = [item for item in content["absolute_anomaly_le"] if item["fault"] == selected_fault]

    fault_df = pl.DataFrame(data=fault_data)

    mapping_key_summary = (
        fault_df.group_by(["mapping_key"])
        .agg(
            [
                pl.col("count").mean().alias("avg_count"),
                pl.col("count").max().alias("max_count"),
            ]
        )
        .sort(["max_count", "avg_count", "mapping_key"], descending=True)
    )

    mapping_key_summary_table = mo.ui.table(mapping_key_summary.to_pandas())
    mo.md(f"""
                #### 3. 故障类型 **{selected_fault}** 的 Service 级别分析

                总计有 {fault_df.get_column("count").sum()} 条数据

                **包含 {fault_df["mapping_key"].n_unique()} 种 Service 在不同条件下的分布**

                {mapping_key_summary_table}
                """)
    return


if __name__ == "__main__":
    app.run()
