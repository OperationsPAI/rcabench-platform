from typing import Any

import altair as alt
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import polars as pl

from .models import BarMeta, BubbleMeta, HeatmapMeta, NewVisInjectionsConfig


def plot_bar(meta: BarMeta) -> alt.HConcatChart | alt.ConcatChart:
    assert meta.data, "Data for bar chart cannot be empty"

    is_metrics_bar = "metric" in meta.x_label.lower()

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
    degree_opacity = alt.when(degree_selection).then(alt.value(0.9)).otherwise(alt.value(0.1))

    bar: alt.Chart | None = None
    if not is_metrics_bar:
        bar = (
            base.mark_bar(stroke="navy", strokeWidth=1, opacity=0.7)
            .encode(
                x=alt.X("name:N", title=meta.x_label, axis=alt.Axis(labelAngle=-45), sort="-y"),
                y=alt.Y("count:Q", title="Injection Count"),
                color=degree_color,
                opacity=degree_opacity,
                xOffset="degree:N",
                tooltip=["degree:N", "name:N", "count:Q"],
            )
            .properties(width=800, height=400, title=meta.title)
        )
    else:
        other_columns = df.select(pl.exclude("degree")).columns
        dropdown = alt.binding_select(options=other_columns, name="X-axis column ")
        xcol_param = alt.param(value=other_columns[0], bind=dropdown)

        bar = (
            base.mark_bar(stroke="navy", strokeWidth=1, opacity=0.7)
            .encode(x=alt.X("x:Q").title(""), y=alt.Y("count:Q", title="Injection Count"), color="Origin:N")
            .transform_calculate(x=f"datum[{xcol_param.name}]")
            .add_params(xcol_param)
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
                    color="white" if meta.matrix[i, j] > meta.matrix.max() / 2 else "black",
                )

    plt.tight_layout()

    plt.savefig(meta.save_path, dpi=300, bbox_inches="tight")


def plot_3d_bubble(meta: BubbleMeta) -> None:
    colors = px.colors.qualitative.Set3
    if len(meta.layer_datas) > len(colors):
        colors = colors * ((len(meta.layer_datas) // len(colors)) + 1)

    value_key_to_color = {key: colors[i % len(colors)] for i, key in enumerate(meta.layer_datas.keys())}

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

    fig.write_image(meta.save_path, width=1200, height=800, scale=2, engine="kaleido")


class VisInjections:
    config = NewVisInjectionsConfig()

    def __init__(self, distributions_dict: dict[str, dict[str, Any]], metrics: list[str]):
        self.distributions_dict = distributions_dict
        self.metrics = metrics

    def display_bars(
        self,
    ) -> dict[str, alt.HConcatChart | alt.ConcatChart | dict[str, alt.HConcatChart | alt.ConcatChart]]:
        """
        Display bar charts for fault, service distributions.
        """

        def _display_bar(key: str, x_label: str, title: str) -> alt.HConcatChart | alt.ConcatChart:
            combined_data = []

            for degree, distributions in self.distributions_dict.items():
                data: dict[str, int | dict[str, int]] = distributions.get(key, {})
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
                        combined_data.append(
                            {
                                "degree": degree,
                                "name": sub_name,
                                "count": sub_count,
                            }
                        )

            return plot_bar(
                BarMeta(
                    data=combined_data,
                    x_label=x_label,
                    y_label="Injection Count",
                    title=title,
                )
            )

        bars: dict[str, alt.HConcatChart | alt.ConcatChart | dict[str, alt.HConcatChart | alt.ConcatChart]] = {}

        for key, config in self.config.bar_configs.items():
            bars[key] = _display_bar(key, config.x_label, config.title)

        return bars
