from rcabench.openapi import DtoInjectionV2Response, DtoInjectionV2SearchReq, InjectionsApi

from rcabench_platform.v2.analysis.aggregation import aggregate, get_fault_type_stats
from rcabench_platform.v2.analysis.data_prepare import batch_process_item
from rcabench_platform.v2.clients.rcabench_ import RCABenchClient
from rcabench_platform.v2.utils.dataframe import format_dataframe

if __name__ == "__main__":
    DEGREES = ["absolute_anomaly", "may_anomaly", "no_anomaly"]

    for degree in DEGREES:
        with RCABenchClient() as client:
            injection_api = InjectionsApi(client)
            injections_dict: dict[str, list[DtoInjectionV2Response]] = {}
            resp = injection_api.api_v2_injections_search_post(
                search=DtoInjectionV2SearchReq(
                    tags=[degree],
                    include_labels=True,
                )
            )
        assert resp.data is not None and resp.data.items is not None, "No injections found with absolute anomaly degree"
        items = [i for i in resp.data.items]
        res = batch_process_item(items, [], "ts")
        df = aggregate(res)
        agg_df = get_fault_type_stats(df)

        format_dataframe(agg_df, "html", output_file=f"temp/res_{degree}.html")
