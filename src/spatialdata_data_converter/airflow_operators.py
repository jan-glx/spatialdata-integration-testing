"""BashOperator that records what it produced as an Airflow asset event.

The same step (e.g. converting xenium_rep1_io) runs as a task in several DAGs: the orchestrator,
and the single-step DAG for running it by hand. Each success records an event on one asset per
step and item -- `to_zarr/xenium_rep1_io`, `tests/spatialdata-plot`, `notebook/densenet`,
`env/ome_sdc` -- so the Assets view shows one history per item across all DAGs and runs.
"""
import json

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, AssetAlias


def asset_alias(kind: str) -> AssetAlias:
    """The alias through which tasks of one kind emit their per-item asset events."""
    return AssetAlias(f"sdc_{kind}")


def sdc_asset(kind: str, item: str) -> Asset:
    return Asset(name=f"{kind}/{item}", uri=f"x-sdc://{kind}/{item}", group=f"sdc_{kind}")


class AssetBashOperator(BashOperator):
    """A BashOperator whose success records an event on the asset `<asset_kind>/<item>`.

    item: `asset_item`, else the NOTEBOOK env var (notebook stage, mapped over env), else the last
    word of the command (`... --dataset X`, `... --repo X`, `... --notebook X`). Works mapped too:
    each instance records its own item. If the command's last output line is a JSON object, it
    becomes the event's `extra` (e.g. record-env's repo commits and package-list hash).
    """

    def __init__(self, *, asset_kind: str, asset_item: str | None = None, **kwargs):
        kwargs.setdefault("outlets", [asset_alias(asset_kind)])
        super().__init__(**kwargs)
        self.asset_kind = asset_kind
        self.asset_item = asset_item

    def execute(self, context):
        result = super().execute(context)
        item = self.asset_item or (self.env or {}).get("NOTEBOOK") or self.bash_command.split()[-1]
        extra = {}
        if isinstance(result, str):
            try:
                parsed = json.loads(result)
            except ValueError:
                parsed = None
            if isinstance(parsed, dict):
                extra = parsed
        context["outlet_events"][asset_alias(self.asset_kind)].add(sdc_asset(self.asset_kind, item), extra=extra)
        return result
