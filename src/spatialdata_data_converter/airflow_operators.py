"""BashOperator that records what it produced as an Airflow asset event.

The same step (e.g. converting xenium_rep1_io) runs as a task in several DAGs: the orchestrator,
and the single-step DAG for running it by hand. Each success records an event on one asset per
step and item -- `to_zarr/xenium_rep1_io`, `tests/spatialdata-plot`, `notebook/densenet`,
`env/ome_sdc` -- so the Assets view shows one history per item across all DAGs and runs.

Where the item is known when the DAG is parsed (single-step DAGs, record_env), the task declares
its asset as a static outlet. That is also what makes the asset show up in the Assets view
(Airflow lists only assets some DAG references). Mapped tasks (the orchestrator's stages) don't
know their item until run time; they declare the kind's alias and add the item's asset to it.
"""
import json

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, AssetAlias


def asset_alias(kind: str) -> AssetAlias:
    """The alias through which mapped tasks of one kind emit their per-item asset events."""
    return AssetAlias(f"sdc_{kind}")


def sdc_asset(kind: str, item: str) -> Asset:
    return Asset(name=f"{kind}/{item}", uri=f"x-sdc://{kind}/{item}", group=f"sdc_{kind}")


class AssetBashOperator(BashOperator):
    """A BashOperator whose success records an event on the asset `<asset_kind>/<item>`.

    item: `asset_item`, else the NOTEBOOK env var (notebook stage, mapped over env), else the last
    word of the command (`... --dataset X`, `... --repo X`, `... --notebook X`).

    Without explicit `outlets`, the asset is a static outlet (item known now: single-step DAGs).
    Mapped tasks pass `outlets=[asset_alias(kind)]` in `.partial()`; each instance then adds its
    own item's asset to the alias. If the command's last output line is a JSON object, it becomes
    the event's `extra` (e.g. record-env's repo commits and package-list hash).
    """

    def __init__(self, *, asset_kind: str, asset_item: str | None = None, **kwargs):
        if "outlets" not in kwargs:
            item = asset_item or str(kwargs["bash_command"]).split()[-1]
            kwargs["outlets"] = [sdc_asset(asset_kind, item)]
        super().__init__(**kwargs)
        self.asset_kind = asset_kind
        self.asset_item = asset_item

    def _item(self) -> str:
        return self.asset_item or (self.env or {}).get("NOTEBOOK") or self.bash_command.split()[-1]

    def execute(self, context):
        result = super().execute(context)
        extra = {}
        if isinstance(result, str):
            try:
                parsed = json.loads(result)
            except ValueError:
                parsed = None
            if isinstance(parsed, dict):
                extra = parsed
        for outlet in self.outlets:
            if isinstance(outlet, AssetAlias):
                context["outlet_events"][outlet].add(sdc_asset(self.asset_kind, self._item()), extra=extra)
            elif isinstance(outlet, Asset) and extra:
                context["outlet_events"][outlet].extra = extra  # the static outlet's event
        return result
