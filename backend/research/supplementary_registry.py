"""Registry and routing helpers for sector supplementary pipelines."""

from __future__ import annotations

from typing import Dict, List, Optional

from .energy_oil_gas_supplementary import get_pipeline_spec as get_energy_pipeline_spec
from .mining_supplementary import get_pipeline_spec as get_resources_pipeline_spec
from .pharma_biotech_supplementary import get_pipeline_spec as get_pharma_pipeline_spec
from .software_saas_supplementary import get_pipeline_spec as get_software_pipeline_spec
from .supplementary_base import SupplementaryPipelineSpec


_PIPELINE_SPECS: Dict[str, SupplementaryPipelineSpec] = {
    spec.pipeline_id: spec
    for spec in [
        get_resources_pipeline_spec(),
        get_pharma_pipeline_spec(),
        get_software_pipeline_spec(),
        get_energy_pipeline_spec(),
    ]
}


def list_pipeline_specs() -> List[SupplementaryPipelineSpec]:
    return list(_PIPELINE_SPECS.values())


def get_pipeline_spec(pipeline_id: str) -> Optional[SupplementaryPipelineSpec]:
    return _PIPELINE_SPECS.get(str(pipeline_id or "").strip())


def resolve_pipeline_id_for_template(template_id: str) -> Optional[str]:
    from ..template_loader import get_template_loader

    loader = get_template_loader()
    contract = loader.get_template_contract(str(template_id or "").strip()) or {}
    pipeline_id = str((contract or {}).get("supplementary_pipeline_id") or "").strip()
    if pipeline_id:
        return pipeline_id
    family = str((contract or {}).get("family") or "").strip()
    fallback_by_family = {
        "resources": "resources_supplementary",
        "pharma": "pharma_biotech_supplementary",
        "software": "software_saas_supplementary",
        "energy": "energy_oil_gas_supplementary",
    }
    return fallback_by_family.get(family)


def resolve_pipeline_spec_for_template(template_id: str) -> Optional[SupplementaryPipelineSpec]:
    pipeline_id = resolve_pipeline_id_for_template(template_id)
    if not pipeline_id:
        return None
    return get_pipeline_spec(pipeline_id)
