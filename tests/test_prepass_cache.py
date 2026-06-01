import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from backend.prepass_cache import (
    load_cached_prepass_rows,
    merge_prepass_source_rows,
    save_cached_prepass_rows,
)


class PrepassCacheTests(unittest.TestCase):
    def test_save_and_load_final_source_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_root = Path(tmp)
            rows = [
                {
                    "source_id": "old",
                    "title": "Permit Approval",
                    "url": "https://example.com/permit.pdf",
                    "published_at": "2026-05-01",
                    "excerpt": "Permit approved.",
                    "bundle_price_sensitive": True,
                }
            ]

            save_meta = save_cached_prepass_rows(
                cache_root=cache_root,
                ticker="ASX:STK",
                exchange="ASX",
                template_id="resources_gold_developer",
                company_name="Strickland Metals Limited",
                source_rows=rows,
                source_meta={"strategy": "built_fresh_bundle", "prepass_top": 20},
            )
            loaded_rows, loaded_meta = load_cached_prepass_rows(
                cache_root=cache_root,
                ticker="ASX:STK",
                exchange="ASX",
                template_id="resources_gold_developer",
                max_age_days=90,
            )

            self.assertEqual(save_meta["cache_status"], "saved")
            self.assertEqual(loaded_meta["cache_status"], "hit")
            self.assertEqual(len(loaded_rows), 1)
            self.assertEqual(loaded_rows[0]["source_id"], "S1")
            self.assertEqual(loaded_rows[0]["title"], "Permit Approval")

    def test_merge_prepends_delta_and_deduplicates_by_url(self):
        cached_rows = [
            {
                "source_id": "S1",
                "title": "Old Resource Update",
                "url": "https://example.com/resource.pdf?utm=1",
                "published_at": "2026-02-01",
                "excerpt": "Old resource update.",
            },
            {
                "source_id": "S2",
                "title": "Placement",
                "url": "https://example.com/placement.pdf",
                "published_at": "2026-01-15",
                "excerpt": "Placement completed.",
            },
        ]
        delta_rows = [
            {
                "source_id": "S1",
                "title": "Permitting Delay",
                "url": "https://example.com/permit-delay.pdf",
                "published_at": "2026-05-20",
                "excerpt": "Permit delayed.",
            },
            {
                "source_id": "S2",
                "title": "Old Resource Update Duplicate",
                "url": "https://example.com/resource.pdf",
                "published_at": "2026-02-01",
                "excerpt": "Duplicate.",
            },
        ]

        merged, meta = merge_prepass_source_rows(
            cached_rows=cached_rows,
            delta_rows=delta_rows,
            max_rows=24,
        )

        self.assertEqual([row["title"] for row in merged], ["Permitting Delay", "Old Resource Update Duplicate", "Placement"])
        self.assertEqual([row["source_id"] for row in merged], ["S1", "S2", "S3"])
        self.assertEqual(merged[0]["prepass_row_origin"], "delta")
        self.assertEqual(meta["deduplicated_rows_count"], 1)

    def test_stale_cache_is_not_loaded(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache_root = Path(tmp)
            save_meta = save_cached_prepass_rows(
                cache_root=cache_root,
                ticker="ASX:STK",
                exchange="ASX",
                template_id="resources_gold_developer",
                company_name="Strickland Metals Limited",
                source_rows=[{"title": "Old", "url": "https://example.com/old.pdf", "excerpt": "Old"}],
                source_meta={},
            )
            path = Path(save_meta["cache_path"])
            payload = json.loads(path.read_text(encoding="utf-8"))
            old = (datetime.utcnow() - timedelta(days=120)).replace(microsecond=0).isoformat() + "Z"
            payload["created_at_utc"] = old
            payload["updated_at_utc"] = old
            path.write_text(json.dumps(payload), encoding="utf-8")

            rows, meta = load_cached_prepass_rows(
                cache_root=cache_root,
                ticker="ASX:STK",
                exchange="ASX",
                template_id="resources_gold_developer",
                max_age_days=90,
            )

            self.assertEqual(rows, [])
            self.assertEqual(meta["cache_status"], "stale")
            self.assertEqual(meta["cache_reason"], "cache_too_old")


if __name__ == "__main__":
    unittest.main()
