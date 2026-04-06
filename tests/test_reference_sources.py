from __future__ import annotations

import unittest

from Scripts.reference_sources import get_nested, resolve_source_secret


class ReferenceSourcesTest(unittest.TestCase):
    def test_get_nested_returns_none_for_missing_path(self) -> None:
        payload = {"tushare": {"token": "abc"}}
        self.assertIsNone(get_nested(payload, "tushare.missing"))

    def test_resolve_source_secret_prefers_local_config(self) -> None:
        registry = {
            "sources": {
                "tushare_hk_basic": {
                    "local_credential_key": "tushare.token",
                    "credential_env": "TUSHARE_TOKEN",
                }
            }
        }
        local_config = {"tushare": {"token": "local-token"}}
        env = {"TUSHARE_TOKEN": "env-token"}
        self.assertEqual(
            resolve_source_secret(
                "tushare_hk_basic",
                registry=registry,
                local_config=local_config,
                env=env,
            ),
            "local-token",
        )


if __name__ == "__main__":
    unittest.main()
