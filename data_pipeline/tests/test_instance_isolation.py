from __future__ import annotations

import os
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from instance_config import acquire_trader_lock, history_path, load_instance_environment
from api.identity import configured_instance_id
from deriv_trading_client import DerivTradingClient


class InstanceIsolationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        for instance, token in (("local", "local-test-token"), ("vps", "vps-test-token")):
            (self.root / f".env.{instance}").write_text(
                f"INSTANCE_ID={instance}\n"
                f"DATA_DIR=data/{instance}\n"
                f"DERIV_APP_ID=app-{instance}\n"
                f"DERIV_API_TOKEN={token}\n"
                f"DERIV_DEMO_ACCOUNT_ID=account-{instance}\n"
            )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_instance_must_be_explicit_and_valid(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "must be set"):
            load_instance_environment(project_root=self.root, environ={})
        with self.assertRaisesRegex(RuntimeError, "must be set"):
            load_instance_environment(
                project_root=self.root,
                environ={"INSTANCE_ID": "production"},
            )

    def test_direct_macos_launch_defaults_to_local_instance(self) -> None:
        with (
            patch.dict(os.environ, {}, clear=True),
            patch("instance_config.sys.platform", "darwin"),
        ):
            selected = load_instance_environment(project_root=self.root)
            self.assertEqual(selected, "local")
            self.assertEqual(os.environ["INSTANCE_ID"], "local")
            self.assertEqual(os.environ["DERIV_API_TOKEN"], "local-test-token")
            self.assertEqual(os.environ["DATA_DIR"], str((self.root / "data/local").resolve()))

    def test_unset_instance_still_fails_closed_on_server_platforms(self) -> None:
        with (
            patch.dict(os.environ, {}, clear=True),
            patch("instance_config.sys.platform", "linux"),
        ):
            with self.assertRaisesRegex(RuntimeError, "must be set"):
                load_instance_environment(project_root=self.root)

    def test_trader_lock_prevents_two_processes_from_sharing_one_instance(self) -> None:
        local_dir = self.root / "data" / "local"
        vps_dir = self.root / "data" / "vps"
        local_lock = acquire_trader_lock(local_dir)
        try:
            with self.assertRaisesRegex(RuntimeError, "already running"):
                acquire_trader_lock(local_dir)
            vps_lock = acquire_trader_lock(vps_dir)
            vps_lock.close()
        finally:
            local_lock.close()

        retry_lock = acquire_trader_lock(local_dir)
        retry_lock.close()

    def test_each_instance_loads_only_its_selected_file_and_data_path(self) -> None:
        roots = {}
        with patch.dict(os.environ, {}, clear=True):
            for instance in ("local", "vps"):
                env = {
                    "INSTANCE_ID": instance,
                    "DERIV_API_TOKEN": "ambient-wrong-token",
                    "POSTGRES_PASSWORD": "ambient-wrong-password",
                }
                self.assertEqual(
                    load_instance_environment(project_root=self.root, environ=env),
                    instance,
                )
                self.assertEqual(env["DERIV_API_TOKEN"], f"{instance}-test-token")
                self.assertEqual(env["DERIV_DEMO_ACCOUNT_ID"], f"account-{instance}")
                self.assertEqual(env["DERIV_APP_ID"], f"app-{instance}")
                self.assertNotIn("POSTGRES_PASSWORD", env)
                os.environ.update(env)
                resolved = history_path(
                    "trade_history.csv",
                    Path(__file__).resolve().parents[1] / "trade_history.csv",
                )
                roots[instance] = resolved
                self.assertEqual(
                    resolved.resolve(),
                    (self.root / "data" / instance / "trade_history.csv").resolve(),
                )
                os.environ.clear()
        self.assertNotEqual(roots["local"], roots["vps"])

    def test_mismatched_file_instance_fails_closed(self) -> None:
        (self.root / ".env.local").write_text(
            "INSTANCE_ID=vps\nDATA_DIR=data/vps\nDERIV_API_TOKEN=wrong\n"
        )
        with self.assertRaisesRegex(RuntimeError, "must be 'local'"):
            load_instance_environment(
                project_root=self.root,
                environ={"INSTANCE_ID": "local"},
            )

    def test_container_uses_only_the_preloaded_selected_environment(self) -> None:
        env = {
            "INSTANCE_ID": "vps",
            "INSTANCE_ENV_PRELOADED": "1",
            "DATA_DIR": "data/vps",
            "DERIV_APP_ID": "app-vps",
            "DERIV_API_TOKEN": "vps-test-token",
            "DERIV_DEMO_ACCOUNT_ID": "account-vps",
        }
        self.assertEqual(
            load_instance_environment(project_root=self.root, environ=env),
            "vps",
        )
        self.assertEqual(env["DERIV_API_TOKEN"], "vps-test-token")
        self.assertEqual(env["DATA_DIR"], str((self.root / "data" / "vps").resolve()))

    def test_dashboard_identity_is_explicit_and_validated(self) -> None:
        self.assertEqual(configured_instance_id({"INSTANCE_ID": "local"}), "local")
        self.assertEqual(configured_instance_id({"INSTANCE_ID": "vps"}), "vps")
        with self.assertRaisesRegex(RuntimeError, "INSTANCE_ID"):
            configured_instance_id({})

    def test_account_logging_masks_identifiers_and_never_prints_token(self) -> None:
        class FakeResponse:
            def raise_for_status(self) -> None:
                pass

            @staticmethod
            def json() -> dict:
                return {
                    "data": [{
                        "account_id": "DOT123456789",
                        "account_type": "demo",
                        "status": "active",
                        "currency": "USD",
                    }]
                }

        client = DerivTradingClient("app-id-secret", "api-token-secret")
        output = io.StringIO()
        with patch("deriv_trading_client.requests.get", return_value=FakeResponse()), redirect_stdout(output):
            client.list_accounts()
        self.assertNotIn("api-token-secret", output.getvalue())
        self.assertNotIn("DOT123456789", output.getvalue())
        self.assertIn("…789", output.getvalue())

    def test_missing_credentials_fail_without_fallback(self) -> None:
        import importlib
        with patch.dict(os.environ, {"INSTANCE_ID": "local"}, clear=True):
            config = importlib.import_module("config")

        for variable in ("DERIV_API_TOKEN", "DERIV_DEMO_ACCOUNT_ID"):
            with self.subTest(variable=variable), patch.dict(os.environ, {}, clear=True):
                with self.assertRaisesRegex(RuntimeError, variable + " is not set"):
                    config._required(variable)


if __name__ == "__main__":
    unittest.main()
