"""Unit tests verifying setup_vm.sh configuration, safety, and systemd definitions."""

from pathlib import Path
import pytest


@pytest.fixture
def vm_script_path() -> Path:
    base_dir = Path(__file__).resolve().parent.parent.parent
    return base_dir / "scripts" / "setup_vm.sh"


def test_setup_vm_script_structure(vm_script_path: Path):
    """Verify setup_vm.sh contains essential safety flags, swap creation, and systemd service."""
    assert vm_script_path.exists(), "setup_vm.sh must exist in scripts/"

    script_content = vm_script_path.read_text(encoding="utf-8")

    # Script safety flags
    assert script_content.startswith("#!/usr/bin/env bash")
    assert "set -euo pipefail" in script_content

    # Swap configuration (Guards e2-micro 1GB RAM)
    assert "mkswap" in script_content
    assert "vm.swappiness=10" in script_content

    # User & Directory isolation
    assert "flightengine" in script_content
    assert "/opt/flight-weather-engine" in script_content

    # Systemd Service Definition
    assert "Description=Eco-Friendly Hybrid Flight-Weather Lakehouse Stream Engine" in script_content
    assert "Restart=always" in script_content
    assert "RestartSec=15" in script_content

    # Weekly Archival Cron Definition
    assert "/etc/cron.d/flight-archival-sentinel" in script_content
    assert "EvictionSentinel" in script_content

    # Firewall hardening
    assert "ufw allow 22/tcp" in script_content


def test_no_hardcoded_secrets_in_script(vm_script_path: Path):
    """Verify script does not contain plain-text passwords or secret keys."""
    script_content = vm_script_path.read_text(encoding="utf-8")
    forbidden_tokens = ["password=", "secret_key=", "api_key=", "AIzaSy", "Bearer "]
    for token in forbidden_tokens:
        assert token not in script_content, f"Found forbidden token {token} in setup_vm.sh"
