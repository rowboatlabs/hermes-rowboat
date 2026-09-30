"""Load the plugin as a package inside a Hermes checkout, the way Hermes loads it.

Run with Hermes's own Python so ``gateway.*`` resolves:

    PYTHONPATH=.testdeps:$HERMES_SRC $HERMES_SRC/venv/bin/python -m pytest tests

where HERMES_SRC is a hermes-agent checkout (default ~/.hermes/hermes-agent).
"""

import importlib.util
import os
import pathlib
import sys

HERMES_SRC = pathlib.Path(os.environ.get("HERMES_SRC", pathlib.Path.home() / ".hermes" / "hermes-agent"))
if str(HERMES_SRC) not in sys.path:
    sys.path.insert(0, str(HERMES_SRC))

_ROOT = pathlib.Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("rowboat_platform", _ROOT / "__init__.py", submodule_search_locations=[str(_ROOT)])
_pkg = importlib.util.module_from_spec(_spec)
sys.modules["rowboat_platform"] = _pkg
_spec.loader.exec_module(_pkg)


class _RegisteringCtx:
    """What ``ctx.register_platform`` does (hermes_cli/plugins.py): the real PlatformEntry, so an
    argument Hermes doesn't know fails here, and Platform("rowboat") becomes a known platform."""

    def register_platform(self, name, label, adapter_factory, check_fn, validate_config=None, required_env=None,
                          install_hint="", **entry_kwargs):
        from gateway.platform_registry import PlatformEntry, platform_registry

        entry_kwargs.setdefault("plugin_name", "rowboat-platform")
        platform_registry.register(PlatformEntry(
            name=name, label=label, adapter_factory=adapter_factory, check_fn=check_fn,
            validate_config=validate_config, required_env=required_env or [], install_hint=install_hint,
            source="plugin", **entry_kwargs,
        ))


_pkg.register(_RegisteringCtx())
