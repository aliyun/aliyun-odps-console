"""Runtime setup applied only when MaxC actually creates a PyODPS client."""

from __future__ import annotations


def configure_user_agent() -> None:
    """Attach the MaxC identity to PyODPS requests without slowing local commands."""
    try:
        from odps import options
    except ImportError:
        return

    from . import __version__

    options.user_agent_pattern = (
        f"maxc-cli/{__version__} $pyodps_version $python_version $os_version"
    )
