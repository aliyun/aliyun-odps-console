"""Runtime setup applied only when MaxC actually creates a PyODPS client."""

from __future__ import annotations

_agent_user_agent: str | None = None


def set_agent_user_agent(value: str | None) -> None:
    """Set the optional Agent/Skill identity appended to remote requests."""
    global _agent_user_agent
    _agent_user_agent = value


def current_agent_user_agent() -> str | None:
    """Return the validated Agent identity active for this CLI invocation."""
    return _agent_user_agent


def outbound_http_user_agent() -> str:
    """Return the safe identity used by non-PyODPS HTTP clients."""
    from . import __version__

    value = f"maxc-cli/{__version__}"
    if _agent_user_agent:
        value = f"{value} {_agent_user_agent}"
    return value


def configure_user_agent() -> None:
    """Attach the MaxC identity to PyODPS requests without slowing local commands."""
    try:
        from odps import options
    except ImportError:
        return

    pattern = f"{outbound_http_user_agent()} $pyodps_version $python_version $os_version"
    options.user_agent_pattern = pattern
