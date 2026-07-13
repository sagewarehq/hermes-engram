"""hermes-engram — agent-side plugin shell.

The real surface lives in ``dashboard/plugin_api.py``, mounted by the hermes
dashboard at ``/api/plugins/hermes-engram``. This module exists so the plugin
shows up in ``hermes plugins`` and participates in the ``plugins.enabled``
gate that the dashboard's plugin-route mounter checks for user-source plugins.
"""

import logging

log = logging.getLogger(__name__)


def register(ctx):
    log.debug("hermes-engram registered (API served by the dashboard plugin)")
