"""
Python automatically imports `sitecustomize` on startup (if present on sys.path).

We use this to shim compatibility between Meltano and the `snowplow-tracker` package.
Some `snowplow-tracker` versions expose `SelfDescribingJson` but not `SelfDescribingJs`
even though some Meltano versions try to import both.

This shim is intentionally minimal and safe: if anything goes wrong, we do nothing.
"""

from __future__ import annotations

import inspect


def _shim_snowplow_tracker() -> None:
    try:
        import snowplow_tracker  # type: ignore
    except Exception:
        return

    # Some versions only export SelfDescribingJson; provide aliases expected by Meltano.
    if hasattr(snowplow_tracker, "SelfDescribingJson"):
        if not hasattr(snowplow_tracker, "SelfDescribing"):
            snowplow_tracker.SelfDescribing = snowplow_tracker.SelfDescribingJson  # type: ignore[attr-defined]
        if not hasattr(snowplow_tracker, "SelfDescribingJs"):
            snowplow_tracker.SelfDescribingJs = snowplow_tracker.SelfDescribingJson  # type: ignore[attr-defined]

    # Some snowplow_tracker builds have an older Emitter signature. Meltano may pass
    # newer kwargs like `request_timeout`; drop unknown kwargs to avoid crashing.
    try:
        from snowplow_tracker.emitters import Emitter  # type: ignore

        orig_init = Emitter.__init__
        sig = inspect.signature(orig_init)

        def patched_init(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            filtered = {k: v for k, v in kwargs.items() if k in sig.parameters}
            return orig_init(self, *args, **filtered)

        Emitter.__init__ = patched_init  # type: ignore[assignment]
    except Exception:
        return


_shim_snowplow_tracker()


