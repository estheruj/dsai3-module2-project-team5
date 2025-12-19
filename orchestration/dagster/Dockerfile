FROM python:3.10-slim

ENV DAGSTER_HOME=/opt/dagster \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_DEFAULT_TIMEOUT=600 \
    PYTHONPATH=/opt/dagster/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    git build-essential ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster

COPY orchestration/dagster/requirements.txt /opt/dagster/requirements.txt
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir --prefer-binary --retries 10 --timeout 600 -r /opt/dagster/requirements.txt

# Dagster code (repositories, jobs, schedules)
COPY orchestration/dagster /opt/dagster/app

# Shim `snowplow_tracker` exports expected by some Meltano versions.
# Python auto-imports sitecustomize (if present on sys.path).
RUN python -c "import site; from pathlib import Path; p=Path(site.getsitepackages()[0])/'sitecustomize.py'; p.write_text('from __future__ import annotations\\n\\ndef _shim_snowplow_tracker():\\n    try:\\n        import inspect\\n        import snowplow_tracker\\n    except Exception:\\n        return\\n\\n    # Export aliases expected by Meltano\\n    if hasattr(snowplow_tracker, \"SelfDescribingJson\"):\\n        if not hasattr(snowplow_tracker, \"SelfDescribing\"):\\n            snowplow_tracker.SelfDescribing = snowplow_tracker.SelfDescribingJson\\n        if not hasattr(snowplow_tracker, \"SelfDescribingJs\"):\\n            snowplow_tracker.SelfDescribingJs = snowplow_tracker.SelfDescribingJson\\n\\n    # Some bundled snowplow_tracker versions have an older Emitter signature.\\n    # Meltano may pass newer kwargs like request_timeout; drop unknown kwargs.\\n    try:\\n        from snowplow_tracker.emitters import Emitter\\n        orig_init = Emitter.__init__\\n        sig = inspect.signature(orig_init)\\n\\n        def patched_init(self, *args, **kwargs):\\n            filtered = {k: v for k, v in kwargs.items() if k in sig.parameters}\\n            return orig_init(self, *args, **filtered)\\n\\n        Emitter.__init__ = patched_init\\n    except Exception:\\n        return\\n\\n_shim_snowplow_tracker()\\n'); print('Wrote', p)"

# Ports: Dagster webserver
EXPOSE 3000

# Command provided by docker-compose per service (webserver/daemon)


