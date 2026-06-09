from beacon.core.config import BeaconConfig

# Beacon (beacon.core.app) is intentionally not re-exported here: app.py
# imports beacon.mqtt, whose decorators import beacon.core.exceptions, so an
# app import in this __init__ makes `import beacon.mqtt` fail on a circular
# import. Import it via `from beacon.core.app import Beacon`.

__all__ = ["BeaconConfig"]
