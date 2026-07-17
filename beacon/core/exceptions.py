class UnsupportedIntervalError(ValueError):
    """Raised when a publisher `every` interval is not a positive number."""

    def __init__(self, every: float):
        super().__init__(f"Unsupported every='{every}'. Use e.g. 1, 0.5.")


class TableDefinitionError(TypeError):
    """Raised when a `Table` subclass declares an invalid schema."""

    def __init__(self, table: str, reason: str):
        super().__init__(f"invalid table definition '{table}': {reason}")


class StorageNotReadyError(RuntimeError):
    """Raised when the storage engine is used before `start()`."""

    def __init__(self, what: str):
        super().__init__(
            f"storage not ready: {what}; "
            "storage starts with app.start() (or StorageEngine.start())"
        )


class UplinkNotReadyError(RuntimeError):
    """Raised when an uplink component is used before `start()`."""

    def __init__(self, what: str):
        super().__init__(f"uplink not ready: {what}")


class UplinkNotEnabledError(RuntimeError):
    """Raised when enqueuing to the uplink while it is disabled in config."""

    def __init__(self, stream: str):
        super().__init__(
            f"uplink is disabled; cannot enqueue to stream '{stream}'. "
            "Set uplink.enabled: true in config."
        )


class UnknownLookupError(LookupError):
    """Raised when a query uses a filter lookup the table doesn't support."""

    def __init__(self, table: str, lookup: str):
        super().__init__(f"unknown lookup '{lookup}' for table '{table}'")
