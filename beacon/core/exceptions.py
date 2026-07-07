class UnsupportedIntervalError(ValueError):
    def __init__(self, every: float):
        super().__init__(f"Unsupported every='{every}'. Use e.g. 1, 0.5.")


class TableDefinitionError(TypeError):
    def __init__(self, table: str, reason: str):
        super().__init__(f"invalid table definition '{table}': {reason}")


class StorageNotReadyError(RuntimeError):
    def __init__(self, what: str):
        super().__init__(
            f"storage not ready: {what}; "
            "storage starts with app.start() (or StorageEngine.start())"
        )


class UnknownLookupError(LookupError):
    def __init__(self, table: str, lookup: str):
        super().__init__(f"unknown lookup '{lookup}' for table '{table}'")
