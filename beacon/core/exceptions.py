class UnsupportedIntervalError(ValueError):
    def __init__(self, every: str):
        super().__init__(f"Unsupported every='{every}'. Use e.g. 1, 0.5.")
