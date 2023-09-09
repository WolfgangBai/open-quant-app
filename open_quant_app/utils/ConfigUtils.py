import toml


class ConfigUtils:
    @staticmethod
    def load(path: str) -> dict:
        return toml.load(path)
