class SparkConnectionException(Exception):
    def __init__(self):
        super().__init__("ERROR: could not connect to spark cluster")


class RedisConnectionException(Exception):
    def __init__(self):
        super().__init__("ERROR: Could Not Connet To Redis")


class FacadeInstanceCreation(Exception):
    def __init__(self):
        super().__init__("ERROR: Cannot Instantiate Facade")
