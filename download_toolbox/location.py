from dataclasses import dataclass


@dataclass
class Location:
    """Representation of spatiotemporal location

    TODO: The intention is to converge on the geoapi representation

    https://www.geoapi.org/snapshot/python/metadata.html#spatial-representation
    """
    name: str
    bounds: tuple
    north: bool
    south: bool

    def __init__(self,
                 name: str,
                 bounds: tuple = None,
                 north: bool = False,
                 south: bool = False):
        self.name = name
        self.north = north
        self.south = south

        if bounds is not None and (north | south):
            raise RuntimeError("Provide a single location")

        self.bounds = list(bounds) if bounds is not None else \
            [90, -180, 0, 180] if north else \
            [0, -180, -90, 180] if south else \
            [90, -180, -90, 180]


