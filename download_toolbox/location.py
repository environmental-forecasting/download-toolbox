class Location:
    """Representation of spatiotemporal location

    TODO: The intention is to converge on the geoapi representation

    https://www.geoapi.org/snapshot/python/metadata.html#spatial-representation
    """

    def __init__(self,
                 name: str,
                 bounds: tuple = None,
                 north: bool = False,
                 south: bool = False):
        self._name = name
        self._north = north
        self._south = south

        assert (north or south) ^ (bounds is not None), "Provide a single location"

        self._bounds = list(bounds) if (not north and not south) else \
            [90, -180, 0, 180] if north else \
            [0, -180, -90, 180] if south else \
            [90, -180, -90, 180]

    @property
    def bounds(self):
        return self._bounds

    @property
    def world(self):
        return (self._north and self._south) or sum(self._bounds) == 540

    @property
    def name(self):
        return self._name

    @property
    def north(self):
        return self._north

    @property
    def south(self):
        return self._south