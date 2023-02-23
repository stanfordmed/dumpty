from decimal import Decimal

from tinydb_serialization import Serializer


class DecimalSerializer(Serializer):
    OBJ_CLASS = Decimal

    def encode(self, obj):
        return str(obj)

    def decode(self, s):
        return Decimal(s)


class FloatSerializer(Serializer):
    OBJ_CLASS = float

    def encode(self, obj):
        return str(obj)

    def decode(self, s):
        return float(s)
