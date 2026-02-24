"""Log format handlers - import all to trigger auto-registration."""
from src.handlers.json_handler import JsonHandler
from src.handlers.text_handler import TextHandler
from src.handlers.protobuf_handler import ProtobufHandler
from src.handlers.avro_handler import AvroHandler

__all__ = ["JsonHandler", "TextHandler", "ProtobufHandler", "AvroHandler"]
