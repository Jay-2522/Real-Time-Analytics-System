"""Subprocess target for genuine process-exit-before-ack integration regression."""
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from support import engine, storage, consumer, next_message
from pipeline_runtime.identity import coordinate

sink=storage(engine())
c,loop=consumer(os.environ['TEST_TOPIC'],os.environ['TEST_GROUP'])
message=next_message(c)

def handle(message):
    sink._handle_message(message.topic(),message.value(),coordinate(message))
    if os.environ['TEST_MODE']=='crash':
        os._exit(77)  # PostgreSQL committed; Kafka input offset deliberately unacknowledged.

loop.process(message,handle)
c.close()
