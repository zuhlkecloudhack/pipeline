import json
from store_pipeline import ParseFlightEventFn


def test_ParseGameEventFn():
    testee = ParseFlightEventFn()
    msg = {
            "flight-number" : "CH5634",
            "message" : "Fly me to the moon",
            "message-type" : "INFO",
            "timestamp" : "2012-04-23T18:25:43.511Z"
           }
    elems = list(testee.process(json.dumps(msg)))
    elem = elems[0]