from sqlalchemy.testing.suite import *  # noqa: F401, F403


import json
from unittest import mock

from sqlalchemy import select
from sqlalchemy.testing import engines, eq_
from sqlalchemy.testing.suite import JSONTest as _JSONTest


class JSONTest(_JSONTest):  # noqa: F811
    def test_round_trip_custom_json(self):
        """Upstream asserts the custom deserializer receives the EXACT
        stored JSON text. The Data API canonicalises JSON on the wire
        (strips whitespace, may reorder keys), so byte-exactness is not a
        guarantee this driver can offer; assert semantic (deserialized)
        equality of the deserializer input instead.
        """
        data_table = self.tables.data_table
        data_element = {"key1": "data1"}

        js = mock.Mock(side_effect=json.dumps)
        jd = mock.Mock(side_effect=json.loads)
        engine = engines.testing_engine(
            options=dict(json_serializer=js, json_deserializer=jd)
        )
        # Under the async suite testing_engine returns an AsyncEngine;
        # this body is sync (greenlet-bridged), so unwrap.
        engine = getattr(engine, "sync_engine", engine)

        data_table.create(engine, checkfirst=True)
        with engine.begin() as conn:
            conn.execute(
                data_table.insert(), {"name": "row1", "data": data_element}
            )
            row = conn.execute(select(data_table.c.data)).first()

            eq_(row, (data_element,))
            eq_(js.mock_calls, [mock.call(data_element)])
            eq_(len(jd.mock_calls), 1)
            (received,) = jd.mock_calls[0].args
            if isinstance(received, bytes):
                received = received.decode()
            eq_(json.loads(received), data_element)

