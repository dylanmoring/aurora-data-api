"""
SQLAlchemy compliance suite Requirements declaration for the Aurora
Data API postgres dialect.

History note: we previously closed ~25 reflection-related requirements
because Aurora Data API's CHAR / int2vector / generate_subscripts
restrictions broke every catalog query SA's reflection layer issues.
Those four root causes are now patched in the dialect (cast CHAR ->
TEXT, int2vector -> int[], generate_subscripts integer cast,
pg_get_indexdef integer cast), so this file is back to defaults plus a
handful of PG-shaped overrides where ``SuiteRequirements`` defaults
miss PG semantics. We let any genuinely remaining Data API limit
surface as a real test failure instead of hiding it behind a blanket
``closed()``.
"""
from sqlalchemy.testing import exclusions
from sqlalchemy.testing.requirements import SuiteRequirements


class Requirements(SuiteRequirements):
    # ---- PG reflection semantics SuiteRequirements doesn't infer ----

    @property
    def unique_constraints_reflect_as_index(self):
        # PG stores UNIQUE constraints as unique indexes; SA's
        # ``get_indexes`` returns them flagged with
        # ``duplicates_constraint``. SuiteRequirements defaults this to
        # closed() (generic baseline); for PG it's open(), and the
        # compliance suite's expected-results helper uses this flag to
        # decide whether the duplicates-constraint index entries appear
        # in the expected output.
        return exclusions.open()

    @property
    def comment_reflection(self):
        # PG supports COMMENT ON and SA reflects them via pg_description.
        return exclusions.open()

    @property
    def comment_reflection_full_unicode(self):
        return exclusions.open()

    @property
    def supports_distinct_on(self):
        # Inherited from PGDialect; the compile path emits
        # ``SELECT DISTINCT ON (col)`` correctly. The default
        # ``SuiteRequirements`` declares this ``closed()`` because the
        # suite-shipped ``DistinctOnTest`` is structured as ``fails_if``
        # this requirement — i.e. it expects to xfail on PG-style
        # dialects. Without declaring this ``open()`` the test runs and
        # fails because the PG-only deprecation warning correctly isn't
        # emitted.
        return exclusions.open()
