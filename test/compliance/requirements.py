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
    def check_constraint_reflection(self):
        # PG stores CHECK constraints in pg_constraint and SA's
        # ``get_check_constraints`` reflects them. Both Aurora PG and
        # the Data API path support this end-to-end.
        return exclusions.open()

    @property
    def identity_columns(self):
        # PG 10+ supports ``GENERATED ... AS IDENTITY``; Aurora PG 16.6
        # of course does.
        return exclusions.open()

    @property
    def identity_columns_standard(self):
        return exclusions.open()

    @property
    def regexp_match(self):
        # PG's ``~`` / ``~*`` operators.
        return exclusions.open()

    @property
    def uuid_data_type(self):
        # PG has a native ``uuid`` column type.
        return exclusions.open()

    @property
    def materialized_views(self):
        # PG ``CREATE MATERIALIZED VIEW``.
        return exclusions.open()

    @property
    def index_reflects_included_columns(self):
        # PG 11+ ``CREATE INDEX ... INCLUDE (col)``.
        return exclusions.open()

    @property
    def reflect_indexes_with_expressions(self):
        # PG supports expression indexes; SA's pg-side reflection
        # returns the expression as the column entry.
        return exclusions.open()

    # ---- PG-supported expression / DDL features ----

    @property
    def supports_bitwise_and(self):
        return exclusions.open()

    @property
    def supports_bitwise_or(self):
        return exclusions.open()

    @property
    def supports_bitwise_xor(self):
        return exclusions.open()

    @property
    def supports_bitwise_not(self):
        return exclusions.open()

    @property
    def supports_bitwise_shift(self):
        # PG defines ``int4 << int4`` but not ``int4 << int8``. The Data
        # API serialises every Python ``int`` as ``longValue`` (= bigint
        # at the PG level), so ``column << :param`` lands as
        # ``integer << bigint`` and PG raises
        # ``operator does not exist: integer << bigint``. ``and/or/xor/not``
        # don't have the int4/int8 split, so those stay open above.
        return exclusions.closed()

    @property
    def datetime_literals(self):
        # PG supports ``DATE 'YYYY-MM-DD'``, ``TIMESTAMP '...'`` etc.
        return exclusions.open()

    @property
    def server_defaults(self):
        # PG supports column-level DEFAULT and SA reflects via
        # ``pg_attrdef``.
        return exclusions.open()

    @property
    def view_column_reflection(self):
        # PG reflects view columns via ``pg_get_viewdef`` and
        # ``information_schema``.
        return exclusions.open()

    @property
    def view_reflection(self):
        return exclusions.open()

    @property
    def views(self):
        return exclusions.open()

    @property
    def inline_check_constraint_reflection(self):
        return exclusions.open()

    @property
    def table_ddl_if_exists(self):
        # PG ``CREATE TABLE IF NOT EXISTS`` / ``DROP TABLE IF EXISTS``.
        return exclusions.open()

    @property
    def index_ddl_if_exists(self):
        # PG ``CREATE INDEX IF NOT EXISTS`` / ``DROP INDEX IF EXISTS``.
        return exclusions.open()

    @property
    def indexes_check_column_order(self):
        # PG ``pg_index.indkey`` preserves column order; SA's reflection
        # round-trips the order correctly.
        return exclusions.open()

    @property
    def fetch_first(self):
        # PG ``FETCH FIRST n ROWS ONLY``.
        return exclusions.open()

    @property
    def fetch_ties(self):
        # PG 13+ ``FETCH FIRST n ROWS WITH TIES``.
        return exclusions.open()

    @property
    def fetch_offset_with_options(self):
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

    # ---- Additional PG-supported features the generic baseline closes ----

    @property
    def regexp_replace(self):
        # PG ``regexp_replace(text, pattern, replacement [, flags])``.
        return exclusions.open()

    @property
    def tuple_in(self):
        # PG ``(a, b) IN ((1, 2), (3, 4))``.
        return exclusions.open()

    @property
    def tuple_in_w_empty(self):
        # PG handles empty tuple IN via SA's compile rewrite.
        return exclusions.open()

    # Aurora Data API is stateless HTTPS — each call gets a fresh
    # session, so PG temp tables (relpersistence='t') created in one
    # call aren't visible from the next. The SuiteRequirements default
    # opens ``temp_table_reflection``; close it for us so the
    # temp-table reflection tests skip rather than fail with
    # ``NoSuchTableError: user_tmp_main``.
    @property
    def temp_table_reflection(self):
        return exclusions.closed()

    @property
    def reflects_pk_names(self):
        # PG's pg_constraint reflects PK constraint names verbatim;
        # SA's get_pk_constraint returns them. The base default is
        # closed() (generic baseline), so the suite's
        # ``test_get_pk_constraint`` wraps the assertion in
        # ``requires.reflects_pk_names.fail_if()`` -- when the assertion
        # passes anyway (we DO reflect names), exclusions raises
        # ``Unexpected success for 'block' (marked as skip)``.
        return exclusions.open()

    @property
    def cross_schema_fk_reflection(self):
        # PG allows FK referencing other schemas; SA reflects via
        # ``pg_constraint.confrelid``.
        return exclusions.open()

    # NOTE: column_collation_reflection / order_by_collation stay closed.
    # Aurora PG's default collation handling apparently differs from
    # what the suite expects (or requires named collations to be
    # available in our database) — opening them gives 1-2 failures.

    @property
    def indexes_with_expressions(self):
        # PG ``CREATE INDEX ON tab ((lower(col)))`` — expression indexes.
        return exclusions.open()

    # NOTE: infinity_floats left closed — PG supports Inf, but the
    # Data API ``doubleValue`` field can't carry IEEE Inf in JSON
    # (it serialises to ``Infinity`` which isn't valid JSON), so the
    # value round-trips broken.

    @property
    def precision_numerics_many_significant_digits(self):
        # PG ``NUMERIC`` supports arbitrary precision.
        return exclusions.open()

    @property
    def precision_numerics_retains_significant_digits(self):
        return exclusions.open()

    @property
    def schema_create_delete(self):
        # PG ``CREATE SCHEMA`` / ``DROP SCHEMA``.
        return exclusions.open()

    @property
    def fk_constraint_option_reflection_ondelete_restrict(self):
        return exclusions.open()

    @property
    def fk_constraint_option_reflection_ondelete_noaction(self):
        return exclusions.open()

    @property
    def fk_constraint_option_reflection_onupdate_restrict(self):
        return exclusions.open()

    @property
    def fk_constraint_option_reflection_ondelete_default(self):
        return exclusions.open()

    @property
    def fk_constraint_option_reflection_ondelete_set_default(self):
        return exclusions.open()

    # ---- PG-supported requirements left closed in the base
    # SuiteRequirements; opening these exposes 200+ tests that the PG
    # dialect compiles correctly and that the Data API can carry
    # (modulo driver-level gaps surfaced by these runs) ----

    @property
    def json_type(self):
        # PG has native ``json`` and ``jsonb``. The PG dialect compiles
        # ``Column(JSON)`` to ``json``; whether the Data API wire format
        # round-trips it is what these 99 tests exercise.
        return exclusions.open()

    @property
    def ctes(self):
        # PG supports WITH RECURSIVE.
        return exclusions.open()

    @property
    def ctes_with_update_delete(self):
        return exclusions.open()

    @property
    def ctes_on_dml(self):
        return exclusions.open()

    @property
    def datetime_timezone(self):
        # PG ``TIMESTAMP WITH TIME ZONE`` / ``TIME WITH TIME ZONE``.
        return exclusions.open()

    @property
    def timestamp_microseconds(self):
        # PG TIMESTAMP carries microseconds.
        return exclusions.open()

    @property
    def unicode_ddl(self):
        # PG handles unicode identifiers natively, but the Data API
        # rejects any named parameter outside ``[A-Za-z0-9_]`` with
        # ``ValidationException: Named parameter syntax is invalid,
        # input: <name>``. SA binds reflection lookups by the
        # identifier itself, so a column named ``mäil`` produces a
        # bind name with non-ASCII chars that the service refuses.
        return exclusions.closed()

    @property
    def window_functions(self):
        # PG supports OVER() / PARTITION BY etc.
        return exclusions.open()

    @property
    def computed_columns(self):
        # PG 12+ supports ``GENERATED ALWAYS AS (...) STORED``. Aurora PG
        # 16.6 has them.
        return exclusions.open()

    @property
    def computed_columns_reflect_persisted(self):
        return exclusions.open()

    @property
    def computed_columns_default_persisted(self):
        return exclusions.open()

    @property
    def computed_columns_stored(self):
        return exclusions.open()

    @property
    def values_expression(self):
        # PG ``VALUES (...) AS t(col1, col2)``.
        return exclusions.open()

    @property
    def array_type(self):
        # PG native ARRAY. Binding now works for both single-dim and
        # multi-dim arrays (driver serialises lists as the Data API's
        # ``arrayValue`` with nested ``arrayValues`` for multi-dim).
        # But the Data API service itself raises
        # ``UnsupportedResultException: The result contains a
        # multidimensional array`` when a SELECT returns one, so the
        # round-trip is dead at the wire format. SA's compliance
        # ``ArrayTest`` uses multi-dim columns in 2 of 3 tests, so
        # opening this gives 1 pass + 2 fails rather than 3 clean
        # skips. Keep closed until either the Data API grows multi-dim
        # support or we ship a PG-array-aware test subclass.
        return exclusions.closed()

    @property
    def empty_inserts(self):
        # PG ``INSERT INTO t DEFAULT VALUES``.
        return exclusions.open()

    @property
    def datetime_implicit_bound(self):
        # ``SELECT literal(<tz_aware_datetime>)`` -- the test sends
        # ``self.data`` as a bind with no explicit CAST. The Data API's
        # typeHint enum has no TIMESTAMPTZ (only TIMESTAMP), so PG
        # serves the result column as ``timestamp`` not ``timestamptz``;
        # by the time it lands in convert_value the tzinfo is gone on
        # the wire and we have nothing to attach UTC to. Real columns
        # of ``TIMESTAMP WITH TIME ZONE`` still round-trip correctly --
        # the bug only affects ``literal()`` selects.
        return exclusions.closed()
