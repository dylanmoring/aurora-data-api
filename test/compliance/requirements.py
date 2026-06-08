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
        return exclusions.open()

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

    # NOTE: temp_table_names / temp_table_comment_reflection stay closed.
    # Aurora Data API is stateless HTTPS — each call gets a fresh
    # session, so PG temp tables (relpersistence='t') created in one
    # call aren't visible from another. Opening these requirements
    # cascades into ~50 reflection failures.

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
