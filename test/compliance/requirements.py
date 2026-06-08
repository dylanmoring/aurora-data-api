"""
SQLAlchemy compliance suite Requirements declaration for the Aurora
Data API postgres dialect.

The exclusions below are genuine Aurora Data API limitations — things
real Postgres can do that the Data API HTTPS surface can't:

* ``generate_subscripts(int2vector, ...)`` returns ``ER_UNDEF_FUNC``.
  This sinks every SQLAlchemy reflection query that touches
  ``pg_index.indkey`` — primary keys, unique constraints, indexes,
  foreign keys, comments, table introspection, etc.
* Result sets containing internal pg ``CHAR`` columns
  (``pg_class.relkind``, ``pg_attribute.attstorage``, etc.) fail with
  ``UnsupportedResultException``. Same blast radius — any reflection
  query that selects from ``pg_catalog`` columns of internal ``char``
  type.

Net effect: reflection is broadly unusable through the Data API. We
close the reflection requirements wholesale rather than chase
individual symptoms — until/unless we add a custom inspector that
swaps the failing PG queries for ones the Data API can run.

Everything else inherits ``SuiteRequirements`` defaults so real
dialect / driver bugs the suite catches remain visible.
"""
from sqlalchemy.testing import exclusions
from sqlalchemy.testing.requirements import SuiteRequirements


class Requirements(SuiteRequirements):
    # Reflection — all paths touch pg_index/pg_catalog queries the Data
    # API can't run (int2vector arg, CHAR result columns).

    @property
    def index_reflection(self):
        return exclusions.closed()

    @property
    def reflect_indexes_with_ascdesc(self):
        return exclusions.closed()

    @property
    def reflect_indexes_with_ascdesc_as_expression(self):
        return exclusions.closed()

    @property
    def reflect_indexes_with_expressions(self):
        return exclusions.closed()

    @property
    def reflect_tables_no_columns(self):
        return exclusions.closed()

    @property
    def unique_constraint_reflection(self):
        return exclusions.closed()

    @property
    def primary_key_constraint_reflection(self):
        return exclusions.closed()

    @property
    def foreign_key_constraint_reflection(self):
        return exclusions.closed()

    @property
    def foreign_key_constraint_name_reflection(self):
        return exclusions.closed()

    @property
    def comment_reflection(self):
        return exclusions.closed()

    @property
    def comment_reflection_full_unicode(self):
        return exclusions.closed()

    @property
    def view_column_reflection(self):
        return exclusions.closed()

    @property
    def view_reflection(self):
        return exclusions.closed()

    @property
    def schema_reflection(self):
        return exclusions.closed()

    @property
    def table_reflection(self):
        return exclusions.closed()

    @property
    def temp_table_reflection(self):
        return exclusions.closed()

    @property
    def temp_table_reflect_indexes(self):
        return exclusions.closed()

    @property
    def temporary_views(self):
        return exclusions.closed()

    @property
    def cross_schema_fk_reflection(self):
        return exclusions.closed()

    @property
    def views(self):
        # View reflection (``get_view_names``, ``get_view_definition``,
        # ``test_get_columns`` over views) issues PG-catalog queries the
        # Data API can't run — same ``CHAR`` result / ``int2vector``
        # surface as the constraint-reflection bucket.
        return exclusions.closed()

    @property
    def column_collation_reflection(self):
        return exclusions.closed()

    @property
    def reflects_pk_names(self):
        return exclusions.closed()

    @property
    def foreign_keys_reflect_as_index(self):
        return exclusions.closed()

    @property
    def unique_constraints_reflect_as_index(self):
        return exclusions.closed()

    @property
    def unique_index_reflect_as_unique_constraints(self):
        return exclusions.closed()
