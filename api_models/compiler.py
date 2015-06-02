import sys
import collections

from django.db.models.sql.where import AND, OR
from django.db.utils import DatabaseError
from django.utils.tree import Node

from functools import wraps

from djangotoolbox.db.basecompiler import NonrelQuery, NonrelCompiler, \
    NonrelInsertCompiler, NonrelUpdateCompiler, NonrelDeleteCompiler


OPERATORS_MAP = {
    'exact': '=',
    'gt': '>',
    'gte': '>=',
    'lt': '<',
    'lte': '<=',
    'in': 'IN',
    'isnull': lambda lookup_type, value: ('=' if value else '!=', None),

    #'startswith': lambda lookup_type, value: ...,
    #'range': lambda lookup_type, value: ...,
    #'year': lambda lookup_type, value: ...,
}

NEGATION_MAP = {
    'exact': '!=',
    'gt': '<=',
    'gte': '<',
    'lt': '>=',
    'lte': '>',
    'in': 'NOTIN',
    'isnull': lambda lookup_type, value: ('!=' if value else '=', None),

    #'startswith': lambda lookup_type, value: ...,
    #'range': lambda lookup_type, value: ...,
    #'year': lambda lookup_type, value: ...,
}

def safe_call(func):
    @wraps(func)
    def _func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except DatabaseError, e:
            raise DatabaseError, DatabaseError(*tuple(e)), sys.exc_info()[2]
    return _func

class BackendQuery(NonrelQuery):
    def __init__(self, compiler, fields):
        super(BackendQuery, self).__init__(compiler, fields)
        self.db_query = self.connection.db_connection.query(self.query.model)

    # This is needed for debugging
    def __repr__(self):
        return '<BackendQuery: ' + str(self.query.model) + '>'

    @safe_call
    def fetch(self, low_mark, high_mark):
        # TODO: run your low-level query here
        if high_mark is None:
            # Infinite fetching
            results = self.db_query.fetch(low_mark)
        elif high_mark > low_mark:
            # Range fetching
            results = self.db_query.fetch(low_mark, high_mark - low_mark)
        else:
            results = ()

        for entity in results:
            yield entity

    @safe_call
    def count(self, limit=None):
        # TODO: implement this
        return self.db_query.count(limit)

    @safe_call
    def delete(self):
        self.db_query.delete()

    @safe_call
    def update(self, fields, values):
        self.db_query.update(fields, values)

    @safe_call
    def order_by(self, ordering):
        if isinstance(ordering, collections.Iterable):
            for order in ordering:
                column = order[0].name
                direction = order[1]
                self.db_query.add_ordering(column, direction)

    @safe_call
    def add_filter(self, column, lookup_type, negated, value):
        if column.primary_key:
            column = '_id'

        if negated:
            try:
                op = NEGATION_MAP[lookup_type]
            except KeyError:
                raise DatabaseError("Lookup type %r can't be negated" % lookup_type)
        else:
            try:
                op = OPERATORS_MAP[lookup_type]
            except KeyError:
                raise DatabaseError("Lookup type %r isn't supported" % lookup_type)

        # Handle special-case lookup types
        if callable(op):
            op, value = op(lookup_type, value)

        self.db_query.filter(column, op, value)

    def add_filters(self, filters):
        if filters.negated:
            self._negated = not self._negated

        if not self._negated and filters.connector != AND:
            raise DatabaseError("Only AND filters are supported.")

        # Remove unneeded children from the tree.
        children = self._get_children(filters.children)

        if self._negated and filters.connector != OR and len(children) > 1:
            raise DatabaseError("When negating a whole filter subgroup "
                                "(e.g. a Q object) the subgroup filters must "
                                "be connected via OR, so the non-relational "
                                "backend can convert them like this: "
                                "'not (a OR b) => (not a) AND (not b)'.")

        # Recursively call the method for internal tree nodes, add a
        # filter for each leaf.
        for child in children:
            if isinstance(child, Node):
                self.add_filters(child)
                continue
            field, lookup_type, value = child.lhs.source, child.lookup_name, child.rhs
            self.add_filter(field, lookup_type, self._negated, value)

        if filters.negated:
            self._negated = not self._negated


class SQLCompiler(NonrelCompiler):
    query_class = BackendQuery

    # This gets called for each field type when you fetch() an entity.
    # db_type is the string that you used in the DatabaseCreation mapping
    def convert_value_from_db(self, db_type, value):
        # Handle list types
        if isinstance(value, (list, tuple)) and len(value) and \
                db_type.startswith('ListField:'):
            db_sub_type = db_type.split(':', 1)[1]
            value = [self.convert_value_from_db(db_sub_type, subvalue)
                     for subvalue in value]
        elif isinstance(value, str):
            # Always retrieve strings as unicode
            value = value.decode('utf-8')
        return value

    # This gets called for each field type when you insert() an entity.
    # db_type is the string that you used in the DatabaseCreation mapping
    def convert_value_for_db(self, db_type, value):
        # TODO: implement this

        if isinstance(value, unicode):
            value = unicode(value)
        elif isinstance(value, str):
            # Always store strings as unicode
            value = value.decode('utf-8')
        elif isinstance(value, (list, tuple)) and len(value) and \
                db_type.startswith('ListField:'):
            db_sub_type = db_type.split(':', 1)[1]
            value = [self.convert_value_for_db(db_sub_type, subvalue)
                     for subvalue in value]
        return value

# This handles inserts and updates of individual entities
class SQLInsertCompiler(NonrelInsertCompiler, SQLCompiler):
    @safe_call
    def insert(self, data, return_id=False):
        pk_column = self.query.get_meta().pk.column
        if pk_column in data:
            data['_id'] = data[pk_column]
            del data[pk_column]

        pk = self.connection.db_connection.insert(self.query.model._meta.model_name, data)
        return pk


class SQLUpdateCompiler(NonrelUpdateCompiler, SQLCompiler):
    @safe_call
    def update(self, values):
        pass


class SQLDeleteCompiler(NonrelDeleteCompiler, SQLCompiler):
    pass