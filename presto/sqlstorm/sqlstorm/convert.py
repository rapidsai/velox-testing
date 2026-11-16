# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

"""Converts SQLStorm PostgreSQL queries to Presto SQL.

sqlglot does the dialect translation. The rewrites in this module patch the cases
where sqlglot's Presto output is not accepted by Presto. Only rewrites that preserve
the query's semantics are applied; queries that need anything approximate are
reported as unsupported instead.
"""

import re
from dataclasses import dataclass

import sqlglot
from sqlglot import exp

# Columns of the StackOverflow tables (v1.0/stackoverflow/schema.sql). Every table has a
# non-null integer primary key named Id.
TABLE_COLUMNS = {
    "posthistorytypes": {"id", "name"},
    "linktypes": {"id", "name"},
    "posttypes": {"id", "name"},
    "closereasontypes": {"id", "name"},
    "votetypes": {"id", "name"},
    "users": {
        "id", "reputation", "creationdate", "displayname", "lastaccessdate", "websiteurl", "location",
        "aboutme", "views", "upvotes", "downvotes", "profileimageurl", "accountid",
    },
    "badges": {"id", "userid", "name", "date", "class", "tagbased"},
    "posts": {
        "id", "posttypeid", "acceptedanswerid", "parentid", "creationdate", "score", "viewcount", "body",
        "owneruserid", "ownerdisplayname", "lasteditoruserid", "lasteditordisplayname", "lasteditdate",
        "lastactivitydate", "title", "tags", "answercount", "commentcount", "favoritecount", "closeddate",
        "communityowneddate", "contentlicense",
    },
    "comments": {"id", "postid", "score", "text", "creationdate", "userdisplayname", "userid", "contentlicense"},
    "posthistory": {
        "id", "posthistorytypeid", "postid", "revisionguid", "creationdate", "userid", "userdisplayname",
        "comment", "text", "contentlicense",
    },
    "postlinks": {"id", "creationdate", "postid", "relatedpostid", "linktypeid"},
    "tags": {"id", "tagname", "count", "excerptpostid", "wikipostid", "ismoderatoronly", "isrequired"},
    "votes": {"id", "postid", "votetypeid", "userid", "creationdate", "bountyamount"},
}  # fmt: skip
BASE_TABLES = set(TABLE_COLUMNS)
TIMESTAMP_COLUMNS = {
    "creationdate", "lastaccessdate", "date", "lasteditdate", "lastactivitydate", "closeddate",
    "communityowneddate",
}  # fmt: skip
TIMESTAMP_LITERAL = re.compile(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2}(\.\d+)?)?)?$")


@dataclass
class Conversion:
    ok: bool
    sql: str | None = None
    reason: str | None = None


class Unsupported(Exception):
    pass


def convert(postgres_sql):
    try:
        statements = [s for s in sqlglot.parse(postgres_sql, read="postgres") if s is not None]
    except sqlglot.errors.ParseError as e:
        return Conversion(False, reason=f"parse: {first_line(e)}")
    if len(statements) != 1:
        return Conversion(False, reason=f"expected 1 statement, found {len(statements)}")

    try:
        tree = rewrite(statements[0])
        presto_sql = tree.sql(dialect="presto", unsupported_level=sqlglot.ErrorLevel.RAISE)
    except Unsupported as e:
        return Conversion(False, reason=f"unsupported: {e}")
    except sqlglot.errors.UnsupportedError as e:
        return Conversion(False, reason=f"sqlglot: {first_line(e)}")
    return Conversion(True, sql=presto_sql)


def rewrite(tree):
    reject_unsupported(tree)
    for rule in (
        rewrite_string_to_array,
        rewrite_lateral_unnest,
        rewrite_select_list_unnest,
        rewrite_join_subquery_conditions,
        rewrite_any_array,
        rewrite_group_by_output_alias,
        rewrite_epoch_of_difference,
        rewrite_count_of_table_alias,
        rewrite_fetch_to_limit,
        rewrite_empty_array_literals,
        rewrite_window_filter,
        cast_timestamp_literals,
        add_functionally_dependent_group_by,
    ):
        tree = rule(tree)
    return tree


def reject_unsupported(tree):
    for with_ in tree.find_all(exp.With):
        if with_.args.get("recursive"):
            raise Unsupported("recursive CTE")
    for func in tree.find_all(exp.Anonymous):
        if func.name.lower() == "age":
            raise Unsupported("AGE returns a calendar interval")


def rewrite_string_to_array(tree):
    """string_to_array(s, d) -> IF(s = '', ARRAY[], SPLIT(s, d)); Postgres returns {} for ''."""
    for node in list(tree.find_all(exp.StringToArray)):
        delimiter = node.expression
        if node.args.get("null") or not (isinstance(delimiter, exp.Literal) and delimiter.is_string and delimiter.this):
            raise Unsupported("string_to_array without a non-empty literal delimiter")
        value = node.this
        node.replace(
            exp.If(
                this=exp.EQ(this=value.copy(), expression=exp.Literal.string("")),
                true=exp.Array(expressions=[]),
                false=exp.Split(this=value.copy(), expression=delimiter.copy()),
            )
        )
    return tree


def iter_ancestors(node, stop):
    node = node.parent
    while node is not None and node is not stop:
        yield node
        node = node.parent


def rewrite_select_list_unnest(tree):
    """SELECT unnest(a) AS x ... -> SELECT _srf.x ... CROSS JOIN UNNEST(a) AS _srf(x).

    A set-returning function in the select list emits one row per array element and no
    row for an empty or NULL array, which is what CROSS JOIN UNNEST does. Several of them
    are zipped with NULL padding (Postgres 10+), as is a multi-argument UNNEST. Window
    functions and aggregates in the argument are evaluated before the expansion in
    Postgres, so those selects are left alone.
    """
    for select in list(tree.find_all(exp.Select)):
        explodes = [e for e in select.find_all(exp.Explode) if e.parent_select is select]
        if not explodes:
            continue
        if any(w.parent_select is select for w in select.find_all(exp.Window)):
            raise Unsupported("set-returning function with window functions")
        for e in explodes:
            if e.this.find(exp.AggFunc):
                raise Unsupported("set-returning function over an aggregate")
            if any(isinstance(a, (exp.AggFunc, exp.Where, exp.Having, exp.Explode)) for a in iter_ancestors(e, select)):
                raise Unsupported("set-returning function inside an aggregate, predicate or another one")

        distinct = []
        for e in explodes:
            if e not in distinct:
                distinct.append(e)
        columns = []
        for i, e in enumerate(distinct):
            projection = e.parent
            name = projection.alias if isinstance(projection, exp.Alias) and projection.this is e else ""
            if not name or name.lower() in {c.lower() for c in columns}:
                name = f"unnest_value{i}"
            columns.append(name)
        arrays = [e.this.copy() for e in distinct]
        for e in explodes:
            e.replace(exp.column(columns[distinct.index(e)], table="_srf"))
        unnest = exp.Unnest(
            expressions=arrays,
            alias=exp.TableAlias(this=exp.to_identifier("_srf"), columns=[exp.to_identifier(c) for c in columns]),
        )
        if select.args.get("from_"):
            select.append("joins", exp.Join(this=unnest, kind="CROSS"))
        else:
            select.set("from_", exp.From(this=unnest))
    return tree


def lateral_unnest_parts(lateral):
    """Returns (array expression, column name) for the lateral unnest shapes, else None."""
    alias = lateral.args.get("alias")
    if alias is None:
        return None
    source = lateral.this if isinstance(lateral, exp.Lateral) else lateral
    if isinstance(source, exp.Unnest) and len(source.expressions) == 1:
        columns = alias.columns
        column = columns[0].name if len(columns) == 1 else alias.name
        return source.expressions[0], column
    if isinstance(source, exp.Subquery) and isinstance(source.this, exp.Select):
        select = source.this
        if len(select.expressions) != 1 or any(select.args.get(k) for k in ("from_", "where", "group", "joins")):
            return None
        projection = select.expressions[0]
        if isinstance(projection, exp.Alias) and isinstance(projection.this, (exp.Explode, exp.Unnest)):
            inner = projection.this
            array = inner.this if isinstance(inner, exp.Explode) else inner.expressions[0]
            return array, projection.alias
    return None


def filter_by_join_condition(array, on, alias, column):
    """FILTER(array, x -> on) with the unnested column replaced by x. A LEFT JOIN keeps the
    matching elements, or one NULL row if none match, which the caller adds."""
    if on.find(exp.Subquery, exp.Select, exp.Exists):
        raise Unsupported("LEFT JOIN LATERAL unnest with a subquery in the join condition")
    condition = on.copy()
    for col in list(condition.find_all(exp.Column)):
        if col.table.lower() == alias.lower() or (not col.table and col.name.lower() == column.lower()):
            if col.name.lower() != column.lower():
                raise Unsupported("LEFT JOIN LATERAL unnest condition on an unknown column")
            col.replace(exp.column("_unnest_x"))
    lambda_ = exp.Lambda(this=condition, expressions=[exp.to_identifier("_unnest_x")])
    return exp.Anonymous(this="FILTER", expressions=[array, lambda_])


def rewrite_lateral_unnest(tree):
    """[LEFT] JOIN [LATERAL] unnest(...) -> CROSS JOIN UNNEST(...); Presto only supports UNNEST on a
    cross join. Functions in FROM are implicitly lateral in Postgres.

    LEFT joins keep a NULL row for an empty or NULL array, matching Postgres.
    """
    for join in list(tree.find_all(exp.Join)):
        lateral = join.this
        if not isinstance(lateral, (exp.Lateral, exp.Unnest)):
            continue
        if isinstance(lateral, exp.Unnest) and (join.kind or "").upper() == "CROSS":
            continue
        parts = lateral_unnest_parts(lateral)
        if parts is None:
            continue
        array, column = parts
        on = join.args.get("on")
        trivially_true = on is None or (isinstance(on, exp.Boolean) and on.this)
        side = (join.side or "").upper()
        array = array.copy()
        if side == "LEFT":
            if not trivially_true:
                array = filter_by_join_condition(array, on, lateral.alias, column)
            array = exp.If(
                this=exp.GT(this=exp.ArraySize(this=array.copy()), expression=exp.Literal.number(0)),
                true=array,
                false=exp.Array(expressions=[exp.Null()]),
            )
        elif side:
            raise Unsupported(f"{side} JOIN LATERAL unnest")
        elif not trivially_true:
            move_to_where(join, [on])

        unnest = exp.Unnest(
            expressions=[array],
            alias=exp.TableAlias(this=exp.to_identifier(lateral.alias), columns=[exp.to_identifier(column)]),
        )
        join.replace(exp.Join(this=unnest, kind="CROSS"))
    return tree


def move_to_where(join, conditions):
    """Moves inner join conditions to the WHERE clause. Equivalent unless a later RIGHT or
    FULL join could make NULL rows that the condition would have removed earlier."""
    select = join.parent
    joins = (select.args.get("joins") or []) if isinstance(select, exp.Select) else []
    if not any(j is join for j in joins) or (join.side or "") or (join.kind or "").upper() not in ("", "INNER", "CROSS"):
        raise Unsupported("join condition cannot be moved to WHERE")
    later = joins[next(i for i, j in enumerate(joins) if j is join) + 1 :]
    if any((j.side or "").upper() in ("RIGHT", "FULL") for j in later):
        raise Unsupported("join condition cannot be moved past a RIGHT or FULL join")
    for condition in conditions:
        select.where(condition.copy(), copy=False)


def rewrite_join_subquery_conditions(tree):
    """Presto rejects subqueries in join conditions; for inner joins they can move to WHERE."""
    for join in list(tree.find_all(exp.Join)):
        on = join.args.get("on")
        if on is None or not on.find(exp.Subquery, exp.Exists, exp.Select):
            continue
        conjuncts = list(on.flatten()) if isinstance(on, exp.And) else [on]
        moved = [c for c in conjuncts if c.find(exp.Subquery, exp.Exists, exp.Select)]
        if (join.side or "").upper():
            continue
        move_to_where(join, moved)
        kept = [c for c in conjuncts if c not in moved]
        if kept:
            join.set("on", exp.and_(*[c.copy() for c in kept]))
        else:
            join.set("on", None)
            join.set("kind", "CROSS")
    return tree


def rewrite_any_array(tree):
    """x = ANY(array) -> CONTAINS(array, x); both are NULL when x is NULL or when x is not found
    and the array contains NULL."""
    for node in list(tree.find_all(exp.Any)):
        target = node.this.unnest()
        if isinstance(target, (exp.Subquery, exp.Select)):
            continue
        comparison = node.parent
        if not (isinstance(comparison, exp.EQ) and comparison.expression is node):
            raise Unsupported("ANY(array) with an operator other than =")
        comparison.replace(exp.Anonymous(this="CONTAINS", expressions=[target.copy(), comparison.this.copy()]))
    return tree


def source_columns(source, tree):
    """Lowercase output column names of a FROM/JOIN source, or None if unknown."""
    if isinstance(source, exp.Table):
        name = source.name.lower()
        for cte in tree.find_all(exp.CTE):
            if cte.alias.lower() == name:
                return select_output_names(cte.this)
        return TABLE_COLUMNS.get(name)
    if isinstance(source, exp.Subquery):
        return select_output_names(source.this)
    if isinstance(source, (exp.Unnest, exp.Lateral)):
        alias = source.args.get("alias")
        if alias is not None and alias.columns:
            return {c.name.lower() for c in alias.columns}
    return None


def select_output_names(query):
    if isinstance(query, exp.Union):
        query = query.left
    if not isinstance(query, exp.Select) or any(isinstance(e, exp.Star) for e in query.expressions):
        return None
    alias = query.parent.args.get("alias") if query.parent is not None else None
    if alias is not None and getattr(alias, "columns", None):
        return {c.name.lower() for c in alias.columns}
    return {e.alias_or_name.lower() for e in query.expressions}


def rewrite_group_by_output_alias(tree):
    """GROUP BY <output alias> -> GROUP BY <aliased expression>.

    Postgres resolves a bare GROUP BY name to an input column first and to an output alias
    only if no input column has that name, so the rewrite applies only when every source's
    columns are known and none of them has the name.
    """
    for select in list(tree.find_all(exp.Select)):
        group = select.args.get("group")
        if not group:
            continue
        aliased = {e.alias.lower(): e.this for e in select.expressions if isinstance(e, exp.Alias)}
        sources = []
        if select.args.get("from_"):
            sources.append(select.args["from_"].this)
        sources.extend(join.this for join in select.args.get("joins") or [])
        input_columns = set()
        for source in sources:
            columns = source_columns(source, tree)
            if columns is None:
                input_columns = None
                break
            input_columns |= columns
        if input_columns is None:
            continue
        for node in list(group.expressions):
            if not isinstance(node, exp.Column) or node.table:
                continue
            name = node.name.lower()
            expression = aliased.get(name)
            if expression is None or name in input_columns or expression.find(exp.AggFunc, exp.Window):
                continue
            node.replace(expression.copy())
    return tree


def rewrite_epoch_of_difference(tree):
    """EXTRACT(EPOCH FROM (a - b)) -> DATE_DIFF('millisecond', b, a) / 1000.0 (the data has ms precision)."""
    for node in list(tree.find_all(exp.Extract)):
        if node.name.upper() != "EPOCH":
            continue
        inner = node.expression.unnest()
        if not isinstance(inner, exp.Sub):
            continue
        diff = exp.Anonymous(
            this="DATE_DIFF",
            expressions=[exp.Literal.string("millisecond"), inner.expression.copy(), inner.this.copy()],
        )
        node.replace(exp.Paren(this=exp.Div(this=diff, expression=exp.Literal.number("1000.0"))))
    return tree


def table_aliases(select):
    """Maps lowercase alias -> lowercase table name for base tables in this SELECT's FROM and JOINs."""
    sources = []
    if select.args.get("from_"):
        sources.append(select.args["from_"].this)
    sources.extend(join.this for join in select.args.get("joins") or [])
    aliases = {}
    for source in sources:
        if isinstance(source, exp.Table) and source.name.lower() in BASE_TABLES:
            aliases[(source.alias or source.name).lower()] = source.name.lower()
    return aliases


def rewrite_count_of_table_alias(tree):
    """COUNT(t) where t is a table alias -> COUNT(t.Id); a row value is NULL only for an unmatched outer join."""
    for count in list(tree.find_all(exp.Count)):
        arg = count.this
        if not isinstance(arg, exp.Column):
            continue
        if isinstance(arg.this, exp.Star) and arg.table:
            alias = arg.table
        elif not arg.table:
            alias = arg.name
        else:
            continue
        select = count.parent_select
        while select is not None:
            if alias.lower() in table_aliases(select):
                arg.replace(exp.column("id", table=alias))
                break
            select = select.parent_select
    return tree


def rewrite_fetch_to_limit(tree):
    for select in tree.find_all(exp.Select):
        fetch = select.args.get("limit")
        if not isinstance(fetch, exp.Fetch):
            continue
        options = fetch.args.get("limit_options")
        if options and (options.args.get("percent") or options.args.get("with_ties")):
            raise Unsupported("FETCH with PERCENT or WITH TIES")
        count = fetch.args.get("count") or exp.Literal.number(1)
        select.set("limit", exp.Limit(expression=count.copy()))
    return tree


def is_timestamp(node):
    """Heuristic for timestamp-typed expressions. Used only to decide whether a date-like
    string literal next to the expression should get an explicit cast."""
    node = node.unnest()
    if isinstance(node, exp.Column):
        name = node.name.lower()
        return name in TIMESTAMP_COLUMNS or "date" in name
    if isinstance(node, (exp.CurrentTimestamp, exp.CurrentDate)):
        return True
    if isinstance(node, exp.Cast):
        return node.to.this in (exp.DataType.Type.TIMESTAMP, exp.DataType.Type.DATE)
    if isinstance(node, (exp.Add, exp.Sub)):
        return is_timestamp(node.this) or is_timestamp(node.expression)
    if isinstance(node, (exp.Min, exp.Max)):
        return is_timestamp(node.this)
    if isinstance(node, exp.Case):
        branches = [i.args.get("true") for i in node.args.get("ifs") or []] + [node.args.get("default")]
        return any(b is not None and is_timestamp(b) for b in branches)
    return False


def is_timestamp_literal(node):
    return isinstance(node, exp.Literal) and node.is_string and TIMESTAMP_LITERAL.match(node.this)


def cast_to_timestamp(literal):
    literal.replace(exp.cast(literal.copy(), exp.DataType.Type.TIMESTAMP))


def cast_timestamp_literals(tree):
    """Casts date-like string literals compared with timestamps, as Postgres does implicitly."""
    for node in list(tree.find_all(exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
        for a, b in ((node.this, node.expression), (node.expression, node.this)):
            if is_timestamp_literal(a) and is_timestamp(b):
                cast_to_timestamp(a)
    for node in list(tree.find_all(exp.Between)):
        if is_timestamp(node.this):
            for bound in (node.args.get("low"), node.args.get("high")):
                if is_timestamp_literal(bound):
                    cast_to_timestamp(bound)
    for node in list(tree.find_all(exp.In)):
        if is_timestamp(node.this):
            for value in node.expressions:
                if is_timestamp_literal(value):
                    cast_to_timestamp(value)
    for node in list(tree.find_all(exp.Coalesce, exp.Nullif)):
        args = [node.this, *node.expressions] if isinstance(node, exp.Coalesce) else [node.this, node.expression]
        # A wrong guess here is a type error in Presto, never a silent change of meaning.
        if any(is_timestamp(arg) or not isinstance(arg.unnest(), exp.Literal) for arg in args):
            for arg in args:
                if is_timestamp_literal(arg):
                    cast_to_timestamp(arg)
    return tree


def rewrite_window_filter(tree):
    """agg(x) FILTER (WHERE c) OVER w -> agg(CASE WHEN c THEN x END) OVER w, for aggregates
    that ignore NULL inputs; Presto supports FILTER only on plain aggregates."""
    for window in list(tree.find_all(exp.Window)):
        node = window.this
        if not isinstance(node, exp.Filter):
            continue
        agg, condition = node.this, node.expression.this
        if not isinstance(agg, (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)):
            raise Unsupported(f"FILTER on window function {agg.key}")
        arg = agg.this
        distinct = isinstance(arg, exp.Distinct)
        values = arg.expressions if distinct else [arg]
        if len(values) != 1:
            raise Unsupported("FILTER on a multi-argument window aggregate")
        value = exp.Literal.number(1) if isinstance(values[0], exp.Star) else values[0]
        case = exp.Case(ifs=[exp.If(this=condition.copy(), true=value.copy())])
        agg = agg.copy()
        agg.set("this", exp.Distinct(expressions=[case]) if distinct else case)
        node.replace(agg)
    return tree


def rewrite_empty_array_literals(tree):
    """COALESCE(array, '{}') -> COALESCE(array, ARRAY[]); '{}' is Postgres' empty array literal."""
    for node in list(tree.find_all(exp.Coalesce)):
        args = [node.this, *node.expressions]
        if not any(isinstance(a.unnest(), (exp.ArrayAgg, exp.Array, exp.Split)) for a in args):
            continue
        for arg in args:
            if isinstance(arg, exp.Literal) and arg.is_string and arg.this == "{}":
                arg.replace(exp.Array(expressions=[]))
    return tree


def needs_grouping(column, select):
    """True if the column is evaluated per group (not inside an aggregate) directly in this SELECT."""
    node = column
    while node.parent is not None and node is not select:
        parent = node.parent
        if isinstance(node, exp.AggFunc) and not (isinstance(parent, exp.Window) and parent.this is node):
            return False
        if isinstance(parent, (exp.Select, exp.Subquery)) and parent is not select:
            return False
        node = parent
    return node is select


def add_functionally_dependent_group_by(tree):
    """Adds columns that Postgres accepts because they depend on a grouped primary key.

    Postgres lets a query select t.col when t.Id is in GROUP BY and t is a base table
    with primary key Id. Adding t.col to GROUP BY leaves the groups unchanged.
    """
    for select in list(tree.find_all(exp.Select)):
        group = select.args.get("group")
        if not group:
            continue
        aliases = table_aliases(select)
        grouped = {(c.table.lower(), c.name.lower()) for c in group.expressions if isinstance(c, exp.Column)}
        keyed = {table for table, name in grouped if name == "id" and table in aliases}
        if len(aliases) == 1 and ("", "id") in grouped:
            keyed.add("")
        if not keyed:
            continue

        output_names = {e.alias_or_name.lower() for e in select.expressions}
        clauses = [*select.expressions, select.args.get("having"), select.args.get("order")]
        for clause in filter(None, clauses):
            for column in clause.find_all(exp.Column):
                key = (column.table.lower(), column.name.lower())
                if not key[0] and key[1] in output_names and not isinstance(column.parent, exp.Alias):
                    continue
                if key[0] in keyed and key not in grouped and needs_grouping(column, select):
                    group.append("expressions", exp.column(column.name, table=column.table or None))
                    grouped.add(key)
    return tree


def first_line(error):
    return str(error).strip().splitlines()[0][:300]
