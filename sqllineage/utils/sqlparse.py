import itertools
from typing import Iterator, List, Union

from sqlparse.engine.grouping import _group, group_functions
from sqlparse.sql import (
    Case,
    Comparison,
    Function,
    Identifier,
    Parenthesis,
    TokenList,
    Where,
)
from sqlparse.tokens import DML, Keyword, Name, Wildcard
from sqlparse.utils import recurse

from sqllineage.utils.entities import SubQueryTuple


def get_innermost_parenthesis(token: Parenthesis):
    # in case of subquery in nested parenthesis, find the innermost one first
    while True:
        idx, sub_paren = token.token_next_by(i=Parenthesis)
        if sub_paren is not None and idx == 1:
            token = sub_paren
        else:
            break
    return token


def is_subquery(token: TokenList) -> bool:
    flag = False
    if isinstance(token, Parenthesis):
        token = get_innermost_parenthesis(token)
        # check if innermost parenthesis contains SELECT
        _, sub_token = token.token_next_by(m=(DML, "SELECT"))
        if sub_token is not None:
            flag = True
    return flag


def get_subquery_parentheses(
    token: Union[Identifier, Function, Where]
) -> List[SubQueryTuple]:
    """
    Retrieve subquery list
    the returned list is either empty when no subquery parsed or list of [parenthesis, alias] tuple
    """
    subquery = []
    as_idx, as_ = token.token_next_by(m=(Keyword, "AS"))
    sublist = list(token.get_sublists())
    if as_ is not None and len(sublist) == 1:
        # CTE: tbl AS (SELECT 1)
        target = sublist[0]
    else:
        if isinstance(token, Function):
            # CTE without AS: tbl (SELECT 1)
            target = token.tokens[-1]
        elif isinstance(token, Where):
            # WHERE col1 IN (SELECT max(col1) FROM tab2)
            target = token
        else:
            # normal subquery: (SELECT 1) tbl
            target = token.token_first(skip_cm=True)
    if isinstance(target, (Case, Where)):
        # CASE WHEN (SELECT count(*) from tab1) > 0 THEN (SELECT count(*) FROM tab1) ELSE -1
        for tk in target.get_sublists():
            if isinstance(tk, Comparison):
                if is_subquery(tk.left):
                    subquery.append(SubQueryTuple(tk.left, tk.left.get_real_name()))
                if is_subquery(tk.right):
                    subquery.append(SubQueryTuple(tk.right, tk.right.get_real_name()))
            elif is_subquery(tk):
                subquery.append(SubQueryTuple(tk, token.get_real_name()))
    if is_subquery(target):
        subquery = [
            SubQueryTuple(get_innermost_parenthesis(target), token.get_real_name())
        ]
    return subquery


def get_parameters(token: Function):
    """
    This is a replacement for sqlparse.sql.Function.get_parameters(), which produces problematic result for:
        if(col1 = 'foo' AND col2 = 'bar', 1, 0)
    This implementation ignores the constant parameter as we don't need them for column lineage for now
    """
    if isinstance(token, Window):
        return token.get_parameters()
    else:
        return [
            tk for tk in token.tokens[-1].tokens if tk.is_group or tk.ttype == Wildcard
        ]


class Window(Function):
    """window function + OVER keyword + window defn"""

    def get_parameters(self) -> Iterator[TokenList]:
        return itertools.chain(
            get_parameters(self.get_window_function()),
            self.get_window_defn().get_sublists(),
        )

    def get_window_function(self) -> Function:
        return self.tokens[0]

    def get_window_defn(self) -> Parenthesis:
        return self.tokens[-1]


@recurse(Window)
def group_window(tlist):
    def match(token):
        return token.is_keyword and token.normalized == "OVER"

    def valid_prev(token):
        return isinstance(token, Function)

    def valid_next(token):
        return isinstance(token, Parenthesis)

    def post(tlist, pidx, tidx, nidx):
        return pidx, nidx

    _group(
        tlist, Window, match, valid_prev, valid_next, post, extend=False, recurse=False
    )


@recurse(Function)
def group_functions_as(tlist):
    """
    This function is to allow parsing columns in functions with CTAS syntax like:
        CREATE TABLE tbl1 AS SELECT coalesce(t1.col1, 0) AS col1 FROM t1;
    The code is mostly taken from original sqlparse.sql.group_functions by
    replacing with one condition.

    This is no longer needed if this PR get merged:
    https://github.com/andialbrecht/sqlparse/pull/662
    """
    has_create = False
    has_table = False
    has_as = False
    for tmp_token in tlist.tokens:
        if tmp_token.value == "CREATE":
            has_create = True
        if tmp_token.value == "TABLE":
            has_table = True
        if tmp_token.value == "AS":
            has_as = True
    if not has_create or not has_table or not has_as:
        return

    tidx, token = tlist.token_next_by(t=Name)
    while token:
        nidx, next_ = tlist.token_next(tidx)
        if isinstance(next_, Parenthesis):
            tlist.group_tokens(Function, tidx, nidx)
        tidx, token = tlist.token_next_by(t=Name, idx=tidx)


def group_function_with_window(tlist):
    group_functions(tlist)
    group_functions_as(tlist)
    group_window(tlist)
