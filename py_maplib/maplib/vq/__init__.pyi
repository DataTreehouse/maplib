from typing import Optional, List, Type, Tuple, Dict, Literal as LiteralType

from maplib import Variable, Literal, IRI

class Expression:
    """
    A SPARQL expression in a virtualized query. Each variant is a subclass, e.g. Expression.And.
    Fields that do not apply to the variant are None.
    """
    Greater: Type["PyExpression__Greater"]
    Less: Type["PyExpression__Less"]
    GreaterOrEqual: Type["PyExpression__GreaterOrEqual"]
    LessOrEqual: Type["PyExpression__LessOrEqual"]
    Equal: Type["PyExpression__Equal"]
    And: Type["PyExpression__And"]
    Or: Type["PyExpression__Or"]
    Not: Type["PyExpression__Not"]
    Bound: Type["PyExpression__Bound"]
    If: Type["PyExpression__If"]
    Variable: Type["PyExpression__Variable"]
    IRI: Type["PyExpression__IRI"]
    Literal: Type["PyExpression__Literal"]
    FunctionCall: Type["PyExpression__FunctionCall"]
    Divide: Type["PyExpression__Divide"]
    Add: Type["PyExpression__Add"]
    Subtract: Type["PyExpression__Subtract"]
    Multiply: Type["PyExpression__Multiply"]
    In: Type["PyExpression__In"]
    Coalesce: Type["PyExpression__Coalesce"]

    left: Optional[Expression]
    middle: Optional[Expression]
    right: Optional[Expression]
    variable: Optional[Variable]
    literal: Optional[Literal]
    function: Optional[str]
    arguments: Optional[List[Expression]]
    expression: Optional[Expression]
    expressions: Optional[List[Expression]]

    def expression_type(self) -> LiteralType["Greater", "Less", "GreaterOrEqual", "LessOrEqual", "Equal", "And", "Or", "Not", "Bound", "If", "Variable", "IRI", "Literal", "FunctionCall", "Divide", "Add", "Subtract", "Multiply", "In", "Coalesce"]:
        """
        The name of the expression variant (workaround for pyo3 issue with matching on enums).
        """


class PyExpression__Greater(Expression):
    left: Expression
    right: Expression


class PyExpression__Less(Expression):
    left: Expression
    right: Expression


class PyExpression__GreaterOrEqual(Expression):
    left: Expression
    right: Expression


class PyExpression__LessOrEqual(Expression):
    left: Expression
    right: Expression


class PyExpression__Equal(Expression):
    left: Expression
    right: Expression


class PyExpression__And(Expression):
    left: Expression
    right: Expression


class PyExpression__Or(Expression):
    left: Expression
    right: Expression


class PyExpression__Not(Expression):
    expression: Expression


class PyExpression__Bound(Expression):
    variable: Variable


class PyExpression__If(Expression):
    left: Expression
    middle: Expression
    right: Expression


class PyExpression__Variable(Expression):
    variable: Variable


class PyExpression__IRI(Expression):
    iri: IRI


class PyExpression__Literal(Expression):
    literal: Literal


class PyExpression__FunctionCall(Expression):
    function: str
    arguments: List[Expression]


class PyExpression__Divide(Expression):
    left: Expression
    right: Expression


class PyExpression__Add(Expression):
    left: Expression
    right: Expression


class PyExpression__Subtract(Expression):
    left: Expression
    right: Expression


class PyExpression__Multiply(Expression):
    left: Expression
    right: Expression


class PyExpression__In(Expression):
    expression: Expression
    expressions: List[Expression]


class PyExpression__Coalesce(Expression):
    expressions: List[Expression]


class XSDDuration:
    """
    An xsd:duration value.
    """
    years: int
    months: int
    days: int
    hours: int
    minutes: int
    seconds: Tuple[int, int]


class OrderExpression:
    """
    An expression with a sort direction, used in Ordered virtualized queries.
    """
    expression: Expression
    ascending: bool


class AggregateExpression:
    """
    An aggregate (e.g. sum, count, group_concat) in a Grouped virtualized query.
    """
    name: str
    expression: Optional[Expression]
    separator: Optional[str]


class VirtualizedQuery:
    """
    A query to be executed by a virtualized database. Each variant is a subclass, e.g. VirtualizedQuery.Basic.
    Fields that do not apply to the variant are None.
    """
    Basic: Type["PyVirtualizedQuery__Basic"]
    Filtered: Type["PyVirtualizedQuery__Filtered"]
    Grouped: Type["PyVirtualizedQuery__Grouped"]
    ExpressionAs: Type["PyVirtualizedQuery__ExpressionAs"]
    InnerJoin: Type["PyVirtualizedQuery__InnerJoin"]
    Ordered: Type["PyVirtualizedQuery__Ordered"]
    Sliced: Type["PyVirtualizedQuery__Sliced"]

    identifier_name: Optional[str]
    column_mapping: Optional[Dict[str, str]]
    resource: Optional[str]
    ids: Optional[List[str]]
    grouping_column_name: Optional[str]
    id_grouping_tuples: Optional[List[Tuple[str, int]]]
    filter: Optional[Expression]
    query: Optional[VirtualizedQuery]
    queries: Optional[List[VirtualizedQuery]]
    by: Optional[List[Variable]]
    ordering: Optional[List[OrderExpression]]
    aggregations: Optional[List[Tuple[Variable, AggregateExpression]]]
    variable: Optional[Variable]
    expression: Optional[Expression]
    limit: Optional[int]
    offset: Optional[int]

    def type_name(self) -> LiteralType["Basic", "Filtered", "Grouped", "ExpressionAs", "InnerJoin", "Ordered", "Sliced"]:
        """
        The name of the query variant.
        """


class PyVirtualizedQuery__Basic(VirtualizedQuery):
    """
    Basic Virtualized Query
    """
    identifier_name: str
    column_mapping: Dict[str, str]
    resource: str
    ids: List[str]
    grouping_column_name: Optional[str]
    id_grouping_tuples: Optional[List[Tuple[str, int]]]


class PyVirtualizedQuery__Filtered(VirtualizedQuery):
    """
    Filtered Virtualized Query
    """
    query: VirtualizedQuery
    filter: Expression


class PyVirtualizedQuery__Grouped(VirtualizedQuery):
    """
    Grouped Virtualized Query
    """
    query: VirtualizedQuery
    by: List[Variable]
    aggregations: List[Tuple[Variable, AggregateExpression]]


class PyVirtualizedQuery__ExpressionAs(VirtualizedQuery):
    """
    ExpressionAs Virtualized Query
    """
    query: VirtualizedQuery
    variable: Variable
    expression: Expression


class PyVirtualizedQuery__InnerJoin(VirtualizedQuery):
    """
    Inner join of Virtualized Queries
    """
    queries: List[VirtualizedQuery]


class PyVirtualizedQuery__Ordered(VirtualizedQuery):
    """
    Ordered Virtualized Query
    """
    query: VirtualizedQuery
    ordering: List[OrderExpression]


class PyVirtualizedQuery__Sliced(VirtualizedQuery):
    """
    Sliced (offset/limit) Virtualized Query
    """
    query: VirtualizedQuery
    offset: int
    limit: Optional[int]
