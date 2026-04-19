from typing import Optional, List, Type, Tuple, Dict, Literal as LiteralType

from chrontext import Variable, Literal, IRI


class Expression:
    And:Type["PyExpression__And"]
    Greater:Type["PyExpression__Greater"]
    Less:Type["PyExpression__Less"]
    left:Optional[Expression]
    right:Optional[Expression]
    Variable:Type["PyExpression__Variable"]
    variable:Optional[Variable]
    IRI:Type["PyExpression__IRI"]
    Literal:Type["PyExpression__Literal"]
    literal:Optional[Literal]
    FunctionCall:["PyExpression__FunctionCall"]
    function: Optional[str]
    arguments: Optional[List[Expression]]

    def expression_type(self) -> str:
        """
        Workaround for pyo3 issue with constructing enums.
        :return:
        """

class XSDDuration:
    years: int
    months: int
    days: int
    hours: int
    minutes: int
    seconds: (int,int)
    """

    """

class PyExpression__And(Expression):
    left:Expression
    right:Expression

class PyExpression__Variable(Expression):
    variable:Variable

class PyExpression__IRI(Expression):
    iri:IRI

class PyExpression__Literal(Expression):
    literal:Literal

class PyExpression__Greater(Expression):
    left:Expression
    right:Expression

class PyExpression__Less(Expression):
    left:Expression
    right:Expression

class PyExpression__FunctionCall(Expression):
    function: str
    arguments: List[Expression]


class AggregateExpression:
    name: str
    expression: Expression
    separator: Optional[str]
    """

    """


class VirtualizedQuery:
    Filtered:Type["PyVirtualizedQuery__Filtered"]
    filter: Optional[Expression]
    query: Optional[VirtualizedQuery]
    Basic:Type["PyVirtualizedQuery__Basic"]
    identifier_name: Optional[str]
    column_mapping: Optional[Dict[str, str]]
    resource: Optional[str]
    ids: Optional[List[str]]
    grouping_column_name: Optional[str]
    id_grouping_tuples: Optional[List[Tuple[str, int]]]
    Grouped:Type["PyVirtualizedQuery__Grouped"]
    by: List[Variable]
    aggregations: Optional[List[Tuple[Variable, AggregateExpression]]]
    ExpressionAs:Type["PyVirtualizedQuery__ExpressionAs"]
    variable: Optional[Variable]
    expression: Optional[Expression]

    def type_name(self) -> LiteralType["Filtered", "Basic"]:
        """

        :return:
        """


class PyVirtualizedQuery__Basic:
    identifier_name: str
    column_mapping: Dict[str, str]
    resource: str
    ids: List[str]
    grouping_column_name: Optional[str]
    id_grouping_tuples: Optional[List[Tuple[str, int]]]

    """
    Basic Virtualized Query
    """

class PyVirtualizedQuery__Filtered:
    query: VirtualizedQuery
    filter: Expression

    """
    Filtered Virtualized Query
    """


class PyVirtualizedQuery__Grouped:
    by: List[Variable]
    aggregations: List[Tuple[Variable, AggregateExpression]]
    """
    Grouped Virtualized Query
    """


class PyVirtualizedQuery__ExpressionAs:
    query: VirtualizedQuery
    variable: Variable
    expression: Expression
    """
    ExpressionAs Virtualized Query    
    """
