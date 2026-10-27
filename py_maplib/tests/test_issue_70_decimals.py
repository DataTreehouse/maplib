from decimal import Decimal

import polars as pl
import pytest
from maplib.maplib import MaplibException

from maplib import Model


def test_issue_70_good_error_message():
    template = """
    @prefix ex: <http://example.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    
    ex:DecimalTemplate [xsd:decimal ?value] :: {
        ottr:Triple(ex:subject, ex:amount, ?value)
    } .
    """

    data = pl.DataFrame(
        {"value": [Decimal("12.3400"), Decimal("798.0")]},
        schema={"value": pl.Decimal(38, 12)},
    )

    model = Model()
    model.add_template(template)
    with pytest.raises(MaplibException) as e:
        model.map("http://example.org/DecimalTemplate", data)

    assert "decimals should have scale" in str(e.value)


def test_issue_70_mapping_ok():
    template = """
    @prefix ex: <http://example.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    
    ex:DecimalTemplate [xsd:decimal ?value] :: {
        ottr:Triple(ex:subject, ex:amount, ?value)
    } .
    """

    data = pl.DataFrame(
        {"value": [Decimal("12.3400"), Decimal("798.0")]},
        schema={"value": pl.Decimal(38, 16)},
    )

    model = Model()
    model.add_template(template)
    model.map("http://example.org/DecimalTemplate", data)
    ttl = model.writes(format="turtle")
    assert '''"12.34"^^xsd:decimal''' in ttl
    assert '''"798"^^xsd:decimal''' in ttl
