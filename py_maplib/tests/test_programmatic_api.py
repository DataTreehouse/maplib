from maplib import Parameter, Variable, Argument, Template, Prefix, a, Instance, XSD, RDFType


def test_template_parameters_accepts_variables():
    v1 = Variable("v1")
    v2 = Variable("v2")
    ottr = Prefix("ottr", "http://ns.ottr.xyz/0.4/")
    ex = Prefix("ex", "https://github.com/DataTreehouse/maplib/example#")
    Template(ex.suf("my_template"),
                 parameters=[v1, v2],
                 instances=[Instance(ottr.suf("Triple"), arguments=[v1, a(), v2])]
    )
def test_template_object_inspection():
    v1 = Variable("v1")
    v2 = Variable("v2")
    ottr = Prefix("ottr", "http://ns.ottr.xyz/0.4/")
    ex = Prefix("ex", "https://github.com/DataTreehouse/maplib/example#")
    t = Template(ex.suf("my_template"),
                 parameters=[v1, v2],
                 instances=[Instance(ottr.suf("Triple"), arguments=[v1, a(), v2])]
                 )

    params = t.parameters
    for p in params:
        assert p.rdf_type == None
        assert not p.optional
        assert p.allow_blank
        assert p.variable.name in {"v1", "v2"}
    for p in params:
        p.rdf_type = RDFType.IRI()
        p.allow_blank = False
        p.variable = Variable("v3")
        p.optional = True

    t.parameters = params
    for p in t.parameters:
        assert p.optional
        assert not p.allow_blank
        assert p.variable.name in {"v3"}

