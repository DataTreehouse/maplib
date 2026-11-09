from maplib import Model


def test_issue_73():
    model = Model()
    model.query("SELECT ?o WHERE { <https://a> !<https://p> ?o }")
