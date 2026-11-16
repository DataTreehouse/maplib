# OWL 2 RL test cases

Used by `py_maplib/tests/test_w3c_owl2_rl.py`, see `LICENSE.md`.

* `profile-RL.rdf` is an unmodified copy of https://www.w3.org/2009/11/owl-test/profile-RL.rdf: the
  test cases of the OWL 2 Test Case Repository whose ontologies are in the OWL 2 RL profile.
* `rdfbased-sem.rdf` holds the test cases of https://www.w3.org/2009/11/owl-test/all.rdf whose
  identifier starts with `rdfbased-sem-`: the core test suite for the OWL 2 RL/RDF rules (OWL 2
  Full semantics) of the OWL Working Group. The file is all.rdf with the other `test:TestCase`
  elements removed; the test cases themselves are unmodified. It was made with:

```python
import re
t = open("all.rdf").read()
head = t[: t.index("<test:TestCase ")]
blocks = re.findall(r"\t<test:TestCase .*?\n\t</test:TestCase>\n", t, flags=re.S)
keep = [b for b in blocks if re.search(r"<test:identifier[^>]*>rdfbased-sem-", b)]
open("rdfbased-sem.rdf", "w").write(head + "".join(keep) + "\n</rdf:RDF>\n")
```
