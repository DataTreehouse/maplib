"""pytest plugin for running the maplib_benches suite on an open-source maplib build.

Features of maplib Enterprise Edition (hybrid and disk storage, SHACL validation, some
serialization) raise ``PanicException: not implemented: Contact Data Treehouse to try!`` in an
open-source build. Only the workload benchmarks of maplib_benches skip those cases themselves, so
this plugin turns that error into a skip ("needs maplib Enterprise Edition") where it is raised,
in a test or in a fixture, as if the code had called pytest.skip. A fixture that is shared by
several tests then skips each of them. On an Enterprise Edition build the error never occurs and
nothing is skipped.

Loaded by run_maplib_benches.py with ``-p maplib_benches_plugin``.
"""

import pytest

EE_MARKER = "Contact Data Treehouse"
EE_REASON = "needs maplib Enterprise Edition"


def _is_ee_only(e: BaseException) -> bool:
    # PanicException derives from BaseException, not Exception.
    return EE_MARKER in str(e)


@pytest.hookimpl(wrapper=True)
def pytest_fixture_setup(fixturedef, request):
    try:
        return (yield)
    except BaseException as e:
        if not _is_ee_only(e):
            raise
        try:
            pytest.skip(EE_REASON)
        except pytest.skip.Exception as skip:
            # pytest caches the outcome of a fixture's setup only for Exceptions, and a Rust
            # panic is a BaseException. Without a cached outcome, the next test using a shared
            # fixture would set it up again with its finalizers still registered, and fail on
            # an assertion inside pytest. The skip is cached as pytest caches a fixture's own
            # pytest.skip, so every test using the fixture is skipped.
            fixturedef.cached_result = (
                None, fixturedef.cache_key(request), (skip, skip.__traceback__)
            )  # fmt: skip
            raise


@pytest.hookimpl(wrapper=True)
def pytest_pyfunc_call(pyfuncitem):
    try:
        return (yield)
    except BaseException as e:
        if _is_ee_only(e):
            pytest.skip(EE_REASON)
        raise
