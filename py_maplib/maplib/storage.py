"""Raw bytes in object storage and local files.

The functions take the same URLs as ``Model.read``, ``Model.write`` and ``Model.serialize``:
Google Cloud Storage (``gs://bucket/key``), Azure Blob Storage (``az://container/key``,
``abfss://...``, ``https://<account>.blob.core.windows.net/...``), Amazon S3 and compatible
stores (``s3://bucket/key``), plain HTTP(S) and ``file://`` URLs, as well as local paths.
Credentials are found the same way, from the environment as the cloud SDKs do (e.g.
``GOOGLE_APPLICATION_CREDENTIALS`` or the metadata server, ``AZURE_STORAGE_*`` or managed
identities, ``AWS_*``), and can be given or overridden with ``storage_options``, using the
option names of the object_store crate (and of Polars' ``storage_options``), e.g.
``{"azure_storage_account_name": ..., "azure_storage_account_key": ...}``.

Every object has a version token, returned by ``put``, ``get``, ``head`` and ``list``. It is
an opaque string: the object's ETag, followed by the store's object version where it has one
(the generation on Google Cloud Storage, which makes conditional updates against it). Pass it
back as ``if_match`` to replace an object only if nobody changed it since::

    from maplib import storage

    data, token = storage.get("gs://bucket/model/manifest.json")
    try:
        storage.put("gs://bucket/model/manifest.json", new_data, if_match=token)
    except storage.PreconditionFailed:
        ...  # someone else published first: read again, merge and retry

Conditional puts on each backend:

- S3, Google Cloud Storage, Azure Blob Storage: made by the store. For S3-compatible stores
  this needs support for ``If-Match`` / ``If-None-Match`` on puts (AWS S3, R2 and MinIO have
  it); ``{"aws_conditional_put": "disabled"}`` turns conditional puts off, and they then raise
  ``NotSupported``.
- Local files and ``file://``: ``if_none_match`` is atomic (the file is published with a hard
  link). ``if_match`` is emulated: the version is compared and the file replaced (atomically,
  with a rename) while an exclusive lock is held on a lock file in the system temporary
  folder. That serializes the writers on one machine, but not writers on several machines
  sharing a network file system. The version token of a local file is made from its inode,
  modification time and size.
- Plain HTTP(S) (WebDAV) servers: no conditional puts, ``NotSupported`` is raised.

Objects are written with a single put, which most stores limit to a few GB (5 GB on S3 and
Google Cloud Storage). Use ``Model.write`` for large triple files. The calls block, and
release the GIL while they wait for the store.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Union

from .maplib import (
    MaplibException,
    _storage_delete,
    _storage_get,
    _storage_head,
    _storage_list,
    _storage_put,
)

__all__ = [
    "put",
    "get",
    "head",
    "exists",
    "delete",
    "list",
    "ObjectInfo",
    "StorageError",
    "NotFound",
    "PreconditionFailed",
    "AlreadyExists",
    "NotSupported",
]

class StorageError(MaplibException):
    """An error reaching or using object storage, the base of the other storage errors."""


class NotFound(StorageError, FileNotFoundError):
    """There is no object at the URL."""


class PreconditionFailed(StorageError):
    """A conditional put failed: the object is no longer at the version given as
    ``if_match`` (it was changed or deleted)."""


class AlreadyExists(PreconditionFailed, FileExistsError):
    """A put with ``if_none_match=True`` failed because there is already an object."""


class NotSupported(StorageError):
    """The store does not support the operation, e.g. conditional puts to plain HTTP."""


@dataclass(frozen=True)
class ObjectInfo:
    """The metadata of an object, from ``head`` or ``list``."""

    url: str
    """The URL (or local path) of the object."""
    name: str
    """For ``list``: the path of the object relative to the listed prefix, with ``/``
    separators. For ``head``: the last segment of the path."""
    size: int
    """The size in bytes."""
    last_modified: datetime
    """When the object was last written, in UTC."""
    version: Optional[str]
    """The version token, for ``put(..., if_match=...)``. None only for stores with neither
    ETags nor object versions (some plain HTTP servers)."""


def _info(url: str, t: Tuple[str, int, int, Optional[str]]) -> ObjectInfo:
    name, size, micros, version = t
    modified = datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc)
    return ObjectInfo(url=url, name=name, size=size, last_modified=modified, version=version)


def put(
    url: str,
    data: Union[bytes, bytearray, memoryview],
    if_match: Optional[str] = None,
    if_none_match: bool = False,
    storage_options: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """Stores ``data`` as the object at ``url``, replacing it if it exists. The object appears
    whole, never partly written.

    Usage:

    >>> token = storage.put("az://container/model/manifest.json", b"{}", if_none_match=True)

    :param url: The URL of the object, or a local path. Missing local folders are created.
    :param data: The bytes to store.
    :param if_match: A version token from ``put``, ``get``, ``head`` or ``list``: the object is
        only replaced if it is still at that version, otherwise ``PreconditionFailed`` is raised
        (also if the object was deleted).
    :param if_none_match: Only store the object if there is none yet, otherwise
        ``AlreadyExists`` (a ``PreconditionFailed``) is raised.
    :param storage_options: Options for reaching the store, e.g. credentials, see the module
        documentation.
    :return: The version token of the new object. None only for stores with neither ETags nor
        object versions (some plain HTTP servers).
    """
    return _storage_put(url, bytes(data) if isinstance(data, memoryview) else data, if_match,
                        if_none_match, storage_options)


def get(url: str, storage_options: Optional[Dict[str, str]] = None) -> Tuple[bytes, Optional[str]]:
    """The object at ``url`` and its version token, for a later ``put(..., if_match=token)``.
    Raises ``NotFound`` if there is no object.

    Usage:

    >>> data, token = storage.get("gs://bucket/model/manifest.json")

    :param url: The URL of the object, or a local path.
    :param storage_options: Options for reaching the store, see the module documentation.
    :return: The bytes of the object and its version token.
    """
    return _storage_get(url, storage_options)


def head(url: str, storage_options: Optional[Dict[str, str]] = None) -> Optional[ObjectInfo]:
    """The metadata of the object at ``url`` (size, time and version token), None if there is
    no object.

    :param url: The URL of the object, or a local path.
    :param storage_options: Options for reaching the store, see the module documentation.
    """
    t = _storage_head(url, storage_options)
    return None if t is None else _info(url, t)


def exists(url: str, storage_options: Optional[Dict[str, str]] = None) -> bool:
    """Whether there is an object at ``url``.

    :param url: The URL of the object, or a local path.
    :param storage_options: Options for reaching the store, see the module documentation.
    """
    return _storage_head(url, storage_options) is not None


def delete(url: str, storage_options: Optional[Dict[str, str]] = None) -> None:
    """Deletes the object at ``url``. Deleting an object that does not exist is not an error.

    :param url: The URL of the object, or a local path.
    :param storage_options: Options for reaching the store, see the module documentation.
    """
    _storage_delete(url, storage_options)


def list(prefix_url: str, storage_options: Optional[Dict[str, str]] = None) -> List[ObjectInfo]:
    """The objects under ``prefix_url``, recursively, sorted by name. The prefix is a "folder":
    ``gs://bucket/model`` lists ``gs://bucket/model/manifest.json`` but not
    ``gs://bucket/model2/x``. An empty list if there are none.

    Usage:

    >>> for o in storage.list("gs://bucket/model"):
    ...     print(o.name, o.size, o.last_modified)

    :param prefix_url: The URL of the folder, or a local folder.
    :param storage_options: Options for reaching the store, see the module documentation.
    :return: The objects, with names relative to the prefix.
    """
    base = prefix_url.rstrip("/")
    return [_info(f"{base}/{t[0]}", t) for t in _storage_list(prefix_url, storage_options)]
