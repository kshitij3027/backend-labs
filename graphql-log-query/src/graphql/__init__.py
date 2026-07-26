"""The GraphQL surface: types, inputs, enums, context, resolvers and the assembled schema.

.. rubric:: THIS PACKAGE IS NAMED ``graphql`` AND SO IS SOMEBODY ELSE'S

``graphql-core`` — the reference GraphQL implementation Strawberry executes on top of — installs a
top-level package also called ``graphql``. This package is ``src.graphql``. The two coexist only
because Python 3 has no implicit relative imports: inside ``src/graphql/query.py``, the statement
``from graphql import GraphQLError`` resolves against ``sys.path`` and finds **graphql-core**, not
this package, because ``src`` is the package root and ``src.graphql`` is only reachable through it.

That is a guarantee about *absolute* imports, so every import in this package is absolute and
fully qualified — ``from src.graphql.types import LogEntry`` for our own modules,
``from graphql import GraphQLError`` for graphql-core. Two rules follow, and breaking either one
produces an import error that reads like a missing dependency rather than like a name collision:

1. **Never write a relative import here** (``from .types import LogEntry``). It works, but it makes
   the two ``graphql`` packages look interchangeable to a reader, and the next person copies the
   style into a module that then genuinely cannot tell which one it meant.
2. **Never put ``src/`` on ``sys.path``.** The images set ``PYTHONPATH=/app`` and import ``src.*``
   from the repository root precisely so that ``import graphql`` has exactly one meaning. Adding
   ``src`` itself to the path would make ``import graphql`` resolve to *this* package and break
   Strawberry from the inside — the failure surfaces as ``ImportError: cannot import name
   'GraphQLError'``, which sends the reader looking at the wrong library.

.. rubric:: Module map

* :mod:`src.graphql.enums` — ``LogLevel``, pinned to :data:`src.generators.LOG_LEVELS`.
* :mod:`src.graphql.types` — ``LogEntry`` plus the cursor-connection view of it.
* :mod:`src.graphql.inputs` — ``LogFilterInput`` and its mapping onto C2's ``LogQuery``.
* :mod:`src.graphql.cursor` — opaque keyset cursor encode/decode (pure, stdlib only).
* :mod:`src.graphql.context` — the request context and the router's ``context_getter``.
* :mod:`src.graphql.query` — the ``Query`` root type.
* :mod:`src.graphql.schema` — the assembled :class:`strawberry.Schema`, exported as ``schema``.
"""
