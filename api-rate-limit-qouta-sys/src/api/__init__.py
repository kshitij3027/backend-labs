# Package marker only. The HTTP surface is split one router per file (health, protected, admin,
# dashboard) and each is included explicitly by `src.main.create_app`, so nothing is re-exported
# here — an aggregating __init__ would make the import graph depend on file order.
