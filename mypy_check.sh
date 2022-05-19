python3 -m mypy \
    --disallow-any-unimported \
    --disallow-any-generics \
    --disallow-untyped-defs \
    --disallow-incomplete-defs \
    --install-types \
    --disallow-untyped-calls \
    py_queue/ main.py