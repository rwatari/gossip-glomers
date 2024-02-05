Solutions for Fly.io Gossip Glomers

Written for Python 3.11

Unzip maelstrom in the top level folder

Make sure to add `src` to python path

Add shebang and make file executable:
```
# in script:
#!/usr/bin/env -S PYENV_VERSION=gossip_glomers python

chmod +x src/{script}.py
```
Run a test:
```
./maelstrom/maelstrom test -w echo --bin src/echo.py --time-limit 5
```