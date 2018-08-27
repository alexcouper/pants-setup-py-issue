A minimal repro repo for the problem described
[here](https://github.com/pantsbuild/pants/issues/6401)


To see the issue, run:

```
./pants setup-py python/my-library::
```

And then from a python3 virtualenv try to install the generated package.

```
(python3) $ pip install dist/Mylibrary-0.1.0.tar.gz
Processing ./dist/Mylibrary-0.1.0.tar.gz
    Complete output from command python setup.py egg_info:
    error in Mylibrary setup command: 'install_requires' must be a string or list of strings containing valid project/version requirement specifiers; 'int' object is not iterable
```
