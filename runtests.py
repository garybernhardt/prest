#!/usr/bin/env python

import sys

import nose


if __name__ == '__main__':
    nose_args = sys.argv + [r'-s',
                            r'-m',
                            r'((?:^|[b_.-])(:?[Tt]est|[Dd]escribe|When|should|it_))',
                            r'--with-doctest',
                            r'--doctest-extension=']
    nose.run(argv=nose_args)

