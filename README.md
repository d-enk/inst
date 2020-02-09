# shared-dict
[![Build Status](https://travis-ci.com/d-enk/shared-dict.svg?branch=master)](https://travis-ci.com/d-enk/shared-dict)
[![codecov.io](https://codecov.io/github/d-enk/shared-dict.svg?branch=master)](https://codecov.io/github/d-enk/shared-dict.svg?branch=master)

with this package you can create dictionary where for each nameSpace own ids for keys 
(string -> uint32 / uint32 -> string)

the dictionary is stored on the etcd server
when adding a new key, async loading will occur on all clients that work with this nameSpace
