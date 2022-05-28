# cdb
Pure Go implementation of D. J. Bernstein's cdb constant database library: http://cr.yp.to/cdb.html

## TODO
* [x] map[string]string <-> cdb的[]byte
* [x] 传入cdb的[]byte，直接按照key搜索value
* [x] 传入一个cdb文件路径，搜索文件中的key对应的value
* [x] 提供将重写cdb文件的某个key的value的接口, 使用os.Rename原子替换
