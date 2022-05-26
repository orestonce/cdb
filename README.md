# cdb
Pure Go implementation of D. J. Bernstein's cdb constant database library: http://cr.yp.to/cdb.html

## TODO
[ ] map[string]string <-> cdb的[]byte
[ ] 传入cdb的[]byte，直接按照key搜索value/value列表
[ ] 传入一个cdb文件路径，搜索文件中的key对应的value/value列表
[ ] 提供将map[string]string直接写入cdb文件的接口，使用os.Rename原子替换
