# cdb
Pure Go implementation of D. J. Bernstein's cdb constant database library: http://cr.yp.to/cdb.html

## TODO
* [x] map[string]string <-> cdb的[]byte
* [x] 传入cdb的[]byte，直接按照key搜索value
* [x] 传入一个cdb文件路径，搜索文件中的key对应的value
* [x] 提供将重写cdb文件的某个key的value的接口, 使用os.Rename原子替换

## 读取速度对比
````
# cdb
goos: windows
goarch: amd64
pkg: github.com/orestonce/cdb
cpu: Intel(R) Core(TM) i7-4790K CPU @ 4.00GHz
BenchmarkCdb_GetValue
BenchmarkCdb_GetValue-8   	 4948628	       238.6 ns/op

# map[string]string
goos: windows
goarch: amd64
pkg: github.com/orestonce/cdb
cpu: Intel(R) Core(TM) i7-4790K CPU @ 4.00GHz
BenchmarkCdb_GetValue2
BenchmarkCdb_GetValue2-8   	 8469826	       164.3 ns/op
````