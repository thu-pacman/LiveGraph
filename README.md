# LiveGraph

## Description
This is the open-source implementation for LiveGraph:

LiveGraph: a transactional graph storage system with purely sequential adjacency list scans.  
Xiaowei Zhu, Guanyu Feng, Marco Serafini, Xiaosong Ma, Jiping Yu, Lei Xie, Ashraf Aboulnaga, and Wenguang Chen.   
Proc. VLDB Endow. 13, 7 (March 2020), 1020–1034. DOI:https://doi.org/10.14778/3384345.3384351

## Dependency
 - [CMake](https://gitlab.kitware.com/cmake/cmake)
 - [TBB](https://github.com/oneapi-src/oneTBB) 
 - OpenMP and C++17
 - AVX-2

## API

### class `livegraph::Graph` 

 Members                        |
--------------------------------|
`public inline  `[`Graph`](#d5/d1f/classlivegraph_1_1Graph_1a9eaef12fb2758edf6fba991e694fc78b)`(std::string block_path,std::string wal_path,size_t _max_block_size,vertex_t _max_vertex_id)` |
`public inline vertex_t `[`get_max_vertex_id`](#d5/d1f/classlivegraph_1_1Graph_1ae32a89731e2bb72261e46256994f50f6)`() const` |
`public timestamp_t `[`compact`](#d5/d1f/classlivegraph_1_1Graph_1a0317d09d47305d76012beeaecbb14110)`(timestamp_t read_epoch_id)` |
`public `[`Transaction`](#de/d80/classlivegraph_1_1Transaction)` `[`begin_transaction`](#d5/d1f/classlivegraph_1_1Graph_1a0dc3e75cc1a569c887ed3644aa58c25d)`()` |
`public `[`Transaction`](#de/d80/classlivegraph_1_1Transaction)` `[`begin_read_only_transaction`](#d5/d1f/classlivegraph_1_1Graph_1a859327efb8164edb84b4cbad088ed129)`()` |
`public `[`Transaction`](#de/d80/classlivegraph_1_1Transaction)` `[`begin_batch_loader`](#d5/d1f/classlivegraph_1_1Graph_1a16e393872779448ceec388242e7840b9)`()` |

### class `livegraph::Transaction` 

 Members                        |
--------------------------------|
`public inline timestamp_t `[`get_read_epoch_id`](#de/d80/classlivegraph_1_1Transaction_1a4cfbb234332801135186637581c703bb)`() const` |
`public vertex_t `[`new_vertex`](#de/d80/classlivegraph_1_1Transaction_1ab56133b8ed4004eab383dc4d4a1ea52c)`(bool use_recycled_vertex)` |
`public void `[`put_vertex`](#de/d80/classlivegraph_1_1Transaction_1adae09fe945b64a309973657b55317138)`(vertex_t vertex_id,std::string_view data)` |
`public bool `[`del_vertex`](#de/d80/classlivegraph_1_1Transaction_1a83b4e9b86c13890c8d7019778149a545)`(vertex_t vertex_id,bool recycle)` |
`public void `[`put_edge`](#de/d80/classlivegraph_1_1Transaction_1aecf88f276f3768a5b0d01253b64ae21b)`(vertex_t src,label_t label,vertex_t dst,std::string_view edge_data,bool force_insert)` |
`public bool `[`del_edge`](#de/d80/classlivegraph_1_1Transaction_1a5b22fa1a3b0a1e33874bfbd99755c597)`(vertex_t src,label_t label,vertex_t dst)` |
`public std::string_view `[`get_vertex`](#de/d80/classlivegraph_1_1Transaction_1a1cd41d8828d4ad5096ded9f340f0ae24)`(vertex_t vertex_id)` |
`public std::string_view `[`get_edge`](#de/d80/classlivegraph_1_1Transaction_1ad445ab1a95bfb81829ef3a9c1e221573)`(vertex_t src,label_t label,vertex_t dst)` |
`public `[`EdgeIterator`](#d4/d30/classlivegraph_1_1EdgeIterator)` `[`get_edges`](#de/d80/classlivegraph_1_1Transaction_1aaec4015431a6afd9c710b3fca427555d)`(vertex_t src,label_t label,bool reverse)` |
`public timestamp_t `[`commit`](#de/d80/classlivegraph_1_1Transaction_1a6a3b303f8a44d7757d3b9cf8c79f23a0)`(bool wait_visable)` |
`public void `[`abort`](#de/d80/classlivegraph_1_1Transaction_1a11db8852227f0bf3bf103483032711f5)`()` |

### class `livegraph::EdgeIterator` 

 Members                        |
--------------------------------|
`public inline bool `[`valid`](#d4/d30/classlivegraph_1_1EdgeIterator_1a016d207a852844281c3e4d33afb9bde5)`() const` |
`public inline void `[`next`](#d4/d30/classlivegraph_1_1EdgeIterator_1aa3af5e0a36848a3da43791744808a534)`()` |
`public inline vertex_t `[`dst_id`](#d4/d30/classlivegraph_1_1EdgeIterator_1a3a545a34b93fcb7a10da270d62195e05)`() const` |
`public inline std::string_view `[`edge_data`](#d4/d30/classlivegraph_1_1EdgeIterator_1aaa5ea498742f1f244d52ef596f652f57)`() const` |

### class `livegraph::Transaction::RollbackExcept` 

```cpp
class livegraph::Transaction::RollbackExcept : public runtime_error
```  

 Members                        |
--------------------------------|
`public inline  `[`RollbackExcept`](#df/d6f/classlivegraph_1_1Transaction_1_1RollbackExcept_1a5495fa81b025448e10b3f754b2065bd6)`(const std::string & what_arg)` |
`public inline  `[`RollbackExcept`](#df/d6f/classlivegraph_1_1Transaction_1_1RollbackExcept_1a470f073f1e4a421ee91b17aa218f296d)`(const char * what_arg)` |
