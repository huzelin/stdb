/*!
 * \file rtree.h
 */
#ifndef STDB_INDEX_RTREE_H_
#define STDB_INDEX_RTREE_H_

#include <math.h>

#include <mutex>
#include <limits>
#include <queue>
#include <sstream>
#include <vector>

#include "stdb/common/basic.h"
#include "stdb/common/exception.h"
#include "stdb/common/rwlock.h"
#include "stdb/common/logging.h"
#include "stdb/common/singleton.h"

namespace stdb {
namespace rtree {

template <int BLOCK_SIZE>
struct BlockPool : public common::Singleton<BlockPool<BLOCK_SIZE>> {
 public:
  char* Allocate() {
    std::lock_guard<std::mutex> lck(mutex_);
    if (blocks_.empty()) {
      auto ptr = reinterpret_cast<char*>(malloc(BLOCK_SIZE));
      return ptr;
    }
    auto ret = blocks_.back();
    blocks_.pop_back();
    return ret;
  }
  void Release(char* ptr) {
    std::lock_guard<std::mutex> lck(mutex_);
    blocks_.push_back(ptr);
  }
  size_t Size() {
    std::lock_guard<std::mutex> lck(mutex_);
    return blocks_.size();
  }

 protected:
  std::vector<char*> blocks_;
  std::mutex mutex_;
};

template <typename DType, int NDIMS>
struct XPoint {
  DType data[NDIMS];
};

template <typename DType, int NDIMS>
struct XRect {
  XPoint<DType, NDIMS> min;
  XPoint<DType, NDIMS> max;
};

template <typename DType, int NDIMS>
static inline bool Intersect(const XRect<DType, NDIMS>& rect1, const XRect<DType, NDIMS>& rect2) {
  for (int i = 0; i < NDIMS; ++i) {
    if (rect1.max.data[i] < rect2.min.data[i]) {
      return false;
    }
    if (rect1.min.data[i] > rect2.max.data[i]) {
      return false;
    }
  }
  return true;
}

template <typename DType, int NDIMS>
static inline bool Intersect(const XRect<DType, NDIMS>& rect, const XPoint<DType, NDIMS>& p) {
  for (int i = 0; i < NDIMS; ++i) {
    if (rect.min.data[i] > p.data[i]) {
      return false;
    }
    if (rect.max.data[i] < p.data[i]) {
      return false;
    }
  }
  return true;
}

template <typename DType, int NDIMS>
static inline std::ostream& operator<<(std::ostream& os, const XPoint<DType, NDIMS>& point) {
  os << "(";
  for (int i = 0; i < NDIMS; ++i) {
    os << point.data[i];
    if (i + 1 != NDIMS) {
      os << " ";
    }
  }
  os << ")";
  return os;
}

template <typename DType, int NDIMS>
static inline std::ostream& operator<<(std::ostream& os, const XRect<DType, NDIMS>& rect) {
  os << "[";
  os << rect.min << ", ";
  os << rect.max;
  os << "]";
  return os;
}

// Distance type between point/rect
enum DistanceType {
  kEuclidean = 0,
};

template <typename DType, int NDIMS>
struct EuclideanDistance {
  typedef XPoint<DType, NDIMS> Point;
  typedef XRect<DType, NDIMS> Rect;
  
  // Distance between point and rect
  // @param rect The rect
  // @param point The point
  // @return Return the minimum distance
  static DType Distance(const Rect& rect, const Point& point) {
    DType dis[NDIMS];
    for (int i = 0; i < NDIMS; ++i) {
      dis[i] = point.data[i] > rect.max.data[i] ? point.data[i] - rect.max.data[i] :
          point.data[i] < rect.min.data[i] ? rect.min.data[i] - point.data[i] : 0;
    }
    DType sum = 0;
    for (int i = 0; i < NDIMS; ++i) {
      sum += dis[i] * dis[i];
    }
    return sqrtf(sum);
  }

  // Distance between point and point
  // @param p1 Point 1
  // @param p2 Point 2
  // @return distance
  static DType Distance(const Point& p1, const Point& p2) {
    DType dis = 0;
    for (int i = 0; i < NDIMS; ++i) {
      dis += (p1.data[i] -p2.data[i]) * (p1.data[i] -p2.data[i]);
    }
    return sqrtf(dis);
  }

  // Distance between r1 and r2
  // @param r1 The rect 1
  // @param r2 The rect 2
  // @return distance
  static DType Distance(const Rect& r1, const Rect& r2) {
    DType dis[NDIMS];
    for (int i = 0; i < NDIMS; ++i) {
      dis[i] = r1.min.data[i] > r2.max.data[i] ? r1.min.data[i] - r2.max.data[i] :
          r1.max.data[i] < r2.min.data[i] ? r2.min.data[i] - r1.max.data[i] : 0;
    }
    DType sum = 0;
    for (int i = 0; i < NDIMS; ++i) {
      sum += dis[i] * dis[i];
    }
    return sqrtf(sum);
  }
};

template <typename DType, int NDIMS, int BLOCK_SIZE, DistanceType distance_type = kEuclidean> 
class RTree {
 public:
  typedef XPoint<DType, NDIMS> Point;
  typedef XRect<DType, NDIMS> Rect;

 protected:
  typedef char* NodePtr;
  
  // RTree node base class.
  class Node {
   protected:
    NodePtr ptr_;
    u32* size_;
    u32* level_;

   public:
    // Distance between point and rect
    // @param rect The rect
    // @param point The point
    // @return Return the minimum distance
    static DType Distance(const Rect& rect, const Point& point) {
      switch (distance_type) {
        case kEuclidean:
        default: {
          return EuclideanDistance<DType, NDIMS>::Distance(rect, point);
        }
      }
    }

    // Distance between point and point
    // @param p1 Point 1
    // @param p2 Point 2
    // @return distance
    static DType Distance(const Point& p1, const Point& p2) {
      switch (distance_type) {
        case kEuclidean:
        default: {
          return EuclideanDistance<DType, NDIMS>::Distance(p1, p2);
        }
      }
    }

    // Distance between r1 and r2
    // @param r1 The rect 1
    // @param r2 The rect 2
    // @return distance
    static DType Distance(const Rect& r1, const Rect& r2) {
      switch (distance_type) {
        case kEuclidean:
        default: {
          return EuclideanDistance<DType, NDIMS>::Distance(r1, r2);
        }
      }
    }

    explicit Node(char* ptr) : ptr_(ptr) {
      size_ = reinterpret_cast<u32*>(ptr);
      level_ = reinterpret_cast<u32*>(ptr + sizeof(u32));
    }
    NodePtr ptr() const { return ptr_; }
    void set_size(u32 size) { *size_ = size; }
    void set_level(u32 level) { *level_ = level; }
    u32 size() const { return *size_; }
    u32 level() const { return *level_; }
  };

  // RTree leaf node
  class LeafNode : public Node {
   protected:
    struct Entry {
      Point point;
      i64 payload;
    };
    Entry* entry_;
    const u32 CAPACITY = (BLOCK_SIZE - 2 * sizeof(u32)) / sizeof(Entry);

   public:
    explicit LeafNode(char* ptr) : Node(ptr) {
      entry_ = reinterpret_cast<Entry*>(ptr + 2 * sizeof(u32));
    }

    // Return the index-th point
    // @param index
    // @return the index-th point
    const Point& point_at(u32 index) {
      return entry_[index].point;
    }
    // Return the index-th point's payload
    // @param index
    // @return the index-th point's payload
    i64 payload_at(u32 index) {
      return entry_[index].payload;
    }

    // Return the debug string
    std::string DebugString() {
      std::stringstream ss;
      ss << " NodePtr=" << (void*)this->ptr() << std::endl;
      ss << " Level=" << this->level() << std::endl;
      ss << " Size=" << this->size() << std::endl;
      ss << " Capacity=" << CAPACITY << std::endl;
      for (u32 i = 0; i < this->size(); ++i) {
        ss << "  " << i << "-th point=" << entry_[i].point << " payload=" << entry_[i].payload;
        if (i + 1 != this->size()) {
          ss << std::endl;
        }
      }
      return ss.str();
    }

    // Insert new point to the leaf, if the leaf is full, insertion fail, return
    // false.
    // @param point To be inserted point
    // @param payload To be inserted point's payload
    // @return Return ture suceess, else failture
    bool Insert(const Point& point, i64 payload) {
      if (this->size() < CAPACITY) {
        entry_[this->size()].point = point;
        entry_[this->size()].payload = payload;
        this->set_size(this->size() + 1);
        return true;
      } else {
        return false;
      }
    }

    // Get the leaf's rect bound
    // @return rect The leaf's rect bound MBR
    Rect GetRect() const {
      Rect rect = { entry_[0].point, entry_[0].point };
      for (u32 i = 0; i < this->size(); ++i) {
        auto& cur_point = entry_[i].point;
        for (u32 j = 0; j < NDIMS; ++j) {
          rect.min.data[j] = rect.min.data[j] < cur_point.data[j] ? rect.min.data[j] : cur_point.data[j];
          rect.max.data[j] = rect.max.data[j] < cur_point.data[j] ? cur_point.data[j] : rect.max.data[j];
        }
      }
      return rect;
    }

    // Split the leaf node
    // @param point To be inserted point
    // @param payload To be inserted point's payload
    // @param new_rect The new node's rect
    // @param new_ptr The new node's ptr
    // @return The old leaf's rect
    Rect Split(const Point& point, i64 payload, Rect* new_rect, NodePtr* new_ptr) {
      DType max_dis = -1;
      u32 l = 0, r = 1;
      for (u32 i = 0; i < this->size(); ++i) {
        for (u32 j = i + 1; j < this->size(); ++j) {
          auto dis = Node::Distance(entry_[i].point, entry_[j].point);
          if (max_dis < dis) {
            max_dis = dis;
            l = i;
            r = j;
          }
        }
      }

      std::vector<u32> l_clusters, r_clusters;
      for (u32 i = 0; i < this->size(); ++i) {
        auto dis_l = Node::Distance(entry_[l].point, entry_[i].point);
        auto dis_r = Node::Distance(entry_[r].point, entry_[i].point);
        if (dis_l < dis_r) {
          l_clusters.push_back(i);
        } else {
          r_clusters.push_back(i);
        }
      }
      if (l_clusters.empty()) {
        auto b = r_clusters.back(); r_clusters.pop_back();
        l_clusters.push_back(b);
      } else if (r_clusters.empty()) {
        auto b = l_clusters.back(); l_clusters.pop_back();
        r_clusters.push_back(b);
      }
      auto dis_l = Node::Distance(entry_[l].point, point);
      auto dis_r = Node::Distance(entry_[r].point, point);

      auto new_leaf_node = LeafNode::MakeNew(0);
      for (auto& index : r_clusters) {
        if (!new_leaf_node.Insert(entry_[index].point, entry_[index].payload)) {
          STDB_THROW("Split insert fail");
        }
      }
      this->set_size(0);
      for (auto& index : l_clusters) {
        if (!this->Insert(entry_[index].point, entry_[index].payload)) {
          STDB_THROW("Split insert fail");
        }
      }

      if (dis_l < dis_r) {
        if (!this->Insert(point, payload)) {
          STDB_THROW("Split insert fail");
        }
      } else {
        if (!new_leaf_node.Insert(point, payload)) {
          STDB_THROW("Split insert fail");
        }
      }
      *new_rect = new_leaf_node.GetRect();
      *new_ptr = new_leaf_node.ptr();

      return GetRect();
    }

    // Make new leaf node.
    static LeafNode MakeNew(u32 level) {
      auto ptr = BlockPool<BLOCK_SIZE>::Get()->Allocate();
      auto node = LeafNode(ptr);
      node.set_level(level);
      node.set_size(0);
      return node;
    }
  };

  // RTree index node
  class IndexNode : public Node {
   protected:
    struct Entry {
      Rect rect;
      NodePtr subnode;
    };
    Entry* entry_;
    const u32 CAPACITY = (BLOCK_SIZE - 2 * sizeof(u32)) / sizeof(Entry);

   public:
    explicit IndexNode(char* ptr) : Node(ptr) {
      entry_ = reinterpret_cast<Entry*>(ptr + 2 * sizeof(u32));
    }

    // Set rect
    // @param index The index
    // @param rect The index's rect
    void set_rect(u32 index, const Rect& rect) {
      entry_[index].rect = rect;
    }
    NodePtr child(u32 index) {
      return entry_[index].subnode;
    }
    const Rect& child_rect(u32 index) {
      return entry_[index].rect;
    }

    // Return debug string
    std::string DebugString() const {
      std::stringstream ss;
      ss << " NodePtr=" << (void*)this->ptr() << std::endl;
      ss << " Level=" << this->level() << std::endl;
      ss << " Size=" << this->size() << std::endl;
      ss << " Capacity=" << CAPACITY << std::endl;
      for (u32 i = 0; i < this->size(); ++i) {
        ss << "  " << i << "-th rect=" << entry_[i].rect << " subnode=" << (void*)entry_[i].subnode;
        if (i + 1 != this->size()) {
          ss << std::endl;
        }
      }
      return ss.str();
    }

    // Get rect
    // @return Return the mbr of index node.
    Rect GetRect() const {
      Rect rect = entry_[0].rect;
      for (u32 i = 1; i < this->size(); ++i) {
        auto& cur_rect = entry_[i].rect;
        for (u32 j = 0; j < NDIMS; ++j) {
          rect.min.data[j] = rect.min.data[j] < cur_rect.min.data[j] ? rect.min.data[j] : cur_rect.min.data[j];
          rect.max.data[j] = rect.max.data[j] < cur_rect.max.data[j] ? cur_rect.max.data[j] : rect.max.data[j];
        }
      }
      return rect;
    }

    // Insert new rect into the node
    // @param rect The rect
    // @param subnode The subnode
    // @return If true, insert success, else failture.
    bool Insert(const Rect& rect, NodePtr subnode) {
      if (this->size() < CAPACITY) {
        entry_[this->size()].rect = rect;
        entry_[this->size()].subnode = subnode;
        this->set_size(this->size() + 1);
        return true;
      } else {
        return false;
      }
    }

    // Split the index node
    // @param rect The new inserted rect
    // @param ptr The new inserted node ptr
    // @param new_rect The splited rect
    // @param new_ptr The splited ptr
    // @return Return the old index node's rect
    Rect Split(const Rect& rect, NodePtr ptr, Rect* new_rect, NodePtr* new_ptr) {
      DType max_dis = -1;
      u32 l = 0, r = 1;
      for (u32 i = 0; i < this->size(); ++i) {
        for (u32 j = i + 1; j < this->size(); ++j) {
          auto dis = Node::Distance(entry_[i].rect, entry_[j].rect);
          if (max_dis < dis) {
            max_dis = dis;
            l = i;
            r = j;
          }
        }
      }

      std::vector<u32> l_clusters, r_clusters;
      for (u32 i = 0; i < this->size(); ++i) {
        auto dis_l = Node::Distance(entry_[l].rect, entry_[i].rect);
        auto dis_r = Node::Distance(entry_[r].rect, entry_[i].rect);
        if (dis_l < dis_r) {
          l_clusters.push_back(i);
        } else {
          r_clusters.push_back(i);
        }
      }

      if (l_clusters.empty()) {
        auto b = r_clusters.back(); r_clusters.pop_back();
        l_clusters.push_back(b);
      } else if (r_clusters.empty()) {
        auto b = l_clusters.back(); l_clusters.pop_back();
        r_clusters.push_back(b);
      }
      auto dis_l = Node::Distance(entry_[l].rect, rect);
      auto dis_r = Node::Distance(entry_[r].rect, rect);

      auto new_index_node = IndexNode::MakeNew(this->level());
      for (auto& index : r_clusters) {
        if (!new_index_node.Insert(entry_[index].rect, entry_[index].subnode)) {
          STDB_THROW("Split insert fail");
        }
      }
      this->set_size(0);
      for (auto& index : l_clusters) {
        if (!this->Insert(entry_[index].rect, entry_[index].subnode)) {
          STDB_THROW("Split insert fail");
        }
      }

      if (dis_l < dis_r) {
        if (!this->Insert(rect, ptr)) {
          STDB_THROW("Split insert fail");
        }
      } else {
        if (!new_index_node.Insert(rect, ptr)) {
          STDB_THROW("Split insert fail");
        }
      }
      *new_rect = new_index_node.GetRect();
      *new_ptr = new_index_node.ptr();

      return GetRect();
    }

    // Choose subnode for Insertion.
    // @param point The point for insertion
    // @return Return nodeptr and index tuple.
    std::tuple<NodePtr, i32> ChooseSubnode(const Point& point) {
      DType min_distance = std::numeric_limits<DType>::max();
      size_t min_index = 0;
      for (size_t i = 0; i < this->size(); ++i) {
        auto distance = Node::Distance(entry_[i].rect, point);
        if (distance < min_distance) {
          min_distance = distance;
          min_index = i;
        }
      }
      return std::make_tuple(entry_[min_index].subnode, min_index);
    }

    // Make new index node
    static IndexNode MakeNew(u32 level) {
      auto ptr = BlockPool<BLOCK_SIZE>::Get()->Allocate();
      auto node = IndexNode(ptr);
      node.set_level(level);
      node.set_size(0);
      return node;
    }
  };

 public:
  virtual ~RTree() { }

  // Insert new node into rtree
  // @param point The point
  // @param payload The point's payload
  void Insert(const Point& point, i64 payload) {
    common::WriteLockGuard guard(rwlock_);
    if (!root_) {
      auto leaf_node = LeafNode::MakeNew(0);
      leaf_node.Insert(point, payload);

      auto root_node = IndexNode::MakeNew(1);
      Rect rect = { point, point };
      root_node.Insert(rect, leaf_node.ptr());

      root_ = root_node.ptr();
    } else {
      std::vector<std::tuple<NodePtr, i32>> paths; 
      ChoosePaths(paths, point);
      Insert(paths, point, payload);
    }
  }

  // knn query
  // @param point The poin
  // @param result The k nearest point for returning
  void KnnQuery(const Point& point, u32 k, std::vector<i64>& result) {
    common::ReadLockGuard guard(rwlock_);
    enum Type {
      kItem = 0,
      kLeaf,
      kIndex,
    };
    typedef std::tuple<DType, Type, i64> ElemType;
    std::priority_queue<ElemType, std::vector<ElemType>, std::greater<ElemType>> pq;
    pq.push(std::make_tuple(0, kIndex, (i64)root_));

    while (!pq.empty()) {
      auto t = pq.top(); pq.pop();
      auto type = std::get<1>(t);
      auto data = std::get<2>(t);

      switch (type) {
        case kItem : {
          result.push_back(data); 
          if (result.size() >= k) return;
        } break;

        case kLeaf : {
          auto node = LeafNode((NodePtr)data);
          for (u32 i = 0; i < node.size(); ++i) {
            data = node.payload_at(i);
            auto& cur_point = node.point_at(i);
            auto distance = Node::Distance(cur_point, point);
            pq.push(std::make_tuple(distance, kItem, data));
          }
        } break;

        case kIndex : {
          auto node = IndexNode((NodePtr)data);
          for (u32 i = 0; i < node.size(); ++i){
            auto& rect = node.child_rect(i);
            auto distance = Node::Distance(rect, point);
            data = (i64)node.child(i);
            if (node.level() == 1) {
              pq.push(std::make_tuple(distance, kLeaf, data));
            } else {
              pq.push(std::make_tuple(distance, kIndex, data));
            }
          }
        } break;
      }
    }
  }

  // Range query
  // @param rect The range MBR
  // @param result The point in the range for returning. 
  void RangeQuery(const Rect& rect, std::vector<i64>& result) {
    common::ReadLockGuard guard(rwlock_);
    std::vector<NodePtr> leafs;
    std::queue<NodePtr> q;
    q.push(root_);

    while (!q.empty()) {
      auto node_ptr = q.front(); q.pop();
      auto node = IndexNode(node_ptr);
      if (node.level() != 1) {
        for (u32 i = 0; i < node.size(); ++i) {
          auto& r = node.child_rect(i);
          if (Intersect(r, rect)) {
            q.push(node.child(i));
          }
        }
      } else {
        for (u32 i = 0; i < node.size(); ++i) {
          auto& r = node.child_rect(i);
          if (Intersect(r, rect)) {
            leafs.push_back(node.child(i));
          }
        }
      }
    }

    for (auto& node_ptr : leafs) {
      auto node = LeafNode(node_ptr);
      for (u32 i = 0; i < node.size(); ++i) {
        auto& p = node.point_at(i);
        if (Intersect(rect, p)) {
          result.push_back(node.payload_at(i));
        }
      }
    }
  }

  // Return the debug string of RTree.
  std::string DebugString() {
    common::ReadLockGuard guard(rwlock_);
    std::stringstream ss;
    
    ss << "Root:" << (void*)root_;
    std::queue<NodePtr> index_node_ptrs, leaf_node_ptrs;
    index_node_ptrs.push(root_);

    while (!index_node_ptrs.empty()) {
      auto ptr = index_node_ptrs.front(); index_node_ptrs.pop();
      IndexNode node(ptr);
      ss << std::endl << node.DebugString() << std::endl;

      for (u32 i = 0; i < node.size(); ++i) {
        if (node.level() > 1) {
          index_node_ptrs.push(node.child(i));
        } else {
          leaf_node_ptrs.push(node.child(i));
        }
      }
    }

    while (!leaf_node_ptrs.empty()) {
      auto ptr = leaf_node_ptrs.front(); leaf_node_ptrs.pop();
      LeafNode node(ptr);
      ss << std::endl << node.DebugString() << std::endl;
    }
    return ss.str();
  }

 protected:
  // Update the path's rect
  // @param paths The inserted paths
  // @param index The index 
  void UpdateRect(const std::vector<std::tuple<NodePtr, i32>>& paths, i32 index) {
    if (paths.empty()) return;
    UpdateRect(paths, index, paths.size() - 1);
  }

  // Recursively update rect
  // @param paths The inserted paths
  // @param index The index
  // @param pos The pos
  void UpdateRect(const std::vector<std::tuple<NodePtr, i32>>& paths, i32 index, u32 pos) {
    if (pos == 0) return;

    auto node_ptr = paths[pos];
    auto index_node = IndexNode(std::get<0>(node_ptr));

    auto child_ptr = index_node.child(index);
    if (index_node.level() == 1) {
      auto child_node = LeafNode(child_ptr);
      index_node.set_rect(index, child_node.GetRect());
    } else {
      auto child_node = IndexNode(child_ptr);
      index_node.set_rect(index, child_node.GetRect());
    }
    UpdateRect(paths, std::get<1>(node_ptr), pos - 1);
  }

  // Insert from paths
  // @param paths The paths
  // @param point To be inserted point
  // @param payload To be inserted point's payload
  void Insert(std::vector<std::tuple<NodePtr, i32>>& paths,
              const Point& point,
              i64 payload) {
    auto leaf = paths.back(); paths.pop_back();
    auto leaf_node = LeafNode(std::get<0>(leaf));
    if (leaf_node.Insert(point, payload)) {
      UpdateRect(paths, std::get<1>(leaf));
      return;
    }

    u32 level = 0;
    Rect new_rect;
    NodePtr new_ptr;
    auto rect = leaf_node.Split(point, payload, &new_rect, &new_ptr);
    UpdateRect(paths, std::get<1>(leaf));

    while (!paths.empty()) {
      auto node_ptr = paths.back(); paths.pop_back();
      auto index_node = IndexNode(std::get<0>(node_ptr));
      if (index_node.Insert(new_rect, new_ptr)) {
        UpdateRect(paths, std::get<1>(node_ptr));
        return;
      }
      level = index_node.level();

      Rect next_new_rect;
      NodePtr next_new_ptr;
      rect = index_node.Split(new_rect, new_ptr, &next_new_rect, &next_new_ptr);
      UpdateRect(paths, std::get<1>(node_ptr));

      new_rect = next_new_rect;
      new_ptr = next_new_ptr;
    }

    auto new_root = IndexNode::MakeNew(level + 1);
    if (!new_root.Insert(rect, root_)) {
      STDB_THROW("Split insert fail");
    }
    if (!new_root.Insert(new_rect, new_ptr)) {
      STDB_THROW("Split insert fail");
    }
    root_ = new_root.ptr();
  }

  // Choose paths for insertion
  // @param paths The paths
  // @param point To be inserted point
  void ChoosePaths(std::vector<std::tuple<NodePtr, i32>>& paths, const Point& point) {
    paths.push_back(std::make_tuple(root_, -1));
    while (true) {
      auto& node_ptr = std::get<0>(paths.back());
      IndexNode index_node(node_ptr);

      auto choice = index_node.ChooseSubnode(point);
      paths.emplace_back(choice);

      if (index_node.level() == 1) break;
    }
  }

 protected:
  NodePtr root_ = nullptr;
  std::mutex mutex_;
  common::RWLock rwlock_;
};

}  // namespace rtree
}  // namespace stdb

#endif  // STDB_INDEX_RTREE_H_
