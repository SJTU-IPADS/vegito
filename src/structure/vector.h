/*
 * The code is a part of our project called VEGITO, which retrofits
 * high availability mechanism to tame hybrid transaction/analytical
 * processing.
 *
 * Copyright (c) 2021 Shanghai Jiao Tong University.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://ipads.se.sjtu.edu.cn/projects/vegito
 *
 */

#ifndef SIMPLE_VECTOR_
#define SIMPLE_VECTOR_

#include "all.h"

template <class T>
class Vector {
 public:
  Vector();
  ~Vector();

  void push_back(T *p);
  void push_back(T p);
  T *push_back();
  void clear();

  inline size_t size() const { return size_; }
  inline T &operator[](size_t id) { return array_[id]; }
  inline T operator[](size_t id) const { return array_[id]; }
  inline T *begin() const { return array_; }
  inline T *end() const { return &array_[size_]; }

 private:
  void enlarge_();

  size_t size_;
  size_t capacity_;

  T *array_;
} __attribute__ ((aligned (CACHE_LINE_SZ)));

template <class T>
Vector<T>::Vector() 
  : size_(0), capacity_(1024)
{
  array_ = (T *) malloc(capacity_ * sizeof(T));
}

template <class T>
Vector<T>::~Vector() {
  free(array_);
}

template <class T>
void Vector<T>::push_back(T *p) {
  size_t tail = size_;
  ++size_;
  if (size_ > capacity_) {
    enlarge_();
  }
  array_[tail] = *p;
}

template <class T>
void Vector<T>::push_back(T p) {
  size_t tail = size_;
  ++size_;
  if (size_ > capacity_) {
    enlarge_();
  }
  array_[tail] = p;
}

template <class T>
T *Vector<T>::push_back() {
  size_t tail = size_;
  ++size_;
  if (size_ > capacity_) {
    enlarge_();
  }
  return &array_[tail];
}

template <class T>
inline void Vector<T>::clear() {
  size_ = 0;
}

template <class T>
inline void Vector<T>::enlarge_() {
  size_t new_cap = capacity_ * 2;
  T *new_array = (T *) malloc(new_cap * sizeof(T));
  memcpy(new_array, array_, capacity_ * sizeof(T));

  array_ = new_array;
  capacity_ = new_cap;
}

#endif
