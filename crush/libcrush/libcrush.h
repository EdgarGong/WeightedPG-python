//
// Copyright (C) 2017 <contact@redhat.com>
//
// Author: Loic Dachary <loic@dachary.org>
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
//
#ifndef _LIBCRUSH_H
#define _LIBCRUSH_H

#ifdef __cplusplus
extern "C"{
#endif

#include <Python.h>
#include <structmember.h>
#include <opcode.h>
#include <frameobject.h>

#if PY_MAJOR_VERSION >= 3

#define MyInt_Check			PyLong_Check
#define MyText_Check			PyUnicode_Check
#define MyText_Type                     PyUnicode_Type
#define MyText_AS_BYTES(o)              PyUnicode_AsASCIIString(o)
#define MyBytes_GET_SIZE(o)             PyBytes_GET_SIZE(o)
#define MyBytes_AS_STRING(o)            PyBytes_AS_STRING(o)
#define MyText_AsString(o)              PyUnicode_AsUTF8(o)
#define MyText_FromFormat               PyUnicode_FromFormat
#define MyInt_FromInt(i)                PyLong_FromLong((long)i)
#define MyInt_AsInt(o)                  (int)PyLong_AsLong(o)
#define MyText_InternFromString(s)      PyUnicode_InternFromString(s)

#define MyType_HEAD_INIT                PyVarObject_HEAD_INIT(NULL, 0)

#else

#define MyInt_Check			PyInt_Check
#define MyText_Check(a)			PyString_Check(a) || PyUnicode_Check(a)
#define MyText_Type                     PyString_Type
#define MyText_AS_BYTES(o)              (Py_INCREF(o), o)
#define MyBytes_GET_SIZE(o)             PyString_GET_SIZE(o)
#define MyBytes_AS_STRING(o)            PyString_AS_STRING(o)
#define MyText_AsString(o)              PyString_AsString(o)
#define MyText_FromFormat               PyUnicode_FromFormat
#define MyInt_FromInt(i)                PyInt_FromLong((long)i)
#define MyInt_AsInt(o)                  (int)PyInt_AsLong(o)
#define MyText_InternFromString(s)      PyString_InternFromString(s)

#define MyType_HEAD_INIT                PyObject_HEAD_INIT(NULL)  0,

#endif /* Py3k */

#ifdef __cplusplus
}
#endif

/* The LibCrush type. */

#ifdef __cplusplus
extern "C"{
#endif
#include "crush.h"
#ifdef __cplusplus
}
#endif

#include "CrushWrapper.h"
#include <map>
#include <vector>

struct OSDMap {
  std::map<uint32_t, std::vector<int32_t>> pg_upmap;
  std::map<uint32_t, std::vector<std::pair<int32_t, int32_t>>> pg_upmap_items;
  std::map<uint32_t, int32_t> pg_upmap_primaries;
};

#ifdef __cplusplus
extern "C"{
#endif
typedef struct LibCrush {
  PyObject_HEAD

  int verbose;
  int backward_compatibility;
  struct crush_map *tunables;

  int has_bucket_weights;
  struct crush_map *map;
  PyObject *types;
  PyObject *items;
  PyObject *ritems;
  int highest_device_id;
  PyObject *rules;
  PyObject *choose_args;

  CrushWrapper crush_wrapper;
  // OSDMap osdmap;
} LibCrush;


extern PyTypeObject LibCrushType;
#ifdef __cplusplus
}
#endif

#endif /* _LIBCRUSH_H */
