/* -*- indent-tabs-mode: nil; tab-width: 4; -*- */

/*
 
 * Greenlet C API Header
 * 
 *
 * This header defines the public C API for interacting with
 * greenlet objects in CPython extensions.
 *
 * It exposes:
 * - Greenlet object structure
 * - Type checking macros
 * - C API function pointer definitions
 * - Capsule import mechanism
 *
 * NOTE:
 * This file is intended for extension/module developers who
 * need direct interaction with greenlet internals at the C level.
 *
 * Do not modify API pointer indices — they must remain stable.
 
 */

#ifndef Py_GREENLETOBJECT_H
#define Py_GREENLETOBJECT_H

#include <Python.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Deprecated version macro.
 * Maintained for backward compatibility.
 * Does not reflect the runtime greenlet version.
 */
#define GREENLET_VERSION "1.0.0"


/*
 * When building outside the greenlet module,
 * use a generic pointer type for implementation.
 */
#ifndef GREENLET_MODULE
#define implementation_ptr_t void*
#endif


/*
 * Core Greenlet Object Structure
 *
 * PyGreenlet extends PyObject and contains:
 * - weak reference list
 * - attribute dictionary
 * - implementation-specific pointer
 */
typedef struct _greenlet {
    PyObject_HEAD
    PyObject* weakreflist;
    PyObject* dict;
    implementation_ptr_t pimpl;
} PyGreenlet;


/*
 * Type checking macro
 * Returns true if object is a PyGreenlet instance.
 */
#define PyGreenlet_Check(op) (op && PyObject_TypeCheck(op, &PyGreenlet_Type))


/* 
 * C API Symbol Definitions
 * 
 *
 * These indices define the position of exported symbols
 * in the C API capsule array.
 *
 * IMPORTANT:
 * Do not change ordering — ABI compatibility depends on it.
 * 
 */

/* Total exported symbols */
#define PyGreenlet_API_pointers 12

/* Type & Exception indices */
#define PyGreenlet_Type_NUM 0
#define PyExc_GreenletError_NUM 1
#define PyExc_GreenletExit_NUM 2

/* Core API functions */
#define PyGreenlet_New_NUM 3
#define PyGreenlet_GetCurrent_NUM 4
#define PyGreenlet_Throw_NUM 5
#define PyGreenlet_Switch_NUM 6
#define PyGreenlet_SetParent_NUM 7

/* State inspection functions */
#define PyGreenlet_MAIN_NUM 8
#define PyGreenlet_STARTED_NUM 9
#define PyGreenlet_ACTIVE_NUM 10
#define PyGreenlet_GET_PARENT_NUM 11


#ifndef GREENLET_MODULE

/*
 * 
 * API Access Layer for External Modules
 * 
 *
 * Modules using the greenlet C API must:
 * 1. Call PyGreenlet_Import()
 * 2. Use the macros below to access functions
 * 
 */

static void** _PyGreenlet_API = NULL;


/* Exported Type */
#define PyGreenlet_Type \
    (*(PyTypeObject*)_PyGreenlet_API[PyGreenlet_Type_NUM])


/* Exported Exceptions */
#define PyExc_GreenletError \
    ((PyObject*)_PyGreenlet_API[PyExc_GreenletError_NUM])

#define PyExc_GreenletExit \
    ((PyObject*)_PyGreenlet_API[PyExc_GreenletExit_NUM])


/*
 * Create new greenlet:
 * greenlet.greenlet(run, parent=None)
 */
#define PyGreenlet_New                                        \
    (*(PyGreenlet * (*)(PyObject * run, PyGreenlet * parent)) \
         _PyGreenlet_API[PyGreenlet_New_NUM])


/*
 * Get currently executing greenlet:
 * greenlet.getcurrent()
 */
#define PyGreenlet_GetCurrent \
    (*(PyGreenlet * (*)(void)) _PyGreenlet_API[PyGreenlet_GetCurrent_NUM])


/*
 * Raise exception in target greenlet:
 * g.throw(typ, val, tb)
 */
#define PyGreenlet_Throw                 \
    (*(PyObject * (*)(PyGreenlet * self, \
                      PyObject * typ,    \
                      PyObject * val,    \
                      PyObject * tb))    \
         _PyGreenlet_API[PyGreenlet_Throw_NUM])


/*
 * Switch execution to target greenlet:
 * g.switch(*args, **kwargs)
 */
#define PyGreenlet_Switch                                              \
    (*(PyObject *                                                      \
       (*)(PyGreenlet * greenlet, PyObject * args, PyObject * kwargs)) \
         _PyGreenlet_API[PyGreenlet_Switch_NUM])


/*
 * Set parent greenlet:
 * g.parent = new_parent
 */
#define PyGreenlet_SetParent                                 \
    (*(int (*)(PyGreenlet * greenlet, PyGreenlet * nparent)) \
         _PyGreenlet_API[PyGreenlet_SetParent_NUM])


/*
 * Get parent greenlet:
 *
 * Returns:
 * - New reference to parent
 * - NULL if no parent (may not indicate exception)
 *
 * Caller must decref returned value.
 */
#define PyGreenlet_GetParent                                    \
    (*(PyGreenlet* (*)(PyGreenlet*))                             \
     _PyGreenlet_API[PyGreenlet_GET_PARENT_NUM])


/* Deprecated alias for backward compatibility */
#define PyGreenlet_GET_PARENT PyGreenlet_GetParent


/* State inspection helpers */

#define PyGreenlet_MAIN                                         \
    (*(int (*)(PyGreenlet*))                                    \
     _PyGreenlet_API[PyGreenlet_MAIN_NUM])

#define PyGreenlet_STARTED                                      \
    (*(int (*)(PyGreenlet*))                                    \
     _PyGreenlet_API[PyGreenlet_STARTED_NUM])

#define PyGreenlet_ACTIVE                                       \
    (*(int (*)(PyGreenlet*))                                    \
     _PyGreenlet_API[PyGreenlet_ACTIVE_NUM])


/*
 * 
 * API Import Macro
 * 
 *
 * Must be called before using any C API functions.
 *
 * Internally imports the PyCapsule:
 *     "greenlet._C_API"
 *
 * NOTE:
 * Capsule location moved to greenlet._greenlet._C_API
 * but compatibility alias is preserved.
 * 
 */
#define PyGreenlet_Import()                                               \
    {                                                                     \
        _PyGreenlet_API = (void**)PyCapsule_Import("greenlet._C_API", 0); \
    }

#endif /* GREENLET_MODULE */


#ifdef __cplusplus
}
#endif

#endif /* !Py_GREENLETOBJECT_H */
