# This file is part of task_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Utilities for measuring execution time.
"""

__all__ = ["logInfo", "timeMethod"]

import functools
import resource
import time
import datetime

from lsst.log import Log, log


def logPairs(obj, pairs, logLevel=Log.DEBUG):
    """Log ``(name, value)`` pairs to ``obj.metadata`` and ``obj.log``

    Parameters
    ----------
    obj : `lsst.task.base.Task`-type
        A `~lsst.task.base.Task` or any other object with these two attributes:

        - ``metadata`` an instance of `lsst.daf.base.PropertyList`` (or other
          object with ``add(name, value)`` method).
        - ``log`` an instance of `lsst.log.Log`.

    pairs : sequence
        A sequence of ``(name, value)`` pairs, with value typically numeric.
    logLevel : optional
        Log level (an `lsst.log` level constant, such as `lsst.log.Log.DEBUG`).
    """
    strList = []
    for name, value in pairs:
        try:
            # Use LongLong explicitly here in case an early value in the
            # sequence is int-sized
            obj.metadata.addLongLong(name, value)
        except TypeError:
            obj.metadata.add(name, value)
        strList.append(f"{name}={value}")
    log("timer." + obj.log.getName(), logLevel, "; ".join(strList))


def logInfo(obj, prefix, logLevel=Log.DEBUG):
    """Log timer information to ``obj.metadata`` and ``obj.log``.

    Parameters
    ----------
    obj : `lsst.task.base.Task`-type
        A `~lsst.task.base.Task` or any other object with these two attributes:

        - ``metadata`` an instance of `lsst.daf.base.PropertyList`` (or other
          object with ``add(name, value)`` method).
        - ``log`` an instance of `lsst.log.Log`.

    prefix
        Name prefix, the resulting entries are ``CpuTime``, etc.. For example
        timeMethod uses ``prefix = Start`` when the method begins and
        ``prefix = End`` when the method ends.
    logLevel : optional
        Log level (an `lsst.log` level constant, such as `lsst.log.Log.DEBUG`).

    Notes
    -----
    Logged items include:

    - ``Utc``: UTC date in ISO format (only in metadata since log entries have
      timestamps).
    - ``CpuTime``: System + User CPU time (seconds). This should only be used
        in differential measurements; the time reference point is undefined.
    - ``MaxRss``: maximum resident set size.

    All logged resource information is only for the current process; child
    processes are excluded.
    """
    cpuTime = time.process_time()
    utcStr = datetime.datetime.utcnow().isoformat()
    res = resource.getrusage(resource.RUSAGE_SELF)
    obj.metadata.add(name=prefix + "Utc", value=utcStr)  # log messages already have timestamps
    logPairs(obj=obj,
             pairs=[
                 (prefix + "CpuTime", cpuTime),
                 (prefix + "UserTime", res.ru_utime),
                 (prefix + "SystemTime", res.ru_stime),
                 (prefix + "MaxResidentSetSize", int(res.ru_maxrss)),
                 (prefix + "MinorPageFaults", int(res.ru_minflt)),
                 (prefix + "MajorPageFaults", int(res.ru_majflt)),
                 (prefix + "BlockInputs", int(res.ru_inblock)),
                 (prefix + "BlockOutputs", int(res.ru_oublock)),
                 (prefix + "VoluntaryContextSwitches", int(res.ru_nvcsw)),
                 (prefix + "InvoluntaryContextSwitches", int(res.ru_nivcsw)),
             ],
             logLevel=logLevel,
             )


def timeMethod(func):
    """Decorator to measure duration of a task method.

    Parameters
    ----------
    func
        The method to wrap.

    Notes
    -----
    Writes various measures of time and possibly memory usage to the task's
    metadata; all items are prefixed with the function name.

    .. warning::

       This decorator only works with instance methods of Task, or any class
       with these attributes:

       - ``metadata``: an instance of `lsst.daf.base.PropertyList` (or other
         object with ``add(name, value)`` method).
       - ``log``: an instance of `lsst.log.Log`.

    Examples
    --------
    To use:

    .. code-block:: python

        import lsst.task.base as taskBase
        class FooTask(taskBase.Task):
            pass

            @taskBase.timeMethod
            def run(self, ...): # or any other instance method you want to time
                pass
    """

    @functools.wraps(func)
    def wrapper(self, *args, **keyArgs):
        logInfo(obj=self, prefix=func.__name__ + "Start")
        try:
            res = func(self, *args, **keyArgs)
        finally:
            logInfo(obj=self, prefix=func.__name__ + "End")
        return res
    return wrapper
