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

import time
import unittest
import numbers
import logging
from collections.abc import Mapping

import lsst.pex.config as pexConfig
import lsst.task.base as taskBase


class AddConfig(pexConfig.Config):
    addend = pexConfig.Field(doc="amount to add", dtype=float, default=3.1)


class AddTask(taskBase.Task):
    ConfigClass = AddConfig

    @taskBase.timeMethod
    def run(self, val):
        self.metadata.add("add", self.config.addend)
        return taskBase.Struct(
            val=val + self.config.addend,
        )


class MultConfig(pexConfig.Config):
    multiplicand = pexConfig.Field(doc="amount by which to multiply", dtype=float, default=2.5)


class MultTask(taskBase.Task):
    ConfigClass = MultConfig

    @taskBase.timeMethod
    def run(self, val):
        self.metadata.add("mult", self.config.multiplicand)
        return taskBase.Struct(
            val=val * self.config.multiplicand,
        )


# prove that registry fields can also be used to hold subtasks
# by using a registry to hold MultTask
multRegistry = pexConfig.makeRegistry("Registry for Mult-like tasks")
multRegistry.register("stdMult", MultTask)


class AddMultConfig(pexConfig.Config):
    add = AddTask.makeField("add task")
    mult = multRegistry.makeField("mult task", default="stdMult")


class AddMultTask(taskBase.Task):
    ConfigClass = AddMultConfig
    _DefaultName = "addMult"

    """First add, then multiply"""

    def __init__(self, **keyArgs):
        taskBase.Task.__init__(self, **keyArgs)
        self.makeSubtask("add")
        self.makeSubtask("mult")

    @taskBase.timeMethod
    def run(self, val):
        with self.timer("context"):
            addRet = self.add.run(val)
            multRet = self.mult.run(addRet.val)
            self.metadata.add("addmult", multRet.val)
            return taskBase.Struct(
                val=multRet.val,
            )

    @taskBase.timeMethod
    def failDec(self):
        """A method that fails with a decorator
        """
        raise RuntimeError("failDec intentional error")

    def failCtx(self):
        """A method that fails inside a context manager
        """
        with self.timer("failCtx"):
            raise RuntimeError("failCtx intentional error")


class AddTwiceTask(AddTask):
    """Variant of AddTask that adds twice the addend"""

    def run(self, val):
        addend = self.config.addend
        return taskBase.Struct(val=val + (2 * addend))


class TaskTestCase(unittest.TestCase):
    """A test case for Task
    """

    def setUp(self):
        self.valDict = dict()

    def tearDown(self):
        self.valDict = None

    def testBasics(self):
        """Test basic construction and use of a task
        """
        for addend in (1.1, -3.5):
            for multiplicand in (0.9, -45.0):
                config = AddMultTask.ConfigClass()
                config.add.addend = addend
                config.mult["stdMult"].multiplicand = multiplicand
                # make sure both ways of accessing the registry work and give
                # the same result
                self.assertEqual(config.mult.active.multiplicand, multiplicand)
                addMultTask = AddMultTask(config=config)
                for val in (-1.0, 0.0, 17.5):
                    ret = addMultTask.run(val=val)
                    self.assertAlmostEqual(ret.val, (val + addend) * multiplicand)

    def testNames(self):
        """Test getName() and getFullName()
        """
        addMultTask = AddMultTask()
        self.assertEqual(addMultTask.getName(), "addMult")
        self.assertEqual(addMultTask.add.getName(), "add")
        self.assertEqual(addMultTask.mult.getName(), "mult")

        self.assertEqual(addMultTask._name, "addMult")
        self.assertEqual(addMultTask.add._name, "add")
        self.assertEqual(addMultTask.mult._name, "mult")

        self.assertEqual(addMultTask.getFullName(), "addMult")
        self.assertEqual(addMultTask.add.getFullName(), "addMult.add")
        self.assertEqual(addMultTask.mult.getFullName(), "addMult.mult")

        self.assertEqual(addMultTask._fullName, "addMult")
        self.assertEqual(addMultTask.add._fullName, "addMult.add")
        self.assertEqual(addMultTask.mult._fullName, "addMult.mult")

    def testLog(self):
        """Test the Task's logger
        """
        addMultTask = AddMultTask()
        self.assertEqual(addMultTask.log.name, "addMult")
        self.assertEqual(addMultTask.add.log.name, "addMult.add")

        log = logging.getLogger("tester")
        addMultTask = AddMultTask(log=log)
        self.assertEqual(addMultTask.log.name, "tester.addMult")
        self.assertEqual(addMultTask.add.log.name, "tester.addMult.add")

    def testGetFullMetadata(self):
        """Test getFullMetadata()
        """
        addMultTask = AddMultTask()
        fullMetadata = addMultTask.getFullMetadata()
        self.assertIsInstance(fullMetadata.get("addMult"), Mapping)
        self.assertIsInstance(fullMetadata.get("addMult:add"), Mapping)
        self.assertIsInstance(fullMetadata.get("addMult:mult"), Mapping)

    def testEmptyMetadata(self):
        task = AddMultTask()
        task.run(val=1.2345)
        task.emptyMetadata()
        fullMetadata = task.getFullMetadata()
        self.assertEqual(len(fullMetadata.get("addMult")), 0)
        self.assertEqual(len(fullMetadata.get("addMult:add")), 0)
        self.assertEqual(len(fullMetadata.get("addMult:mult")), 0)

    def testReplace(self):
        """Test replacing one subtask with another
        """
        for addend in (1.1, -3.5):
            for multiplicand in (0.9, -45.0):
                config = AddMultTask.ConfigClass()
                config.add.retarget(AddTwiceTask)
                config.add.addend = addend
                config.mult["stdMult"].multiplicand = multiplicand
                addMultTask = AddMultTask(config=config)
                for val in (-1.0, 0.0, 17.5):
                    ret = addMultTask.run(val=val)
                    self.assertAlmostEqual(ret.val, (val + (2 * addend)) * multiplicand)

    def testFail(self):
        """Test timers when the code they are timing fails
        """
        addMultTask = AddMultTask()
        try:
            addMultTask.failDec()
            self.fail("Expected RuntimeError")
        except RuntimeError:
            self.assertTrue("failDecEndCpuTime" in addMultTask.metadata)
        try:
            addMultTask.failCtx()
            self.fail("Expected RuntimeError")
        except RuntimeError:
            self.assertTrue("failCtxEndCpuTime" in addMultTask.metadata)

    def testTimeMethod(self):
        """Test that the timer is adding the right metadata
        """
        addMultTask = AddMultTask()
        addMultTask.run(val=1.1)
        # Check existence and type
        for key, keyType in (("Utc", str),
                             ("CpuTime", float),
                             ("UserTime", float),
                             ("SystemTime", float),
                             ("MaxResidentSetSize", numbers.Integral),
                             ("MinorPageFaults", numbers.Integral),
                             ("MajorPageFaults", numbers.Integral),
                             ("BlockInputs", numbers.Integral),
                             ("BlockOutputs", numbers.Integral),
                             ("VoluntaryContextSwitches", numbers.Integral),
                             ("InvoluntaryContextSwitches", numbers.Integral),
                             ):
            for when in ("Start", "End"):
                for method in ("run", "context"):
                    name = method + when + key
                    self.assertIn(name, addMultTask.metadata.keys(),
                                  name + " is missing from task metadata")
                    self.assertIsInstance(addMultTask.metadata.get(name), keyType,
                                          f"{name} is not of the right type "
                                          f"({keyType} vs {type(addMultTask.metadata.get(name))})")
        # Some basic sanity checks
        currCpuTime = time.process_time()
        self.assertLessEqual(
            addMultTask.metadata.get("runStartCpuTime"),
            addMultTask.metadata.get("runEndCpuTime"),
        )
        self.assertLessEqual(addMultTask.metadata.get("runEndCpuTime"), currCpuTime)
        self.assertLessEqual(
            addMultTask.metadata.get("contextStartCpuTime"),
            addMultTask.metadata.get("contextEndCpuTime"),
        )
        self.assertLessEqual(addMultTask.metadata.get("contextEndCpuTime"), currCpuTime)
        self.assertLessEqual(
            addMultTask.add.metadata.get("runStartCpuTime"),
            addMultTask.metadata.get("runEndCpuTime"),
        )
        self.assertLessEqual(addMultTask.add.metadata.get("runEndCpuTime"), currCpuTime)


if __name__ == "__main__":
    unittest.main()
