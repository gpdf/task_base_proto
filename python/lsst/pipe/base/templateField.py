# This file is part of task_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Module defining config classes for all Task subclasses.
"""
__all__ = ["TemplateField"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from numbers import Number

# -----------------------------
#  Imports for other modules --
# -----------------------------
import lsst.pex.config as pexConfig

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# ------------------------
#  Exported definitions --
# ------------------------


class TemplateField(pexConfig.Field):
    """This Field is specialized for use with connection templates.
    Specifically it treats strings or numbers as valid input, as occasionally
    numbers are used as a cycle counter in templates.

    The reason for the specialized field, is that when numbers are involved
    with the config override system through pipelines or from the command line,
    sometimes the quoting to get appropriate values as strings gets
    complicated. This will simplify the process greatly.
    """
    def _validateValue(self, value):
        if value is None:
            return

        if not (isinstance(value, str) or isinstance(value, Number)):
            raise TypeError(f"Value {value} is of incorrect type {pexConfig.config._typeStr(value)}."
                            f" Expected type str or a number")
        if self.check is not None and not self.check(value):
            ValueError("Value {value} is not a valid value")

    def __set__(self, instance, value, at=None, label='assignment'):
        # validate first, even though validate will be called in super
        self._validateValue(value)
        # now, explicitly make it into a string
        value = str(value)
        super().__set__(instance, value, at, label)
