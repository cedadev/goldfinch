"""
time_split.py
=============

Holds class DurationSplitter used to chop up time series into chunks of
decades, years or months.
"""

import re


class SimpleDate:

    def __init__(self, y, m, d):
        self.y = int(y)
        self.m = int(m)
        self.d = int(d)
        self.date = "%4d%02d%02d" % (self.y, self.m, self.d)

    def __repr__(self):
        return self.date

    def __lt__(self, d):
        if self.date < d.date:
            return True

        return False

    def __eq__(self, d):
        if self.date == d.date:
            return True

        return False


class DurationSplitter:
    """
    Splits into sensible time chunks based on inputs.
    """
    known_chunk_units = [None, "decade", "year", "month"]

    def __init__(self, chunk_unit=None):
        """
        Allows the setting of a persistent chunk_unit.
        """
        self._checkChunkUnit(chunk_unit)
        self.chunk_unit = chunk_unit

    def _convertDate(self, date):
        """
        Converts and returns date to a SimpleDate instance. If format is bad it raises an exception.
        """
        if isinstance(date, str):
            if len(date) != 8 or not re.match(r"^\d{8}$", date):
                raise Exception("Invalid date: %s" % str(date))

            return SimpleDate(date[:4], date[4:6], date[6:8])

        elif type(date) in (type((1, 2)), type([1, 2])):
            if len(date) != 3:
                raise Exception("Invalid date: %s" % str(date))

            for i in date:
                if isinstance(i, int):
                    raise Exception("Invalid date: %s" % str(date))

            return SimpleDate(date[0], date[1], date[2])

        else:
            raise Exception("Invalid date: %s" % str(date))

    def _checkChunkUnit(self, chunk_unit):
        if chunk_unit not in self.known_chunk_units:
            raise Exception("Invalid chunk unit '%s' not in list of %s." % (chunk_unit, str(self.known_chunk_units)))

    def splitDuration(self, start_date, end_date, chunk_unit=None):
        """
        Splits duration into  a list of n lists of [start, end] where each is represented as a SimpleDate
        instance with attributes of t.year, t.month, t.day. The list is returned.

        All time splits are done in logical places, e.g.:
         * decade -> at start of each year ending in 0
         * year -> 1st of jan each year to 31st dec
         * month -> first to last day of month

        All input dates times are represented as one of the following:
         * string: "YYYYMMDD"
         * tuple: (y, m, d)
        """
        start = self._convertDate(start_date)
        end = self._convertDate(end_date)

        if chunk_unit is not None:
            self._checkChunkUnit(chunk_unit)
        else:
            chunk_unit = self.chunk_unit

        # Assign a variable to record chunks in
        chunks = []
        ct_appended = False
        ct = start

        this_chunk = [ct]

        while ct < end:
            ct = self._addDay(ct)

            if (self._isLastDayOfMonth(ct) and chunk_unit == "month") or \
               (self._isLastDayOfYear(ct) and chunk_unit == "year") or \
               (self._isLastDayOfDecade(ct) and chunk_unit == "decade"):

                this_chunk.append(ct)
                chunks.append(this_chunk[:])

                next_day = self._addDay(ct)
                this_chunk = [next_day]
                ct_appended = True

            else:
                # Always the case for: chunk_unit=None
                ct_appended = False

        # Now add end onto last one if it does not match end exactly
        if ct_appended is False:
            this_chunk.append(ct)
            chunks.append(this_chunk[:])

        return chunks

    def _isLastDayOfMonth(self, date):
        "Returns True or False."
        ndays = self._daysInMonth(date.y, date.m)
        if date.d == ndays:
            return True

        return False

    def _isLastDayOfYear(self, date):
        "Returns True or False."
        if date.m == 12 and date.d == 31:
            return True

        return False

    def _isLastDayOfDecade(self, date):
        """
        Returns True or False.
        We define decades as 200001010000 - 200912312359
        """
        if self._isLastDayOfYear(date) and date.y % 10 == 9:
            return True

        return False

    def _addDay(self, date):
        """
        Returns one day added to this date.
        """
        (y, m, d) = (date.y, date.m, date.d)

        if self._isLastDayOfYear(date):
            y += 1
            m = 1
            d = 1
        elif self._isLastDayOfMonth(date):
            m += 1
            d = 1
        else:
            d += 1

        return SimpleDate(y, m, d)

    def _daysInMonth(self, y, m):
        if y % 4 == 0 and m == 2:
            return 29
        else:
            return int("dummy 31 28 31 30 31 30 31 31 30 31 30 31".split()[m])
