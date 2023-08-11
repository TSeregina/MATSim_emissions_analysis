"""
This stage takes a huge events.xml.gz file containing emissions,
breaks it into several xml.gz files, extracting only emissions events.

[break_after] specifies the number of emissions events per split file,
[extract_event_types] specifies the events types to extract, i.e. ["coldEmissionEvent","warmEmissionEvent"].

Previous stage: simulation output with emissions events xml.gz file
Next stage: emissionsClean (parallelized)
"""

#==============================
# Class to split events.xml.gz file into chunks with elements of type in [extract_types] list,
# s.t. each chunk contains the number of elements [break_after].
#==============================
import os
import sys
from xml.sax import parse
from xml.sax.saxutils import XMLGenerator
import gzip

import attributes as attr


class CycleFile(object):

    def __init__(self, filename):
        self.basename, self.ext = os.path.splitext(filename)
        self.index = 0
        self.open_next_file()

    def open_next_file(self):
        self.index += 1
        self.file = gzip.open(self.name(), 'wt')  # gzip

    def name(self):
        return '%s%s%s.gz' % (self.basename, self.index, self.ext)

    def cycle(self):
        self.file.close()
        self.open_next_file()

    def write(self, str):
        self.file.write(str.decode("utf-8") if isinstance(str, bytes) else str)

    def close(self):
        self.file.close()


class XMLBreakExtract(XMLGenerator):

    def __init__(self, extract_types=None, break_after=100, out=None, *args, **kwargs):
        XMLGenerator.__init__(self, out, encoding='utf-8', *args, **kwargs)
        self.out_file = out
        self.extract_types = extract_types
        self.break_after = break_after
        self.context = []
        self.count = 0

    def startElement(self, name, attrs):
        if "type" in attrs.getNames() and attrs["type"] in self.extract_types:
            XMLGenerator.startElement(self, name, attrs)
            self.context.append((name, attrs))
            self.count += 1

    def endElement(self, name):
        if len(self.context):
            XMLGenerator.endElement(self, name)
            self.context.pop()
            if self.count == self.break_after:
                self.count = 0
                self.out_file.cycle()
                XMLGenerator.startDocument(self)

# File path/name for split files
fp_CycleFile = attr.simout_emissions_fp.split(".gz")[0]

parse(gzip.open(attr.simout_emissions_fp, 'rb'),
      XMLBreakExtract(extract_event_types = attr.extract_event_types,
                      break_after = int(attr.break_after),
                      out=CycleFile(fp_CycleFile)))
