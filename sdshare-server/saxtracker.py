
from xml.sax import handler
            
# --- State tracker

class SAXTracker(handler.ContentHandler):

    def __init__(self, keep_contents_of = []):
        self._elemstack = []
        self._contents = ""
        self._keep_contents_of = {}
        
        for element in keep_contents_of:
            self._keep_contents_of[element] = 1
        
    def startElement(self, name, attrs):
        self._contents = ""
        self._elemstack.append(name)

    def characters(self, data):
        if self._elemstack != [] and \
           self._keep_contents_of.has_key(self._elemstack[-1]):
            self._contents = self._contents + data
        
    def endElement(self, name):
        del self._elemstack[-1]
