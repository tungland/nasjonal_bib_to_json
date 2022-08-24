import pymarc
from pymongo import MongoClient
from time import time

"""Stream Marc-XML to MongoDB"""

class myhandler(pymarc.XmlHandler):
    def __init__(self, strict=False, normalize_form=None):
        super().__init__(strict, normalize_form)
        self.client = MongoClient()
        self.db = self.client.nasjonalbibliografien
        self.collection = self.db.records
        self.count = 0
        self.start = time()
        
    def process_record(self, record):
        dct = record.as_dict()        
        t = self.collection.insert_one(dct)
        self.count += 1        
        if (self.count % 10000) == 0 :
            print("Records parsed: {}".format(self.count))
            now = time()
            print(("Time elapsed: {}".format(now - self.start)))
       
    
def main():
    #handler = pymarc.XmlHandler()
    handler = myhandler()
    
    pymarc.parse_xml('/home/larsm/my_projects/nasjonal_bib_to_json/data/nasjonalbibliografien_root_2022.xml', handler)    
   
if __name__ == "__main__":
    main()