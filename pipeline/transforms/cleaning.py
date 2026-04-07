import apache_beam as beam
import re
from datetime import datetime, timezone


class CleanAndValidate(beam.DoFn):

    #We create a regular expression to check if the url starts with http or https. 
    #"r" indicates it is a raw string. Backslashes are taken literally
    URL_RE = re.compile(r'^https?://') 

    def process(self, element):

        #We define a function to remove  extra whitespaces from text fields. First we split the string on any whitespaces
        #with split() and then use " ".join(...) to join words back together with a single space.
        #^ - start of the string
        #http - matches "http"
        #s? - the "s" is optional (https)

        def sanitize(text):
            return " ".join(str(text).split())
        try:
            record = {
                "id":           element["id"],
                "title":        sanitize(element["title"]),
                "description":  sanitize(element["description"]),
                "url":          str(element["url"]).strip(),
                "extracted_at": str(element["extracted_at"]).strip(),
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }

            if not record["title"]:
                return
            if not record["description"]:
                return
            if not self.URL_RE.match(record["url"]):
                return

            yield record
        except (ValueError, KeyError):
            pass


def to_bq_row(element):
    return element