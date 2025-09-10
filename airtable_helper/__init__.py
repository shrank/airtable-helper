# flake8: noqa: E510,E722
import inspect
import os
import copy
import threading
import time
import sys
import requests
from pyairtable import Api
from pyairtable.formulas import match, GTE, LAST_MODIFIED_TIME, AND
from datetime import datetime, UTC, timedelta

# create multivalue dropdown field object
def create_multivalue(values):
    print("airtable_helper.create_multivalue() is obsolete\n")
    return values

class airtable_helper:
    def __init__(self, smartsheet_mode=False, sheet_id=None, base_id=None, enable_debug=False):
        self.api = Api(os.environ['AIRTABLE_API_KEY'])
        if(sheet_id is None):
            sheet_id = os.getenv("AIRTABLE_SHEET_ID")
        if(base_id is None):
            base_id = os.getenv("AIRTABLE_BASE_ID")
            
        stats_url = os.getenv("AIRTABLE_STATS_URL","")
        if(stats_url != ""):
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            self.stats = {
                "url": stats_url,
                "headers": None
            }
            stats_id = os.getenv("AIRTABLE_STATS_ID","")
            stats_header = os.getenv("AIRTABLE_STATS_HEADER","")
            if(stats_id != ""):
                self.stats["id"] = stats_id
            else:
                import uuid
                self.stats["id"] = str(uuid.uuid4())
                print("set dynamic statistics ID: %s" % self.stats["id"], file=sys.stderr)
            if(stats_header != ""):
                parts = stats_header.split(":")
                self.stats["headers"]={parts[0]: ":".join(parts[1:]).strip()}
            self.api.session.hooks['response'].append(self._api_stats)

        if(enable_debug is False):
            enable_debug = os.getenv("AIRTABLE_DEBUG","") == "True" 
        if(enable_debug):
            self.api.session.hooks['response'].append(self._api_debug)

        self.view_id = None
        if("/" in sheet_id):
            a = sheet_id.split("/")
            sheet_id = a[0]
            self.view_id = a[1]
        self.base_id = base_id
        self.sheet_id = sheet_id
        self.sheet = self.api.table(base_id, sheet_id)
        self.data = {}
        self.columns = {}
        self.last_timestamp = None
        self.updateRows={}
        self.model = None
        self.typecast = True
        self.smartsheet_mode=smartsheet_mode
        self.wh = {}
        self.autorefresh = None

    # load table model
    def loadModel(self):
        self.model = self.sheet.schema()
        self.columns = {}
        for a in self.model.fields:
            self.columns[a.name] = a.name
            


    # load all column headers
    def loadColumns(self):
        self.loadModel()

    def _loadColumns(self):
        self.columns = {}
        if(len(self.data) == 0):
            return
        for i in range(20):
          if(i>=len(self.data)):
              break
          for a in self.data[i].keys():
              self.columns[a] = a

    # return a copy of itself without vlaues
    def get_copy(self):
        data = copy.copy(self)
        data.data = None
        data.last_timestamp = None
        data.updateRows={}
        return data

    # get all rows from sheet
    def getAll(self, columns=None, formula=None):
        if(columns is None):
            self.data = self.sheet.all(formula=formula, view=self.view_id)
        else:
            self.data = self.sheet.all(fields = columns, formula=formula, view=self.view_id)
        if(len(self.columns) == 0):
            self._loadColumns()
        self.last_timestamp = datetime.now(UTC).isoformat()
        print(self.last_timestamp)
        return self.data

    # get all rows changed since we last got any rows
    def getUpdated(self, columns=None, formula=None):
        if(self.last_timestamp is None):
            return self.getAll(columns, formula)
        modified = GTE(LAST_MODIFIED_TIME(), self.last_timestamp)
        if(formula is not None):
            modified = AND(formula, modified)
        return self.getAll(columns=columns, formula=modified)

    # convert dict to row object
    def dict2row(self,values, row=None, diff=False, skip_nonexistend=None):
        if(skip_nonexistend is None):
            skip_nonexistend = self.smartsheet_mode is False
        new_row = { "fields": {}}
        if(diff and row is not None):
            for a in values.keys():
                if(a not in self.columns and skip_nonexistend):
                    continue
                if(a not in row["fields"] or row["fields"][a] != values[a]):
                    new_row["fields"][a] = values[a]
        else:
            new_row["fields"] = values

        if (row is not None):
            new_row["id"] = row["id"]

        if (len(new_row["fields"]) > 0):
            return new_row
        return None

    # update row
    def update(self, row, values, diff=False):
        self.addUpdate(row, values, diff)
        return self.commitUpdate()

    # add new to queue for bulk update
    def addUpdate(self, row, values, diff=False):
        new_row = self.dict2row(values, row, diff)
        if(new_row is not None):
            self.updateRows[new_row["id"]] = new_row

    # bulk update all rows in queue
    def commitUpdate(self):
        if (len(self.updateRows) > 0):
            rows=[]
            for a in self.updateRows:
                rows.append(self.updateRows[a])
            res = self.sheet.batch_update(rows, typecast=self.typecast)
            self.updateRows={}
            return res

    # create a contact field object
    def addContact(self, column, value):
        raise Exception("not implemented: %s()" % inspect.currentframe().f_code.co_nam)

    # insert row
    def insert(self, values):
        return self.insert_bulk([values])
    
    # insert rows
    def insert_bulk(self, rows):
        return self.sheet.batch_create(rows, typecast=self.typecast)

    # get value from cell
    def getValue(self, row, key, default=None):
        if(key in row["fields"]):
            if isinstance(row["fields"][key], list) and self.smartsheet_mode:
              return ", ".join(row["fields"][key])    
            return row["fields"][key]
        return default

    # get cell object
    def getCell(self, row, key):
        if(key in row["fields"]):
          return {"value": row["fields"][key]}
        return None

    # find first row where `column` has `value`
    def find_first_row(self, column, value):
        for a in self.data:
            if(self.getValue(a, column) == value):
                return a
        return None

    def get_first_row(self, column, value):
        return self.sheet.first(formula=match({column: value}), view=self.view_id)        

    # attach file to row
    def attachment_to_row(self,row_id, column, file_data, name):
        return self.sheet.upload_attachment(row_id, column, content=file_data, filename=name)
          
    # attach comment to row
    def comments_to_row(self, row, comments):
        raise Exception("not implemented: %s()" % inspect.currentframe().f_code.co_nam)
        id = None
        for c in comments:
            n = smartsheet.models.comment.Comment()
            n.text = c

            if(id == None):

              if(len(row.discussions) > 0):
                id = row.discussions[0].id
              else:
                a = self.smart.Discussions.get_row_discussions(self.sheet, row.id).data
                if(len(a) > 0):
                    id = a[0].id
                else:
                    a = self.smart.Discussions.create_discussion_on_row(self.sheet, row.id,  smartsheet.models.discussion.Discussion(props={"comment":n})).data
                    id = a.id
                    continue
              if(id == None):
                  return

            self.smart.Discussions.add_comment_to_discussion(self.sheet, id, n)

    # get all webhooks
    def get_webhooks(self):
        return self.api.base(self.base_id).webhooks()

    # create new webhook
    def create_webhook(self,name, url, columns=None, enabled=True, sources=["client", "formSubmission", "formPageSubmission", "automation" ], dataTypes=["tableData"]):
        options = {"options": {
            "filters": {
                "dataTypes": dataTypes,
                "recordChangeScope": self.sheet_id,
                "fromSources": sources
                }
            }
        }
        if(self.view_id is not None):
            options["options"]["filters"]["recordChangeScope"] = self.view_id
        if(columns is not None):
            if(self.model is None):
                self.loadModel()
            column_ids = []
            for b in columns:
              for a in self.model.fields:
                  if(a.name == b):
                      column_ids.append(a.id)

            options["options"]["filters"]["watchDataInFieldIds"] = column_ids
        res = self.api.base(self.base_id).add_webhook(url,options)
        self.wh[name] = res
        return res

 
    # delete existing webhook
    def delete_webhook(self, name):
        if(name in self.wh):
            return self.wh[name].delete()
        return None

    def delete_by_url(self, url):
        for a in self.get_webhooks():
            if(a.notification_url == url):
                a.delete()

    def autorefresh_webhooks(self):
      if(self.autorefresh is not None):
        return
      self.autorefresh =  webhook_refresh_thread()
      self.autorefresh.data = self
      self.autorefresh.start()

    def _api_debug(self, resp, *args, **kwargs):
      time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
      duration = resp.elapsed / timedelta(milliseconds=1)

      print("%s: %s %s [%d] duration: %d ms" % (time_str,resp.request.method, resp.url, resp.status_code, duration), file=sys.stderr)

    def _api_stats(self, resp, *args, **kwargs):
      data = {
          "event": "airtable request",
          "sourcetype": "airtable",
          "fields": {
              "source": self.stats["id"],
              "url": resp.url,
              "method": resp.request.method,
              "status_code": resp.status_code,
              "elapsed": resp.elapsed / timedelta(milliseconds=1)
            }
        }

      try:
          requests.post(self.stats["url"], headers=self.stats["headers"], json=data, timeout=0.5, verify=False)
      except Exception as e:
          print(e, file=sys.stderr)


class webhook_refresh_thread(threading.Thread):
  def run(self):
      while True:
          time.sleep(24*60*60)  # sleep 1 day
          print("refresh webhooks")
          for a in self.data.wh.values():
            try:
              self.data.sheet.api.post(f"https://api.airtable.com/v0/bases/{self.data.base_id}/webhooks/{a.id}/refresh")
            except Exception as e:
              print(e)
      

if __name__ == '__main__':
    import dotenv
    dotenv.load_dotenv()
    a = airtable_helper()
    a.getUpdated(formula=match({"FLAG": True}))
    time.sleep(20)
#    print(a.getUpdated(formula=match({"FLAG": True})))
