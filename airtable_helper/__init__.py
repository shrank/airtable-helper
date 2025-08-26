# flake8: noqa: E510,E722
import inspect
import os
import copy
from pyairtable import Api
from pyairtable.formulas import match, GTE, LAST_MODIFIED_TIME
from datetime import datetime, UTC

# create multivalue dropdown field object
def create_multivalue(values):
    raise Exception("not implemented: %s()" % inspect.currentframe().f_code.co_nam)

class airtable_helper:
    def __init__(self, smartsheet_mode=False, sheet_id=None, base_id=None):
        self.api = Api(os.environ['AIRTABLE_API_KEY'])
        if(sheet_id is None):
            sheet_id = os.getenv("AIRTABLE_SHEET_ID")
        if(base_id is None):
            base_id = os.getenv("AIRTABLE_BASE_ID")
        self.sheet = self.api.table(base_id, sheet_id)
        self.data = {}
        self.columns = {}
        self.last_timestamp = None
        self.updateRows={}
        self.model = None
        self.typecast = True
        self.smartsheet_mode=smartsheet_mode

    # load table model
    def loadModel(self):
        self.model = self.sheet.schema()

    # load all column headers
    def loadColumns(self):
        self._loadColumns()

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
        data.columns = {}
        data.last_timestamp = None
        data.updateRows={}
        return data

    # get all rows from sheet
    def getAll(self, columns=None, formula=None):
        if(columns is None):
            self.data = self.sheet.all(formula=formula)
        else:
            self.data = self.sheet.all(fields = columns, formula=formula)
        self._loadColumns()
        self.last_timestamp = datetime.now(UTC).isoformat()
        print(self.last_timestamp)
        return self.data

    # get all rows changed since we last got any rows
    def getUpdated(self, columns=None):
        if(self.last_timestamp is None):
            return self.getAll(columns)
        formula = GTE(LAST_MODIFIED_TIME(), self.last_timestamp)
        return self.getAll(columns=columns, formula=formula)

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
        return self.sheet.first(formula=match({column: value}))        

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
        raise Exception("not implemented: %s()" % inspect.currentframe().f_code.co_nam)
        return self.smart.Webhooks.list_webhooks().data

    # create new webhook
    def create_webhook(self,name, url, columns=None, enabled=True):
        raise Exception("not implemented: %s()" % inspect.currentframe().f_code.co_nam)
        item = smartsheet.models.webhook.Webhook()
        item.callback_url = url
        item.name = name
        item.events =  '*.*'
        item.scope = "sheet"
        item.version = 1
        item.scope_object_id = int(self.sheet)
        if(columns is not None):
            if(len(self.columns) == 0):
                self.loadColumns()
            subscope=[]
            for a in columns:
                if(a in self.columns):
                    subscope.append(self.columns[a])
            item.subscope = smartsheet.models.webhook_subscope.WebhookSubscope(props={"column_ids":subscope})
        res = self.smart.Webhooks.create_webhook(item).data
        if(res.enabled != enabled):
            self.smart.Webhooks.update_webhook(res.id_, smartsheet.models.webhook.Webhook(props={"enabled": enabled}))

    # delete existing webhook
    def delete_webhook(self, name):
        raise Exception("not implemented: %s()" % inspect.currentframe().f_code.co_nam)
        for a in self.get_webhooks():
            if(a.name == name):
                self.smart.Webhooks.delete_webhook(a.id_)
                
if __name__ == '__main__':
    import dotenv
    dotenv.load_dotenv()
    a = airtable_helper()
    a.insert({"AP ID":"TEST1", "Tags": ["jj","AA"]})
    