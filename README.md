# airtable-helper
Simple class that helps dealing with airtable api, like dealing with columin ids and types

it uses environment variables for keys and ids:
````
AIRTABLE_API_KEY=your_secret_access_token
AIRTABLE_SHEET_ID=your_sheet_id
# or AIRTABLE_SHEET_ID=your_sheet_id/your_view_id
AIRTABLE_BASE_ID=your_base_id
``` 

turn on debug on stderr
`AIRTABLE_DEBUG=True`

turm on statics reporting use
```
AIRTABLE_STATS_URL=https://your.stats.server

# optional ID
AIRTABLE_STATS_ID=appID

#optional headers
AIRTABLE_STATS_HEADER=Authorization: asfdkjlkjdlfa
```

## caveats

- airtables doesn't return empty cells in row results

## smartsheet_mode
the smartsheet mode flag ensures compatibility with the smartsheet_helper class(https://github.com/shrank/smartsheet-helper)

