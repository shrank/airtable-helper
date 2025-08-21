# airtable-helper
Simple class that helps dealing with airtable api, like dealing with columin ids and types

it uses environment variables for keys and ids:

AIRTABLE_API_KEY=your_secret_access_token
AIRTABLE_SHEET_ID=your_sheet_id
AIRTABLE_BASE_ID=your_base_id

## caveats

- airtables doesn't return empty cells in row results

## smartsheet_mode
the smartsheet mode flag ensures compatibility with the smartsheet_helper class(https://github.com/shrank/smartsheet-helper)

