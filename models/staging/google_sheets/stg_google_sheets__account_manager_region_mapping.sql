select
    string_field_1 as account_manager,
    string_field_0 as state
from {{ source('google_sheets', 'account_manager_region_mapping') }}
