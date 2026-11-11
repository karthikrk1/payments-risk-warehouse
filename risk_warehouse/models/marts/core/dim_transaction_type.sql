-- One row per transaction type, from the transaction_types seed.

select
    {{ dbt_utils.generate_surrogate_key(['transaction_type']) }} as transaction_type_key,
    transaction_type,
    transaction_type_label,
    sender_direction,
    receiver_kind,
    description
from {{ ref('transaction_types') }}
