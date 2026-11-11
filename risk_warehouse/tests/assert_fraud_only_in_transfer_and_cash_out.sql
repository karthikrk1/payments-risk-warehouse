-- PaySim only simulates fraud on transfers and cash-outs. A fraud row of any other type means
-- the source changed or a type was mapped wrongly. Returns offending rows (the test passes on 0).

select
    transaction_id,
    transaction_type
from {{ ref('stg_paysim__transactions') }}
where is_fraud
  and transaction_type not in ('transfer', 'cash_out')
