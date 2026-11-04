{#- Control totals: every valid staged claim must land in fct_claims. Returns rows only on mismatch. -#}
with staged as (
    select
        count(*) as row_count,
        sum(claim_amount) as total_amount
    from {{ ref('stg_healthcare__claims') }}
    where claim_amount is not null
),

fact as (
    select
        count(*) as row_count,
        sum(claim_amount) as total_amount
    from {{ ref('fct_claims') }}
)

select
    staged.row_count as staged_rows,
    fact.row_count as fact_rows,
    staged.total_amount as staged_amount,
    fact.total_amount as fact_amount
from staged
cross join fact
where
    staged.row_count != fact.row_count
    or staged.total_amount != fact.total_amount
